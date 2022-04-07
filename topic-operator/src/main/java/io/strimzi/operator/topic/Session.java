/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.topic.zk.Zk;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.http.HttpServer;
import io.vertx.micrometer.backends.BackendRegistries;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.strimzi.operator.common.InvalidConfigurationException;

import java.security.Security;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

public class Session extends AbstractVerticle {

    private final static Logger LOGGER = LogManager.getLogger(Session.class);

    private final static String SASL_TYPE_PLAIN = "plain";
    private final static String SASL_TYPE_SCRAM_SHA_256 = "scram-sha-256";
    private final static String SASL_TYPE_SCRAM_SHA_512 = "scram-sha-512";

    private static final int HEALTH_SERVER_PORT = 8080;


    private final Config config;
    private final WorkerExecutor executor;
    private final TopicOperatorState topicOperatorState;
    private final KubernetesClient kubeClient;

    /*test*/ KafkaImpl kafka;
    private AdminClient adminClient;
    /*test*/ K8sImpl k8s;
    private KafkaStreamsTopicStoreService service; // if used
    /*test*/ TopicOperator topicOperator;
    /*test*/ Watch topicWatch;
    /*test*/ ZkTopicsWatcher topicsWatcher;
    /*test*/ TopicConfigsWatcher topicConfigsWatcher;
    /*test*/ ZkTopicWatcher topicWatcher;
    /*test*/ PrometheusMeterRegistry metricsRegistry;
    K8sTopicWatcher watcher;
    /** The id of the periodic reconciliation timer. This is null during a periodic reconciliation. */
    private volatile Long timerId;
    private volatile boolean stopped = false;
    private Zk zk;
    private volatile HttpServer healthServer;

    /**
     * @param kubeClient kubernetes client
     * @param config  TopicOperator config
     * @param executor vertx worker executor for long blocking tasks
     */
    public Session(KubernetesClient kubeClient, Config config, WorkerExecutor executor) {
        this(kubeClient, config, executor, new TopicOperatorState());
    }

    /**
     * @param kubeClient kubernetes client
     * @param config  TopicOperator config
     * @param executor vertx worker executor for taks that can block longer than 10 seconds
     * @param topicOperatorState used to communicate liveness/readiness to health server from things that can potentially fail
     */
    public Session(KubernetesClient kubeClient, Config config, WorkerExecutor executor, TopicOperatorState topicOperatorState) {
        this.kubeClient = kubeClient;
        this.config = config;
        this.executor = executor;
        this.topicOperatorState = topicOperatorState;
        StringBuilder sb = new StringBuilder(System.lineSeparator());
        for (Config.Value<?> v: Config.keys()) {
            sb.append("\t").append(v.key).append(": ").append(Util.maskPassword(v.key, config.get(v).toString())).append(System.lineSeparator());
        }
        LOGGER.info("Using config:{}", sb.toString());
        this.metricsRegistry = (PrometheusMeterRegistry) BackendRegistries.getDefaultNow();
    }

    /**
     * Stop the operator.
     */
    @Override
    public void stop(Promise<Void> stop) throws Exception {
        this.stopped = true;
        Long timerId = this.timerId;
        if (timerId != null) {
            vertx.cancelTimer(timerId);
        }
        vertx.executeBlocking(blockingResult -> {
            long timeout = 120_000L;
            long deadline = System.currentTimeMillis() + timeout;
            LOGGER.info("Stopping");
            LOGGER.debug("Stopping kube watch");
            topicWatch.close();
            LOGGER.debug("Stopping zk watches");
            topicsWatcher.stop();

            Promise<Void> promise = Promise.promise();
            Handler<Long> longHandler = new Handler<>() {
                @Override
                public void handle(Long inflightTimerId) {
                    if (!topicOperator.isWorkInflight()) {
                        LOGGER.debug("Inflight work has finished");
                        promise.complete();
                    } else if (System.currentTimeMillis() > deadline) {
                        LOGGER.error("Timeout waiting for inflight work to finish");
                        promise.complete();
                    } else {
                        LOGGER.debug("Waiting for inflight work to finish");
                        vertx.setTimer(1_000, this);
                    }
                }
            };
            longHandler.handle(null);

            promise.future().compose(ignored -> {
                if (service != null) {
                    service.stop();
                }
                return Future.succeededFuture();
            });

            promise.future().compose(ignored -> {

                LOGGER.debug("Disconnecting from zookeeper {}", zk);
                zk.disconnect(zkResult -> {
                    if (zkResult.failed()) {
                        LOGGER.warn("Error disconnecting from zookeeper: {}", String.valueOf(zkResult.cause()));
                    }
                    long timeoutMs = Math.max(1, deadline - System.currentTimeMillis());
                    LOGGER.debug("Closing AdminClient {} with timeout {}ms", adminClient, timeoutMs);
                    try {
                        adminClient.close(Duration.ofMillis(timeoutMs));
                        HttpServer healthServer = this.healthServer;
                        if (healthServer != null) {
                            healthServer.close();
                        }
                    } catch (TimeoutException e) {
                        LOGGER.warn("Timeout while closing AdminClient with timeout {}ms", timeoutMs, e);
                    } finally {
                        LOGGER.info("Stopped");
                        blockingResult.complete();
                    }
                });
                return Future.succeededFuture();
            });
        }, stop);
    }

    @SuppressWarnings({"JavaNCSS", "MethodLength", "CyclomaticComplexity"})
    @Override
    public void start(Promise<Void> start) {
        LOGGER.info("Starting");

        String dnsCacheTtl = System.getenv("STRIMZI_DNS_CACHE_TTL") == null ? "30" : System.getenv("STRIMZI_DNS_CACHE_TTL");
        Security.setProperty("networkaddress.cache.ttl", dnsCacheTtl);

        this.adminClient = AdminClient.create(adminClientProperties());
        LOGGER.debug("Using AdminClient {}", adminClient);
        this.kafka = new KafkaImpl(adminClient, vertx);
        LOGGER.debug("Using Kafka {}", kafka);
        Labels labels = config.get(Config.LABELS);

        String namespace = config.get(Config.NAMESPACE);
        LOGGER.debug("Using namespace {}", namespace);
        this.k8s = new K8sImpl(vertx, kubeClient, labels, namespace);
        LOGGER.debug("Using k8s {}", k8s);

        String clientId = config.get(Config.CLIENT_ID);
        LOGGER.debug("Using client-Id {}", clientId);

        startHealthServer(topicOperatorState)
                .onFailure(t -> {
                    LOGGER.error("Failed to start health server", t);

                    // Not really necessary to set this, as a health server that didn't state, will never return a 200
                    // But wanted to keep it consistent with out failure states
                    topicOperatorState.setAlive(false);
                    start.fail(t);
                })
                .onSuccess(httpServer -> healthServer = httpServer)
                .compose(ignored -> Zk.create(vertx,
                        config.get(Config.ZOOKEEPER_CONNECT),
                        config.get(Config.ZOOKEEPER_SESSION_TIMEOUT_MS).intValue(),
                        config.get(Config.ZOOKEEPER_CONNECTION_TIMEOUT_MS).intValue()))
                .onFailure(cause -> {
                    LOGGER.error("Failed to start ZK client", cause);
                    topicOperatorState.setAlive(false);
                    start.fail(cause);
                })
                .onSuccess(zookeeper -> {
                    zk = zookeeper;
                    LOGGER.debug("Using ZooKeeper {}", zk);
                })
                .compose(zk -> executor.executeBlocking((Handler<Promise<TopicStore>>) storePromise -> {
                    Instant startedAt = Instant.now();
                    TopicStore topicStore = createTopicStore(zk, config);
                    LOGGER.info("Topic store created, took {} ms", Duration.between(startedAt, Instant.now()).toMillis());
                    storePromise.complete(topicStore);
                }))
                .onFailure(cause -> {
                    LOGGER.error("Failed to create topic store.", cause);
                    topicOperatorState.setAlive(false);
                    start.fail(cause);
                })
                .onSuccess(topicStore -> LOGGER.debug("Using TopicStore {}", topicStore))
                .compose(topicStore -> createTopicOperatorAndZkWatchers(labels, namespace, topicStore))
                .compose(topicOperator -> {
                    Promise<Void> initReconcilePromise = Promise.promise();
                    watcher = new K8sTopicWatcher(topicOperator, initReconcilePromise.future(), this::startWatcher);
                    LOGGER.debug("Starting watcher");
                    return startWatcher().compose(ignored -> Future.succeededFuture(initReconcilePromise));
                })
                .onSuccess(this::createPeriodicReconcileTrigger)
                .onSuccess(ignored -> {
                    start.complete();
                    topicOperatorState.setReady(true);
                    LOGGER.info("Started");
                });
    }

    private Future<TopicOperator> createTopicOperatorAndZkWatchers(Labels labels, String namespace, TopicStore topicStore) {
        topicOperator = new TopicOperator(vertx, kafka, k8s, topicStore, labels, namespace, config, new MicrometerMetricsProvider());
        LOGGER.debug("Using Operator {}", topicOperator);

        topicConfigsWatcher = new TopicConfigsWatcher(topicOperator);
        LOGGER.debug("Using TopicConfigsWatcher {}", topicConfigsWatcher);
        topicWatcher = new ZkTopicWatcher(topicOperator);
        LOGGER.debug("Using TopicWatcher {}", topicWatcher);
        topicsWatcher = new ZkTopicsWatcher(topicOperator, topicConfigsWatcher, topicWatcher);
        LOGGER.debug("Using TopicsWatcher {}", topicsWatcher);
        topicsWatcher.start(zk);
        return Future.succeededFuture(topicOperator);
    }

    private void createPeriodicReconcileTrigger(Promise<Void> initReconcilePromise) {
        final Long interval = config.get(Config.FULL_RECONCILIATION_INTERVAL_MS);
        Handler<Long> periodic = new Handler<>() {
            @Override
            public void handle(Long oldTimerId) {
                if (!stopped) {
                    timerId = null;
                    boolean isInitialReconcile = oldTimerId == null;
                    topicOperator.getPeriodicReconciliationsCounter().increment();
                    topicOperator.reconcileAllTopics(isInitialReconcile ? "initial " : "periodic ").onComplete(result -> {
                        if (isInitialReconcile) {
                            initReconcilePromise.complete();
                        }
                        if (!stopped) {
                            timerId = vertx.setTimer(interval, this);
                        }
                    });
                }
            }
        };
        periodic.handle(null);
    }

    private TopicStore createTopicStore(Zk zk, Config config) {
        String topicsPath = config.get(Config.TOPICS_PATH);
        if (config.get(Config.USE_ZOOKEEPER_TOPIC_STORE)) {
            return new ZkTopicStore(zk, topicsPath);
        } else {
            boolean exists = zk.getPathExists(topicsPath);
            CompletionStage<KafkaStreamsTopicStoreService> cs;
            if (exists) {
                cs = Zk2KafkaStreams.upgrade(zk, config, adminClientProperties(), false);
            } else {
                KafkaStreamsTopicStoreService ksc = new KafkaStreamsTopicStoreService();
                cs = ksc.start(config, adminClientProperties()).thenCompose(s -> CompletableFuture.completedFuture(ksc));
            }
            try {
                KafkaStreamsTopicStoreService kafkaStreamsTopicStoreService = cs.toCompletableFuture().get();
                this.service = kafkaStreamsTopicStoreService;
                return kafkaStreamsTopicStoreService.store;
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void setSaslConfigs(Properties kafkaClientProps) {
        String saslMechanism = null;
        String jaasConfig = null;
        String username = config.get(Config.SASL_USERNAME);
        String password = config.get(Config.SASL_PASSWORD);
        String configSaslMechanism = config.get(Config.SASL_MECHANISM);

        if (username.isEmpty() || password.isEmpty()) {
            throw new InvalidConfigurationException("SASL credentials are not set");
        }

        if (SASL_TYPE_PLAIN.equals(configSaslMechanism)) {
            saslMechanism = "PLAIN";
            jaasConfig = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";";
        } else if (SASL_TYPE_SCRAM_SHA_256.equals(configSaslMechanism) || SASL_TYPE_SCRAM_SHA_512.equals(configSaslMechanism)) {
            jaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + username + "\" password=\"" + password + "\";";

            if (SASL_TYPE_SCRAM_SHA_256.equals(configSaslMechanism)) {
                saslMechanism = "SCRAM-SHA-256";
            } else if (SASL_TYPE_SCRAM_SHA_512.equals(configSaslMechanism)) {
                saslMechanism = "SCRAM-SHA-512";
            }
        } else {
            throw new IllegalArgumentException("Invalid SASL_MECHANISM type: " + config.get(config.SASL_MECHANISM));
        }

        if (saslMechanism != null) {
            kafkaClientProps.setProperty(SaslConfigs.SASL_MECHANISM, saslMechanism);
        }
        if (jaasConfig != null) {
            kafkaClientProps.setProperty(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
        }
    }

    Properties  adminClientProperties() {
        Properties kafkaClientProps = new Properties();
        kafkaClientProps.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.get(Config.KAFKA_BOOTSTRAP_SERVERS));
        kafkaClientProps.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, config.get(Config.APPLICATION_ID));

        String securityProtocol = config.get(Config.SECURITY_PROTOCOL);
        boolean tlsEnabled = Boolean.parseBoolean(config.get(Config.TLS_ENABLED));

        if (tlsEnabled && !securityProtocol.isEmpty()) {
            if (!securityProtocol.equals("SSL") && !securityProtocol.equals("SASL_SSL")) {
                throw new InvalidConfigurationException("TLS is enabled but the security protocol does not match SSL or SASL_SSL");
            }
        }

        if (!securityProtocol.isEmpty()) {
            kafkaClientProps.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        } else if (tlsEnabled) {
            kafkaClientProps.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SSL");
        } else {
            kafkaClientProps.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
        }

        if (securityProtocol.equals("SASL_SSL") || securityProtocol.equals("SSL") || tlsEnabled) {
            kafkaClientProps.setProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, config.get(Config.TLS_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM));

            if (!config.get(Config.TLS_TRUSTSTORE_LOCATION).isEmpty()) {
                kafkaClientProps.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, config.get(Config.TLS_TRUSTSTORE_LOCATION));
            }

            if (!config.get(Config.TLS_TRUSTSTORE_PASSWORD).isEmpty()) {
                if (config.get(Config.TLS_TRUSTSTORE_LOCATION).isEmpty()) {
                    throw new InvalidConfigurationException("TLS_TRUSTSTORE_PASSWORD was supplied but TLS_TRUSTSTORE_LOCATION was not supplied");
                }
                kafkaClientProps.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, config.get(Config.TLS_TRUSTSTORE_PASSWORD));
            }

            if (!config.get(Config.TLS_KEYSTORE_LOCATION).isEmpty() && !config.get(Config.TLS_KEYSTORE_PASSWORD).isEmpty()) {
                kafkaClientProps.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, config.get(Config.TLS_KEYSTORE_LOCATION));
                kafkaClientProps.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, config.get(Config.TLS_KEYSTORE_PASSWORD));
            }
        }

        if (Boolean.parseBoolean(config.get(Config.SASL_ENABLED))) {
            setSaslConfigs(kafkaClientProps);
        }
        return kafkaClientProps;
    }

    Future<Void> startWatcher() {
        Promise<Void> promise = Promise.promise();
        try {
            LOGGER.debug("Watching KafkaTopics matching {}", config.get(Config.LABELS).labels());

            Session.this.topicWatch = kubeClient.resources(KafkaTopic.class, KafkaTopicList.class)
                    .inNamespace(config.get(Config.NAMESPACE)).withLabels(config.get(Config.LABELS).labels()).watch(watcher);
            LOGGER.debug("Watching setup");
            promise.complete();
        } catch (Throwable t) {
            promise.fail(t);
        }
        return promise.future();
    }

    /**
     * Start an HTTP health server
     */
    private Future<HttpServer> startHealthServer(TopicOperatorState tos) {

        return this.vertx.createHttpServer()
                .requestHandler(request -> {
                    switch (request.path()) {
                        case "/healthy":
                            request.response().setStatusCode(tos.isAlive() ? 200 : 500).end();
                            break;
                        case "/ready":
                            request.response().setStatusCode(tos.isReady() ? 200 : 500).end();
                            break;
                        case "/metrics":
                            request.response().setStatusCode(200).end(metricsRegistry.scrape());
                            break;
                    }
                })
                .listen(HEALTH_SERVER_PORT);
    }
}
