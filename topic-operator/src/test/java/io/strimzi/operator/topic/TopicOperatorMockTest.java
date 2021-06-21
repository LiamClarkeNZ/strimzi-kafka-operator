/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaTopicBuilder;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.mockkube.MockKube;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static io.strimzi.test.TestUtils.waitFor;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;

@ExtendWith(VertxExtension.class)
public class TopicOperatorMockTest {
    private static final Logger LOGGER = LogManager.getLogger(TopicOperatorMockTest.class);
    private static EmbeddedKafkaCluster cluster;

    private KubernetesClient kubeClient;
    private Session session;
    private static Vertx vertx;
    private String deploymentId;
    private AdminClient adminClient;
    private TopicConfigsWatcher topicsConfigWatcher;
    private ZkTopicWatcher topicWatcher;
    private PrometheusMeterRegistry metrics;
    private ZkTopicsWatcher topicsWatcher;

    // TODO this is all in common with TOIT, so factor out a common base class

    @BeforeAll
    public static void before() throws IOException {
        VertxOptions options = new VertxOptions().setMetricsOptions(
                new MicrometerMetricsOptions()
                        .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                        .setEnabled(true));
        vertx = Vertx.vertx(options);

        cluster = new EmbeddedKafkaCluster(1);
        cluster.start();
    }

    @AfterAll
    public static void after() throws InterruptedException, ExecutionException {
        vertx.close().toCompletionStage().toCompletableFuture().get();
    }

    @BeforeEach
    public void createMockKube(VertxTestContext context) throws Exception {
        MockKube mockKube = new MockKube();
        mockKube.withCustomResourceDefinition(Crds.kafkaTopic(),
                        KafkaTopic.class, KafkaTopicList.class, KafkaTopic::getStatus, KafkaTopic::setStatus);
        kubeClient = mockKube.build();

        Properties p = new Properties();
        p.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        adminClient = AdminClient.create(p);

        Map<String, String> m = new HashMap<>();
        m.put(io.strimzi.operator.topic.Config.KAFKA_BOOTSTRAP_SERVERS.key, cluster.bootstrapServers());
        m.put(io.strimzi.operator.topic.Config.ZOOKEEPER_CONNECT.key, cluster.zKConnectString());
        m.put(io.strimzi.operator.topic.Config.ZOOKEEPER_CONNECTION_TIMEOUT_MS.key, "30000");
        m.put(io.strimzi.operator.topic.Config.NAMESPACE.key, "myproject");
        m.put(io.strimzi.operator.topic.Config.CLIENT_ID.key, "myproject-client-id");
        m.put(io.strimzi.operator.topic.Config.FULL_RECONCILIATION_INTERVAL_MS.key, "10000");
        session = new Session(kubeClient, new io.strimzi.operator.topic.Config(m));

        vertx.deployVerticle(session, ar -> {
            if (ar.succeeded()) {
                deploymentId = ar.result();
                topicsConfigWatcher = session.topicConfigsWatcher;
                topicWatcher = session.topicWatcher;
                topicsWatcher = session.topicsWatcher;
                metrics = session.metricsRegistry;
                metrics.forEachMeter(meter -> metrics.remove(meter));
                context.completeNow();
            } else {
                ar.cause().printStackTrace();
                context.failNow(new Throwable("Failed to deploy session"));
            }
        });
        if (!context.awaitCompletion(60, TimeUnit.SECONDS)) {
            context.failNow(new Throwable("Test timeout"));
        }

        int timeout = 30_000;

        waitFor("Topic watcher not started",  1_000, timeout,
            () -> this.topicWatcher.started());
        waitFor("Topic configs watcher not started", 1_000, timeout,
            () -> this.topicsConfigWatcher.started());
        waitFor("Topic watcher not started", 1_000, timeout,
            () -> this.topicsWatcher.started());
        //waitFor(context, () -> this.topicsConfigWatcher.started(), timeout, "Topic configs watcher not started");
        //waitFor(context, () -> this.topicWatcher.started(), timeout, "Topic watcher not started");
    }

    @AfterEach
    public void tearDown(VertxTestContext context) {
        if (vertx != null && deploymentId != null) {
            vertx.undeploy(deploymentId, undeployResult -> {
                topicWatcher.stop();
                topicsWatcher.stop();
                topicsConfigWatcher.stop();
                metrics.close();
                waitFor("Topic watcher stopped",  1_000, 30_000,
                    () -> !this.topicWatcher.started());
                waitFor("Topic configs watcher stopped", 1_000, 30_000,
                    () -> !this.topicsConfigWatcher.started());
                waitFor("Topic watcher stopped", 1_000, 30_000,
                    () -> !this.topicsWatcher.started());
                waitFor("Metrics watcher stopped", 1_000, 30_000,
                    () -> this.metrics.isClosed());
                if (adminClient != null) {
                    adminClient.close();
                }

                context.completeNow();
            });
        }
    }

    private void createInKube(KafkaTopic topic) {
        Crds.topicOperation(kubeClient).create(topic);
    }

    private void updateInKube(KafkaTopic topic) {
        LOGGER.info("Updating topic {} in kube", topic.getMetadata().getName());
        Crds.topicOperation(kubeClient).withName(topic.getMetadata().getName()).patch(topic);
    }

    @Test
    public void testCreatedWithoutTopicNameInKube(VertxTestContext context) {
        LOGGER.info("Test started");

        int retention = 100_000_000;
        KafkaTopic kt = new KafkaTopicBuilder()
                .withNewMetadata()
                    .withName("my-topic")
                    .withNamespace("myproject")
                    .addToLabels(Labels.STRIMZI_KIND_LABEL, "topic")
                    .addToLabels(Labels.KUBERNETES_NAME_LABEL, "topic-operator")
                .endMetadata()
                .withNewSpec()
                    .withPartitions(1)
                    .withReplicas(1)
                    .addToConfig("retention.bytes", retention)
                .endSpec().build();

        testCreatedInKube(context, kt);
    }

    void testCreatedInKube(VertxTestContext context, KafkaTopic kt) {
        String kubeName = kt.getMetadata().getName();
        String kafkaName = kt.getSpec().getTopicName() != null ? kt.getSpec().getTopicName() : kubeName;
        int retention = (Integer) kt.getSpec().getConfig().get("retention.bytes");

        createInKube(kt);

        // Check created in Kafka
        waitUntilTopicExistsInKafka(kafkaName);
        LOGGER.info("Topic has been created");
        Future<Topic> fromKafka = getFromKafka(kafkaName);
        fromKafka.onSuccess(topic -> context.verify(() -> assertThat(topic.getTopicName().toString(), is(kafkaName))))
                  //reconcile after no changes.
                 .onComplete(x -> reconcile())
                 // Check things still the same
                 .onSuccess(topic -> getFromKafka(kafkaName)
                                     .onSuccess(topicAgain -> context.verify(() -> assertThat(topic, is(topicAgain)))));

        // Config change + reconcile
        updateInKube(new KafkaTopicBuilder(kt).editSpec().addToConfig("retention.bytes", retention + 1).endSpec().build());
        waitUntilTopicInKafka(kafkaName, config -> Integer.toString(retention + 1).equals(config.get("retention.bytes").value()));

        // Another reconciliation
        reconcile().onComplete(x -> getFromKafka(kafkaName)
                                       // Check things still the same
                                       .onSuccess(topic -> context.verify(() -> {
                                            Topic expected = new Topic.Builder(fromKafka.result())
                                                    .withConfigEntry("retention.bytes", Integer.toString(retention + 1))
                                                    .build();
                                            assertThat(topic, is(expected));
                    })))
                    .onSuccess(x -> context.completeNow());

        // Reconcile after change #partitions change
        // Check things still the same
        // Try to add a matching spec.topicName
        // Check things still the same
        // Try to change spec.topicName
        // Check error
        // Try to change spec.topicName back
        // Check things still the same (recover from error)
        // Try to remove spec.topicName
        // Check things still the same
    }

    Future<Topic> getFromKafka(String topicName) {
        Future<TopicMetadata> kafkaMetadata = session.kafka.topicMetadata(Reconciliation.DUMMY_RECONCILIATION, new TopicName(topicName));
        return kafkaMetadata.map(TopicSerialization::fromTopicMetadata);
    }

    private void waitUntilTopicExistsInKafka(String topicName) {
        waitUntilTopicInKafka(topicName, Objects::nonNull);
    }

    private void waitUntilTopicInKafka(String topicName, Predicate<Config> p) {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        AtomicReference<Config> ref = new AtomicReference<>();
        waitFor("Creation of topic " + topicName, 1_000, 60_000, () -> {
            try {
                Map<ConfigResource, Config> descriptionMap = adminClient.describeConfigs(Collections.singletonList(configResource)).all().get();
                Config desc = descriptionMap.get(configResource);
                if (p.test(desc)) {
                    ref.set(desc);
                    return true;
                }
                return false;
            } catch (Exception e) {
                return false;
            }
        });
        ref.get();
    }

    Future<?> reconcile() {
        return session.topicOperator.reconcileAllTopics("test");
    }


    @Test
    public void testCreatedWithSameTopicNameInKube(VertxTestContext context) {

        int retention = 100_000_000;
        KafkaTopic kt = new KafkaTopicBuilder()
                .withNewMetadata()
                    .withName("my-topic")
                    .withNamespace("myproject")
                    .addToLabels(Labels.STRIMZI_KIND_LABEL, "topic")
                .endMetadata()
                .withNewSpec()
                    .withTopicName("my-topic") // the same as metadata.name
                    .withPartitions(1)
                    .withReplicas(1)
                    .addToConfig("retention.bytes", retention)
                .endSpec().build();

        testCreatedInKube(context, kt);
    }

    @Test
    public void testCreatedWithDifferentTopicNameInKube(VertxTestContext context) {
        int retention = 100_000_000;
        KafkaTopic kt = new KafkaTopicBuilder()
                .withNewMetadata()
                    .withName("my-topic")
                    .withNamespace("myproject")
                    .addToLabels(Labels.STRIMZI_KIND_LABEL, "topic")
                .endMetadata()
                .withNewSpec()
                    .withTopicName("DIFFERENT") // different to metadata.name
                    .withPartitions(1)
                    .withReplicas(1)
                    .addToConfig("retention.bytes", retention)
                .endSpec().build();

        testCreatedInKube(context, kt);
    }

    @Test
    public void testCreatedWithDefaultsInKube(VertxTestContext context) {
        int retention = 100_000_000;
        KafkaTopic kt = new KafkaTopicBuilder()
                .withNewMetadata()
                    .withName("my-topic")
                    .withNamespace("myproject")
                    .addToLabels(Labels.STRIMZI_KIND_LABEL, "topic")
                .endMetadata()
                .withNewSpec()
                    .addToConfig("retention.bytes", retention)
                .endSpec().build();
    
        testCreatedInKube(context, kt);
    }

    @Test
    public void testReconciliationPaused(VertxTestContext context) throws InterruptedException {
        LOGGER.info("Test started");

        int retention = 100_000_000;
        KafkaTopic kt = new KafkaTopicBuilder()
                .withNewMetadata()
                    .withName("my-topic")
                    .withNamespace("myproject")
                    .addToLabels(Labels.STRIMZI_KIND_LABEL, "topic")
                    .addToLabels(Labels.KUBERNETES_NAME_LABEL, "topic-operator")
                    .withAnnotations(singletonMap("strimzi.io/pause-reconciliation", "true"))
                .endMetadata()
                .withNewSpec()
                    .withPartitions(1)
                    .withReplicas(1)
                    .addToConfig("retention.bytes", retention)
                .endSpec()
                .build();

        testNotCreatedInKube(context, kt);
    }

    void testNotCreatedInKube(VertxTestContext context, KafkaTopic kt) throws InterruptedException {
        String kubeName = kt.getMetadata().getName();
        String kafkaName = kt.getSpec().getTopicName() != null ? kt.getSpec().getTopicName() : kubeName;
        int retention = (Integer) kt.getSpec().getConfig().get("retention.bytes");

        createInKube(kt);

        Thread.sleep(2000);
        LOGGER.info("Topic has not been created");

        getFromKafka(kafkaName).onSuccess(topic -> context.verify(() -> assertThat(topic, is(nullValue()))))
                               // Reconcile after no changes
                               .onSuccess(topic -> reconcile())
                               // Check things still the same
                               .onSuccess(topic -> assertThat(topic, is(nullValue())));

        // Config change + reconcile
        updateInKube(new KafkaTopicBuilder(kt).editSpec().addToConfig("retention.bytes", retention + 1).endSpec().build());

        // Another reconciliation
        // Check things still the same
        reconcile().onSuccess(x -> getFromKafka(kafkaName)
                                   .onSuccess(topic -> context.verify(() -> assertThat(topic, is(nullValue())))))
                    .onSuccess(x -> context.completeNow());
    }

}
