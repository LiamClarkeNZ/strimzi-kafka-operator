/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.apicurio.registry.utils.kafka.AsyncProducer;
import io.apicurio.registry.utils.kafka.ProducerActions;
import io.apicurio.registry.utils.streams.diservice.AsyncBiFunctionService;
import io.apicurio.registry.utils.streams.ext.ForeachActionDispatcher;
import io.apicurio.registry.utils.streams.ext.LoggingStateRestoreListener;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Integer.parseInt;

/**
 * A service to configure and start/stop KafkaStreamsTopicStore.
 */
public class KafkaStreamsTopicStoreService {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsTopicStoreService.class);

    private final List<AutoCloseable> closeables = new ArrayList<>();

    /* test */ KafkaStreams streams;
    /* test */ TopicStore store;
    private String queryableStoreName;

    static class ServiceyStuffWrapper {
        private final AsyncBiFunctionService.WithSerdes<String, String, Integer> serviceImpl;
        private final String queryableStoreName;

        ServiceyStuffWrapper(AsyncBiFunctionService.WithSerdes<String, String, Integer> serviceImpl, String queryableStoreName) {

            this.serviceImpl = serviceImpl;
            this.queryableStoreName = queryableStoreName;
        }

        public AsyncBiFunctionService.WithSerdes<String, String, Integer> getServiceImpl() {
            return serviceImpl;
        }

        public String getQueryableStoreName() {
            return queryableStoreName;
        }
    }

    public CompletionStage<TopicStore> start(Config config, Properties kafkaProperties) {
        String inputTopic = config.get(Config.STORE_TOPIC);
        String storeTopic = config.get(Config.STORE_NAME);

        // check if entry topic has the right configuration
        Admin admin = Admin.create(kafkaProperties);
        LOGGER.info("Starting ...");
        return toCS(admin.describeCluster().nodes())
                .thenApply(nodes -> new Context(nodes.size()))
                .thenCompose(c -> toCS(admin.listTopics().names()).thenApply(c::setTopics))
                .thenApply(c -> {
                    if (c.topics.contains(inputTopic)) {
                        validateExistingStoreTopic(inputTopic, admin, c);
                    } else {
                        createTopic(inputTopic, admin, c);
                    }
                    return c;
                })
                .thenApply(c -> createTopic(storeTopic, admin, c))
                .thenCompose(v -> createKafkaStreams(config, kafkaProperties, inputTopic, storeTopic))
                .thenApply(serviceWrapper -> createKafkaTopicStore(config, kafkaProperties, inputTopic, serviceWrapper))
                .whenCompleteAsync((v, t) -> {
                    // use another thread to stop, if needed
                    try {
                        if (t != null) {
                            LOGGER.warn("Failed to start.", t);
                            stop();
                        } else {
                            LOGGER.info("Started.");
                        }
                    } finally {
                        close(admin);
                    }
                });
    }

    private TopicStore createKafkaTopicStore(Config config, Properties kafkaProperties, String storeTopic, ServiceyStuffWrapper serviceWrapper) {
        LOGGER.info("Creating topic store ...");
        ProducerActions<String, TopicCommand> producer = new AsyncProducer<>(
                kafkaProperties,
            Serdes.String().serializer(),
            new TopicCommandSerde()
        );
        closeables.add(producer);

        StoreQueryParameters<ReadOnlyKeyValueStore<String, Topic>> storeQueryParameters = StoreQueryParameters.fromNameAndType(config.get(Config.STORE_NAME), QueryableStoreTypes.keyValueStore());
        ReadOnlyKeyValueStore<String, Topic> store = streams.store(storeQueryParameters.enableStaleStores());

        this.store = new KafkaStreamsTopicStore(store, storeTopic, producer, serviceWrapper.getServiceImpl());
        return this.store;
    }

    private CompletableFuture<ServiceyStuffWrapper> createKafkaStreams(Config config, Properties kafkaProperties, String inputTopic, String storeTopic) {
        LOGGER.info("Creating Kafka Streams, store name: {}", storeTopic);
        long timeoutMillis = config.get(Config.STALE_RESULT_TIMEOUT_MS);
        ForeachActionDispatcher<String, Integer> dispatcher = new ForeachActionDispatcher<>();
        WaitForResultService serviceImpl = new WaitForResultService(timeoutMillis, dispatcher);
        closeables.add(serviceImpl);

        AtomicBoolean done = new AtomicBoolean(false); // no need for dup complete
        CompletableFuture<ServiceyStuffWrapper> cf = new CompletableFuture<>();
        KafkaStreams.StateListener listener = (newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING && !done.getAndSet(true)) {
                cf.completeAsync(() -> new ServiceyStuffWrapper(serviceImpl, queryableStoreName)); // complete in a different thread
            }
            if (newState == KafkaStreams.State.ERROR) {
                cf.completeExceptionally(new IllegalStateException("KafkaStreams error"));
            }
        };

        // provide some Kafka Streams defaults
        Properties streamsProperties = new Properties();
        streamsProperties.putAll(kafkaProperties);
        Object rf = kafkaProperties.get(StreamsConfig.REPLICATION_FACTOR_CONFIG);
        if (rf == null) {
            // this will pickup default broker settings
            streamsProperties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "-1");
        }

        TopicStoreTopologyProvider topicStoreTopologyProvider = new TopicStoreTopologyProvider(inputTopic, storeTopic, streamsProperties, dispatcher);
        Topology topology = topicStoreTopologyProvider.get();

        streams = new KafkaStreams(topology, streamsProperties);
        streams.setStateListener(listener);
        streams.setGlobalStateRestoreListener(new LoggingStateRestoreListener());
        closeables.add(streams);
        streams.start();

        return cf;
    }

    private CompletionStage<Void> createTopic(String topic, Admin admin, Context c) {
        LOGGER.info("Creating new topic: {}", topic);
        int rf = Math.min(3, c.clusterSize);
        int minISR = Math.max(rf - 1, 1);
        NewTopic newTopic = new NewTopic(topic, 1, (short) rf)
            .configs(Collections.singletonMap(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(minISR)));
        return toCS(admin.createTopics(Collections.singleton(newTopic)).all());
    }

    private void validateExistingStoreTopic(String storeTopic, Admin admin, Context c) {
        LOGGER.info("Validating existing store topic: {}", storeTopic);
        ConfigResource storeTopicConfigResource = new ConfigResource(ConfigResource.Type.TOPIC, storeTopic);
        toCS(admin.describeTopics(Collections.singleton(storeTopic)).topicNameValues().get(storeTopic))
                .thenApply(td -> c.setRf(td.partitions().stream().map(tp -> tp.replicas().size()).min(Integer::compare).orElseThrow()))
                .thenCompose(c2 -> toCS(admin.describeConfigs(Collections.singleton(storeTopicConfigResource)).values().get(storeTopicConfigResource))
                        .thenApply(cr -> c2.setMinISR(parseInt(cr.get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).value()))))
                .thenApply(c3 -> {
                    if (c3.rf != Math.min(3, c3.clusterSize) || c3.minISR != c3.rf - 1) {
                        LOGGER.warn("Durability of the topic [{}] is not sufficient for production use - replicationFactor: {}, {}: {}. " +
                                        "Increase the replication factor to at least 3 and configure the {} to {}.",
                                storeTopic, c3.rf, TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, c3.minISR, TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, c3.minISR);
                    }
                    return null;
                });
    }

    public void stop() {
        LOGGER.info("Stopping services ...");
        Collections.reverse(closeables);
        closeables.forEach(KafkaStreamsTopicStoreService::close);
    }

    private static void close(AutoCloseable service) {
        try {
            service.close();
        } catch (Exception e) {
            LOGGER.warn("Exception while closing service: {}", service, e);
        }
    }

    private static <T> CompletionStage<T> toCS(KafkaFuture<T> kf) {
        CompletableFuture<T> cf = new CompletableFuture<>();
        kf.whenComplete((v, t) -> {
            if (t != null) {
                cf.completeExceptionally(t);
            } else {
                cf.complete(v);
            }
        });
        return cf;
    }

    static class Context {
        int clusterSize;
        Set<String> topics = Collections.emptySet(); // to make spotbugs happy
        int rf;
        int minISR;

        public Context(int clusterSize) {
            this.clusterSize = clusterSize;
        }

        public Context setTopics(Set<String> topics) {
            this.topics = topics;
            return this;
        }

        public Context setRf(int rf) {
            this.rf = rf;
            return this;
        }

        public Context setMinISR(int minISR) {
            this.minISR = minISR;
            return this;
        }
    }

}
