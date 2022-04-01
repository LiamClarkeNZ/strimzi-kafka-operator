/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;


import java.util.Optional;
import java.util.Properties;
import java.util.function.Supplier;

import static io.strimzi.operator.topic.TopicCommand.Type.DELETE;

/**
 * Kafka Streams topology provider for TopicStore.
 */
public class TopicStoreTopologyProvider implements Supplier<Topology> {
    private final String inputTopic;
    private final String storeTopic;
    private final Properties kafkaProperties;
    private final ForeachAction<? super String, ? super Integer> dispatcher;
    private KTable<String, Topic> storeRef;

    public String  getStoreName() {
        return storeRef.queryableStoreName();
    }


    static class CommandAndMaybeTopic {
        private final TopicCommand command;
        private final Topic maybeTopic;

        CommandAndMaybeTopic(TopicCommand command, Topic maybeTopic) {
            this.command = command;
            this.maybeTopic = maybeTopic;
        }

        public TopicCommand getCommand() {
            return command;
        }

        public Optional<Topic> getMaybeTopic() {
            return Optional.ofNullable(maybeTopic);
        }
    }


    static class CommandAndMaybeError {

        private final TopicCommand command;
        private final Integer maybeError;

        CommandAndMaybeError(TopicCommand command, Integer maybeError) {
            this.command = command;
            this.maybeError = maybeError;
        }

        public TopicCommand getCommand() {
            return command;
        }

        public Optional<Integer> getMaybeError() {
            return Optional.ofNullable(maybeError);
        }
    }

    static class TopicSerde implements Serde<Topic> {

        @Override
        public Serializer<Topic> serializer() {
            return (key, topic) -> TopicSerialization.toJson(topic);
        }

        @Override
        public Deserializer<Topic> deserializer() {
            return (key, bytes) -> TopicSerialization.fromJson(bytes);
        }
    }

    public TopicStoreTopologyProvider(
            String inputTopic,
            String storeTopic,
            Properties kafkaProperties,
            ForeachAction<? super String, ? super Integer> dispatcher
    ) {
        this.inputTopic = inputTopic;
        this.storeTopic = storeTopic;
        this.kafkaProperties = kafkaProperties;
        this.dispatcher = dispatcher;
    }

    @Override
    public Topology get() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, TopicCommand> input = builder.stream(inputTopic, Consumed.with(Serdes.String(), new TopicCommandSerde()));

        //Gross Kafka Streams, gross, use a real builder...
        Materialized<String, Topic, KeyValueStore<Bytes, byte[]>> storeConf = Materialized.<String, Topic, KeyValueStore<Bytes, byte[]>>as(storeTopic)
                                                                                          .withKeySerde(Serdes.String())
                                                                                          .withValueSerde(new TopicSerde());
        KTable<String, Topic> store = builder.table(storeTopic, storeConf);
        this.storeRef = store;

        KStream<String, CommandAndMaybeTopic> leftJoined = input.leftJoin(store, CommandAndMaybeTopic::new);

        KStream<String, CommandAndMaybeError> errorChecked = leftJoined.mapValues(joined -> {
            TopicCommand cmd = joined.getCommand();
            Optional<Topic> maybeTopic = joined.getMaybeTopic();

            Integer error = null;
            switch (cmd.getType()) {
                case CREATE:
                    if (maybeTopic.isPresent()) {
                        error = KafkaStreamsTopicStore.toIndex(TopicStore.EntityExistsException.class);
                    }
                    break;
                case UPDATE:
                    // we can happily stick the thing in
                    break;
                case DELETE:
                    if (maybeTopic.isEmpty()) {
                        error = KafkaStreamsTopicStore.toIndex(TopicStore.NoSuchEntityExistsException.class);
                    }
                    break;
            }

            return new CommandAndMaybeError(cmd, error);
        });


        //noinspection OptionalGetWithoutIsPresent - we're already checking for the presence higher up
        Branched<String, CommandAndMaybeError> erroredBranch = Branched.withConsumer(erroredStream ->
                erroredStream.foreach((key, value) ->
                        dispatcher.apply(value.getCommand().getUuid(), value.getMaybeError().get())));

        Branched<String, CommandAndMaybeError> allGood = Branched.withConsumer(goodStream -> {

            //While the topic for a delete is already null, I want to be explicit here
            goodStream.mapValues(value -> value.getCommand().getType() != DELETE ? value.getCommand().getTopic() : null)
                      .to(storeTopic, Produced.with(Serdes.String(), new TopicSerde()));
            goodStream.foreach((key, value) -> dispatcher.apply(value.getCommand().getUuid(), null));
        });


       errorChecked.split()
                   .branch((key, value) -> value.getMaybeError().isPresent(), erroredBranch)
                   .defaultBranch(allGood);


        return builder.build(kafkaProperties);
    }

}