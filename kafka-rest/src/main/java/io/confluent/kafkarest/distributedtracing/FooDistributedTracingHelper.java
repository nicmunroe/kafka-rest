package io.confluent.kafkarest.distributedtracing;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Configurable;

import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.entities.ProduceRecord;

/**
 * TODO: Class Description
 */
public interface FooDistributedTracingHelper {

    String KAFKA_TOPIC_TAG_KEY = "kafka.topic";
    String KAFKA_KEY_TAG_KEY = "kafka.key";
    String KAFKA_OFFSET_TAG_KEY = "kafka.offset";
    String KAFKA_PARTITION_TAG_KEY = "kafka.partition";
    String KAFKA_TIMESTAMP_TAG_KEY = "kafka.timestamp";
    String KAFKA_SERIALIZED_KEY_SIZE_TAG_KEY = "kafka.serializedKeySize";
    String KAFKA_SERIALIZED_VALUE_SIZE_TAG_KEY = "kafka.serializedValueSize";
    String KAFKA_CLIENT_ID_TAG_KEY = "kafka.client.id";
    String KAFKA_GROUP_ID_TAG_KEY = "kafka.group.id";

    void register(Configurable<?> config, KafkaRestConfig appConfig);

    default @Nullable Map<String, String> generateTagsForProduceRecord(
        @NotNull ProduceRecord<?, ?> record,
        @Nullable KafkaProducer<?, ?> kafkaProducer,
        @Nullable String topic,
        @Nullable Integer partitionOverride
    ) {
        //noinspection ConstantConditions
        if (record == null) {
            return null;
        }

        Map<String, String> tags = new HashMap<>(3);

        Object key = record.getKey();
        if (key instanceof String && !"".equals(key)) {
            tags.put(KAFKA_KEY_TAG_KEY, key.toString());
        }

        if (topic != null) {
            tags.put(KAFKA_TOPIC_TAG_KEY, topic);
        }

        if (partitionOverride == null) {
            tags.put(KAFKA_PARTITION_TAG_KEY, String.valueOf(record.getPartition()));
        }
        else {
            tags.put(KAFKA_PARTITION_TAG_KEY, String.valueOf(partitionOverride));
        }

        // TODO: Pull clientId from the KafkaProducer and add a tag? It would require reflection...

        return tags;
    }

    default @Nullable Map<String, String> generateTagsForRecordMetadata(
        @NotNull RecordMetadata metadata
    ) {
        //noinspection ConstantConditions
        if (metadata == null) {
            return null;
        }

        Map<String, String> tags = new HashMap<>(6);

        tags.put(KAFKA_TOPIC_TAG_KEY, metadata.topic());
        tags.put(KAFKA_OFFSET_TAG_KEY, String.valueOf(metadata.offset()));
        tags.put(KAFKA_PARTITION_TAG_KEY, String.valueOf(metadata.partition()));
        tags.put(KAFKA_TIMESTAMP_TAG_KEY, String.valueOf(metadata.timestamp()));
        tags.put(KAFKA_SERIALIZED_KEY_SIZE_TAG_KEY, String.valueOf(metadata.serializedKeySize()));
        tags.put(KAFKA_SERIALIZED_VALUE_SIZE_TAG_KEY, String.valueOf(metadata.serializedValueSize()));

        return tags;
    }

    @Nullable Object startSpanForClientCall(
        @NotNull ContainerRequestContext containerRequest,
        @NotNull ProduceRecord<?, ?> produceRecord,
        @NotNull String spanName,
        @Nullable Map<String, String> extraTags
    );

    void completeSpanForClientCall(
        @Nullable Object clientSpanTracingContext,
        @Nullable Throwable error,
        @Nullable Map<String, String> extraTags
    );
}
