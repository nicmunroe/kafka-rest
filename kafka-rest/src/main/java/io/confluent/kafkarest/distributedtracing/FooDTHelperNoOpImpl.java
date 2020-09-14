package io.confluent.kafkarest.distributedtracing;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Configurable;

import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.entities.ProduceRecord;

/**
 * TODO: Class Description
 */
public class FooDTHelperNoOpImpl implements FooDistributedTracingHelper {

    @Override
    public void register(Configurable<?> config, KafkaRestConfig appConfig) {
        // Do nothing.
    }

    @Nullable
    @Override
    public Object startSpanForClientCall(
        @NotNull ContainerRequestContext containerRequest,
        @NotNull ProduceRecord<?, ?> produceRecord,
        @NotNull String spanName,
        @Nullable Map<String, String> extraTags
    ) {
        return null;
    }

    @Override
    public void completeSpanForClientCall(
        @Nullable Object clientSpanTracingContext, @Nullable Throwable error, @Nullable Map<String, String> extraTags
    ) {

    }


}
