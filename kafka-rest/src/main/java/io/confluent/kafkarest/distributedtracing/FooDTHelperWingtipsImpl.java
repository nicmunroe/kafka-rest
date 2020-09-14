package io.confluent.kafkarest.distributedtracing;

import com.nike.wingtips.Span;
import com.nike.wingtips.Tracer;
import com.nike.wingtips.http.HttpObjectForPropagation;
import com.nike.wingtips.http.HttpRequestTracingUtils;
import com.nike.wingtips.http.RequestWithHeaders;
import com.nike.wingtips.tags.KnownZipkinTags;
import com.nike.wingtips.util.TracingState;
import com.nike.wingtips.zipkin2.WingtipsToZipkinLifecycleListener;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Map;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Configurable;

import io.confluent.kafkarest.KafkaRestConfig;
import io.confluent.kafkarest.entities.ProduceRecord;

import static com.nike.wingtips.util.AsyncWingtipsHelperStatic.runnableWithTracing;
import static com.nike.wingtips.util.AsyncWingtipsHelperStatic.supplierWithTracing;

/**
 * TODO: Class Description
 */
public class FooDTHelperWingtipsImpl implements FooDistributedTracingHelper {

    public static final String PRODUCE_RECORDS_CORRELATION_TAG_NAME = "independent_record_trace_ids";
    public static final String RECORD_TRACE_INHERIT_MODE_HEADER_KEY = "inherit-trace-mode";

    @Override
    public void register(Configurable<?> config, KafkaRestConfig appConfig) {
        config.register(FooWingtipsServerSpanFilter.class);

        // TODO: Set this up so you can specify values in config.
        Tracer.getInstance().addSpanLifecycleListener(
            new WingtipsToZipkinLifecycleListener(
                "rest-proxy-foo",
                "http://localhost:9411"
            )
        );
    }

    @Override
    public @Nullable Object startSpanForClientCall(
        @NotNull ContainerRequestContext containerRequest,
        @NotNull ProduceRecord<?, ?> produceRecord,
        @NotNull String spanName,
        @Nullable Map<String, String> extraTags
    ) {
        TracingState containerRequestTracingState = containerRequestTracingState(containerRequest);
        TracingState parentTracingState = determineParentTracingState(
            containerRequest, containerRequestTracingState, produceRecord
        );

        return supplierWithTracing(
            () -> {
                Span clientSpan = Tracer.getInstance().startSpanInCurrentContext(spanName, Span.SpanPurpose.CLIENT);
                if (extraTags != null) {
                    extraTags.forEach(clientSpan::putTag);
                }

                HttpRequestTracingUtils.propagateTracingHeaders(
                    new TracingPropagationForKafkaHeaders(produceRecord.getHeaders()),
                    clientSpan
                );

                handleCorrelationIds(containerRequestTracingState, clientSpan);

                return TracingState.getCurrentThreadTracingState();
            },
            parentTracingState
        ).get();
    }

    protected @Nullable TracingState containerRequestTracingState(
        @NotNull ContainerRequestContext containerRequest
    ) {
        return (TracingState) containerRequest.getProperty(TracingState.class.getName());
    }

    protected @Nullable TracingState determineParentTracingState(
        @NotNull ContainerRequestContext containerRequest,
        @Nullable TracingState containerRequestTracingState,
        @NotNull ProduceRecord<?, ?> produceRecord
    ) {
        // Use the explicitly-requested parent span info from the record metadata first, if possible.
        Span recordParentSpan = HttpRequestTracingUtils.fromRequestWithHeaders(
            new RequestWithKafkaHeaders(produceRecord.getHeaders()),
            null
        );

        if (recordParentSpan != null) {
            return new TracingState(new ArrayDeque<>(Collections.singleton(recordParentSpan)), null);
        }

        // No tracing headers in the record itself, so look at the container request to see what InheritTraceMode
        //      we should use.
        InheritTraceMode inheritTraceMode = InheritTraceMode.fromContainerRequest(containerRequest);
        switch (inheritTraceMode) {
            case INHERIT_FROM_OVERALL_REQUEST:
                return containerRequestTracingState;
            case CREATE_NEW_TRACE:
                // Returning null will cause a new trace to be created for the record.
                return null;
            default:
                throw new UnsupportedOperationException("Unhandled InheritTraceMode type: " + inheritTraceMode.name());
        }
    }

    protected void handleCorrelationIds(
        @Nullable TracingState containerRequestTracingState,
        @NotNull Span recordSpan
    ) {
        Span containerReqSpan = (containerRequestTracingState == null)
                                ? null
                                : containerRequestTracingState.getActiveSpan();

        if (containerReqSpan != null && !containerReqSpan.getTraceId().equals(recordSpan.getTraceId())
        ) {
            // The trace ID for the produce record's parent span is different than the overall HTTP request span's
            //      trace. Add a correlation ID to the overall HTTP request span so investigators can find all the
            //      traces that resulted from the overall HTTP request.
            String existingCorrelationTag = containerReqSpan.getTags().get(PRODUCE_RECORDS_CORRELATION_TAG_NAME);
            String correlationTraceId = recordSpan.getTraceId();
            String newCorrelationTag = (existingCorrelationTag == null)
                                     ? correlationTraceId
                                     : existingCorrelationTag + "," + correlationTraceId;
            containerReqSpan.putTag(PRODUCE_RECORDS_CORRELATION_TAG_NAME, newCorrelationTag);
        }
    }

    @Override
    public void completeSpanForClientCall(
        @Nullable Object clientSpanTracingContext,
        @Nullable Throwable error,
        @Nullable Map<String, String> extraTags
    ) {
        if (clientSpanTracingContext == null) {
            return;
        }

        TracingState clientSpanTracingState = ((TracingState) clientSpanTracingContext);
        Span clientSpan = clientSpanTracingState.getActiveSpan();
        if (clientSpan == null) {
            return;
        }

        runnableWithTracing(() -> {
            if (error != null) {
                clientSpan.putTag(KnownZipkinTags.ERROR, error.toString());
            }

            if (extraTags != null) {
                extraTags.forEach(clientSpan::putTag);
            }

            clientSpan.close();
        }, clientSpanTracingState).run();
    }

    protected static class TracingPropagationForKafkaHeaders implements HttpObjectForPropagation {

        private final @NotNull Headers headers;

        public TracingPropagationForKafkaHeaders(@NotNull Headers headers) {
            this.headers = headers;
        }

        @Override
        public void setHeader(String headerKey, String headerValue) {
            headers.remove(headerKey);
            headers.add(headerKey, headerValue.getBytes(StandardCharsets.UTF_8));
        }
    }

    public static class RequestWithKafkaHeaders implements RequestWithHeaders {

        private final @NotNull Headers kafkaHeaders;

        public RequestWithKafkaHeaders(@NotNull Headers kafkaHeaders) {
            this.kafkaHeaders = kafkaHeaders;
        }

        @Override
        public String getHeader(String headerName) {
            for (Header header : kafkaHeaders) {
                if (header.key().equalsIgnoreCase(headerName)) {
                    return new String(header.value(), StandardCharsets.UTF_8);
                }
            }

            return null;
        }

        @Override
        public Object getAttribute(String name) {
            return null;
        }
    }

    public enum InheritTraceMode {
        INHERIT_FROM_OVERALL_REQUEST("inherit"),
        CREATE_NEW_TRACE("newtrace");

        private final String headerValue;

        InheritTraceMode(String headerValue) {
            this.headerValue = headerValue;
        }

        public static InheritTraceMode fromContainerRequest(
            @Nullable ContainerRequestContext request
        ) {
            if (request == null) {
                return INHERIT_FROM_OVERALL_REQUEST;
            }

            String desiredMode = request.getHeaders().getFirst(RECORD_TRACE_INHERIT_MODE_HEADER_KEY);
            if (desiredMode != null) {
                for (InheritTraceMode mode : values()) {
                    if (mode.headerValue.equalsIgnoreCase(desiredMode)) {
                        return mode;
                    }
                }
            }

            // No match found, so default to inherit mode.
            return INHERIT_FROM_OVERALL_REQUEST;
        }
    }

}
