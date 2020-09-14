package io.confluent.kafkarest.distributedtracing;

import com.nike.internal.util.StringUtils;
import com.nike.wingtips.Span;
import com.nike.wingtips.TraceHeaders;
import com.nike.wingtips.Tracer;
import com.nike.wingtips.http.HttpRequestTracingUtils;
import com.nike.wingtips.tags.HttpTagAndSpanNamingAdapter;
import com.nike.wingtips.tags.HttpTagAndSpanNamingStrategy;
import com.nike.wingtips.tags.ZipkinHttpTagStrategy;
import com.nike.wingtips.util.TracingState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;

import io.confluent.kafkarest.distributedtracing.wingtips.JaxRs2RequestTagAdapter;
import io.confluent.kafkarest.distributedtracing.wingtips.JaxRs2RequestWithHeaders;

import static com.nike.wingtips.util.AsyncWingtipsHelperStatic.runnableWithTracing;
import static com.nike.wingtips.util.AsyncWingtipsHelperStatic.unlinkTracingFromCurrentThread;

/**
 * TODO: Class Description
 */
public class FooWingtipsServerSpanFilter implements ContainerRequestFilter, ContainerResponseFilter {

    public static final Logger logger = LoggerFactory.getLogger(FooWingtipsServerSpanFilter.class);

    private final HttpTagAndSpanNamingStrategy<ContainerRequestContext, ContainerResponseContext> tagStrategy =
        ZipkinHttpTagStrategy.getDefaultInstance();
    private final HttpTagAndSpanNamingAdapter<ContainerRequestContext, ContainerResponseContext> tagAdapter =
        JaxRs2RequestTagAdapter.getDefaultInstance();

    @Override
    public void filter(ContainerRequestContext requestContext) {
        // Surround the tracing filter logic with a try/finally that guarantees the original tracing and MDC info found
        //      on the current thread at the beginning of this method is restored to this thread before this method
        //      returns. Otherwise there's the possibility of incorrect tracing information sticking around on this
        //      thread and potentially polluting other requests.

        TracingState originalThreadInfo = TracingState.getCurrentThreadTracingState();
        try {
            Span overallRequestSpan = createNewSpanForRequest(requestContext);
            // Put the new span's trace info into the request attributes.
            requestContext.setProperty(Span.class.getName(), overallRequestSpan);
            requestContext.setProperty(TracingState.class.getName(), TracingState.getCurrentThreadTracingState());

            // Handle request tagging.
            tagStrategy.handleRequestTagging(overallRequestSpan, requestContext, tagAdapter);
        }
        finally {
            unlinkTracingFromCurrentThread(originalThreadInfo);
        }
    }

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) {
        // TODO: Surround all of this with try/catch
        TracingState tracingState = (TracingState) requestContext.getProperty(TracingState.class.getName());

        runnableWithTracing(
            () -> {
                Span overallRequestSpan = (Span) requestContext.getProperty(Span.class.getName());

                // Add trace ID response header.
                responseContext.getHeaders().putSingle(TraceHeaders.TRACE_ID, overallRequestSpan.getTraceId());

                // Handle response/error tagging and final span name.
                // TODO: Is there any way to get a handle on any exception that may have occurred?
                tagStrategy.handleResponseTaggingAndFinalSpanName(
                    overallRequestSpan, requestContext, responseContext, null, tagAdapter
                );

                Tracer.getInstance().completeRequestSpan();
            },
            tracingState
        ).run();
    }

    protected Span createNewSpanForRequest(ContainerRequestContext request) {
        // See if there's trace info in the incoming request's headers. If so it becomes the parent trace.
        Tracer tracer = Tracer.getInstance();
        final Span parentSpan = HttpRequestTracingUtils.fromRequestWithHeaders(
            new JaxRs2RequestWithHeaders(request),
            null
        );
        Span newSpan;

        if (parentSpan != null) {
            logger.debug("Found parent Span {}", parentSpan);
            newSpan = tracer.startRequestWithChildSpan(
                parentSpan,
                getInitialSpanName(request, tagStrategy, tagAdapter)
            );
        }
        else {
            newSpan = tracer.startRequestWithRootSpan(
                getInitialSpanName(request, tagStrategy, tagAdapter),
                null
            );
            logger.debug("Parent span not found, starting a new span {}", newSpan);
        }
        return newSpan;
    }

    protected String getInitialSpanName(
        ContainerRequestContext request,
        HttpTagAndSpanNamingStrategy<ContainerRequestContext, ?> namingStrategy,
        HttpTagAndSpanNamingAdapter<ContainerRequestContext, ?> adapter
    ) {
        // Try the naming strategy first.
        String spanNameFromStrategy = namingStrategy.getInitialSpanName(request, adapter);

        if (StringUtils.isNotBlank(spanNameFromStrategy)) {
            return spanNameFromStrategy;
        }

        // The naming strategy didn't have anything for us. Fall back to something reasonable.
        return request.getMethod();
    }

}
