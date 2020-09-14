package io.confluent.kafkarest.distributedtracing.wingtips;

import com.nike.internal.util.StringUtils;
import com.nike.wingtips.tags.HttpTagAndSpanNamingAdapter;

import org.glassfish.jersey.server.ExtendedUriInfo;
import org.glassfish.jersey.uri.UriTemplate;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.core.UriInfo;

/**
 * Extension of {@link HttpTagAndSpanNamingAdapter} that knows how to handle JAX-RS 2 {@link ContainerRequestContext}
 * and {@link ContainerResponseContext} objects.
 */
public class JaxRs2RequestTagAdapter
    extends HttpTagAndSpanNamingAdapter<ContainerRequestContext, ContainerResponseContext> {

    @SuppressWarnings("WeakerAccess")
    protected static final JaxRs2RequestTagAdapter DEFAULT_INSTANCE = new JaxRs2RequestTagAdapter();

    /**
     * @return A reusable, thread-safe, singleton instance of this class that can be used by anybody who wants to use
     * this class and does not need any customization.
     */
    public static JaxRs2RequestTagAdapter getDefaultInstance() {
        return DEFAULT_INSTANCE;
    }

    /**
     * Since this class represents server requests/responses (not clients), we only want to consider HTTP status codes
     * greater than or equal to 500 to be an error. From a server's perspective, a 4xx response is the correct
     * response to a bad request, and should therefore not be considered an error (again, from the server's
     * perspective - the client may feel differently).
     *
     * @param response The response object.
     * @return The value of {@link #getResponseHttpStatus(ContainerResponseContext)} if it is greater than or equal to
     * 500, or null otherwise.
     */
    @Override
    public @Nullable String getErrorResponseTagValue(@Nullable ContainerResponseContext response) {
        Integer statusCode = getResponseHttpStatus(response);
        if (statusCode != null && statusCode >= 500) {
            return statusCode.toString();
        }

        // Status code does not indicate an error, so return null.
        return null;
    }

    @Override
    public @Nullable String getRequestUrl(@Nullable ContainerRequestContext request) {
        if (request == null) {
            return null;
        }

        return request.getUriInfo().getRequestUri().toString();
    }

    @Override
    public @Nullable Integer getResponseHttpStatus(@Nullable ContainerResponseContext response) {
        if (response == null) {
            return null;
        }

        return response.getStatus();
    }

    @Override
    public @Nullable String getRequestHttpMethod(@Nullable ContainerRequestContext request) {
        if (request == null) {
            return null;
        }

        return request.getMethod();
    }


    @Override
    public @Nullable String getRequestPath(@Nullable ContainerRequestContext request) {
        if (request == null) {
            return null;
        }

        return request.getUriInfo().getAbsolutePath().getPath();
    }

    @Override
    public @Nullable String getRequestUriPathTemplate(
        @Nullable ContainerRequestContext request,
        @Nullable ContainerResponseContext response
    ) {
        String result = route(request);
        if (StringUtils.isBlank(result)) {
            return null;
        }

        return result;
    }

    /**
     * NOTE: This code was pulled from Wingtips'
     * <a href="https://github.com/Nike-Inc/wingtips/blob/6115f79fcd9342862191f79020d9d83f8b5b5fc7/wingtips-jersey2/src/main/java/com/nike/wingtips/jersey2/SpanCustomizingApplicationEventListener.java">SpanCustomizingApplicationEventListener</a>
     * which was in turn mostly copied from Zipkin's
     * <a href="https://github.com/openzipkin/brave/blob/1cffdc124647643800f624f0499dabffcabf649b/instrumentation/jersey-server/src/main/java/brave/jersey/server/SpanCustomizingApplicationEventListener.java">SpanCustomizingApplicationEventListener</a>.
     *
     * <p>This returns the matched template as defined by a base URL and path expressions.
     *
     * <p>Matched templates are pairs of (resource path, method path) added with
     * {@code RoutingContext#pushTemplates(UriTemplate, UriTemplate)}.
     * This code skips redundant slashes from either source caused by Path("/") or Path("").
     *
     * @return The URI path template (route) pulled from the given request, or blank string "" if it could not
     * be found.
     */
    protected @NotNull String route(@Nullable ContainerRequestContext request) {
        if (request == null) {
            return "";
        }

        UriInfo uriInfo = request.getUriInfo();
        if (!(uriInfo instanceof ExtendedUriInfo)) {
            return "";
        }
        List<UriTemplate> templates = ((ExtendedUriInfo)uriInfo).getMatchedTemplates();
        int templateCount = templates.size();
        if (templateCount == 0) {
            return "";
        }
        StringBuilder builder = null; // don't allocate unless you need it!
        String basePath = uriInfo.getBaseUri().getPath();
        String result = null;

        if (!"/".equals(basePath)) { // skip empty base paths
            result = basePath;
        }
        for (int i = templateCount - 1; i >= 0; i--) {
            String template = templates.get(i).getTemplate();
            if ("/".equals(template)) {
                continue; // skip allocation
            }
            if (builder != null) {
                builder.append(template);
            }
            else if (result != null) {
                builder = new StringBuilder(result).append(template);
                result = null;
            }
            else {
                result = template;
            }
        }

        return (result != null)
               ? result
               : (builder != null)
                 ? builder.toString()
                 : "";
    }

    @Override
    public @Nullable String getHeaderSingleValue(@Nullable ContainerRequestContext request, @NotNull String headerKey) {
        if (request == null) {
            return null;
        }

        return request.getHeaders().getFirst(headerKey);
    }

    @Override
    public @Nullable List<String> getHeaderMultipleValue(
        @Nullable ContainerRequestContext request, @NotNull String headerKey
    ) {
        if (request == null) {
            return null;
        }

        return request.getHeaders().get(headerKey);
    }

    @Override
    public @Nullable String getSpanHandlerTagValue(
        @Nullable ContainerRequestContext request, @Nullable ContainerResponseContext response
    ) {
        return "jaxrs2";
    }
}
