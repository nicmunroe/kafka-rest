package io.confluent.kafkarest.distributedtracing.wingtips;

import com.nike.wingtips.http.RequestWithHeaders;

import javax.validation.constraints.NotNull;
import javax.ws.rs.container.ContainerRequestContext;

/**
 * TODO: Class Description
 */
public class JaxRs2RequestWithHeaders implements RequestWithHeaders {

    private final @NotNull ContainerRequestContext request;

    public JaxRs2RequestWithHeaders(@NotNull ContainerRequestContext request) {
        this.request = request;
    }

    @Override
    public String getHeader(String headerName) {
        return request.getHeaders().getFirst(headerName);
    }

    @Override
    public Object getAttribute(String name) {
        return request.getProperty(name);
    }
}
