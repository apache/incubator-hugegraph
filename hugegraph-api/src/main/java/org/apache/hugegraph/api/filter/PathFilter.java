package org.apache.hugegraph.api.filter;

import java.io.IOException;

import jakarta.inject.Singleton;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.ext.Provider;

@Provider
@Singleton
@PreMatching
public class PathFilter implements ContainerRequestFilter {


    @Override
    public void filter(ContainerRequestContext context)
            throws IOException {

        context.setProperty("RequestTime", System.currentTimeMillis());

    }
}
