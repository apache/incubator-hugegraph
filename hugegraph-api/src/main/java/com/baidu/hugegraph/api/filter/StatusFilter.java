package com.baidu.hugegraph.api.filter;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import javax.ws.rs.NameBinding;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.ext.Provider;

@Provider
public class StatusFilter implements ContainerResponseFilter {

    @Override
    public void filter(ContainerRequestContext requestContext,
                       ContainerResponseContext responseContext)
                       throws IOException {
        if (responseContext.getStatus() == 200) {
            for (Annotation i : responseContext.getEntityAnnotations()) {
                if (i instanceof Status) {
                    responseContext.setStatus(((Status) i).value());
                    break;
                }
            }
        }
    }

    @NameBinding
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Status {
        final int OK = 200;
        final int CREATED = 201;

        int value();
    }
}