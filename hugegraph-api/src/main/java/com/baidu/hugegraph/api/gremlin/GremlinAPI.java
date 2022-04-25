/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.api.gremlin;

import java.util.Map;
import java.util.Set;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;

import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.CompressInterceptor.Compress;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.exception.HugeGremlinException;
import com.baidu.hugegraph.metrics.MetricsUtil;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

@Path("gremlin")
@Singleton
public class GremlinAPI extends API {

    private static final Histogram gremlinInputHistogram =
            MetricsUtil.registerHistogram(GremlinAPI.class, "gremlin-input");
    private static final Histogram gremlinOutputHistogram =
            MetricsUtil.registerHistogram(GremlinAPI.class, "gremlin-output");

    private static final Set<String> FORBIDDEN_REQUEST_EXCEPTIONS =
            ImmutableSet.of("java.lang.SecurityException",
                            "jakarta.ws.rs.ForbiddenException");
    private static final Set<String> BAD_REQUEST_EXCEPTIONS = ImmutableSet.of(
            "java.lang.IllegalArgumentException",
            "java.util.concurrent.TimeoutException",
            "groovy.lang.",
            "org.codehaus.",
            "com.baidu.hugegraph."
    );

    @Context
    private Provider<HugeConfig> configProvider;

    private GremlinClient client;

    public GremlinClient client() {
        if (this.client != null) {
            return this.client;
        }
        HugeConfig config = this.configProvider.get();
        String url = config.get(ServerOptions.GREMLIN_SERVER_URL);
        int timeout = config.get(ServerOptions.GREMLIN_SERVER_TIMEOUT) * 1000;
        int maxRoutes = config.get(ServerOptions.GREMLIN_SERVER_MAX_ROUTE);
        this.client = new GremlinClient(url, timeout, maxRoutes, maxRoutes);
        return this.client;
    }

    @POST
    @Timed
    @Compress
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Response post(@Context HugeConfig conf,
                         @Context HttpHeaders headers,
                         String request) {
        /* The following code is reserved for forwarding request */
        // context.getRequestDispatcher(location).forward(request, response);
        // return Response.seeOther(UriBuilder.fromUri(location).build())
        // .build();
        // Response.temporaryRedirect(UriBuilder.fromUri(location).build())
        // .build();
        String auth = headers.getHeaderString(HttpHeaders.AUTHORIZATION);
        Response response = this.client().doPostRequest(auth, request);
        gremlinInputHistogram.update(request.length());
        gremlinOutputHistogram.update(response.getLength());
        return transformResponseIfNeeded(response);
    }

    @GET
    @Timed
    @Compress(buffer=(1024 * 40))
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Response get(@Context HugeConfig conf,
                        @Context HttpHeaders headers,
                        @Context UriInfo uriInfo) {
        String auth = headers.getHeaderString(HttpHeaders.AUTHORIZATION);
        String query = uriInfo.getRequestUri().getRawQuery();
        MultivaluedMap<String, String> params = uriInfo.getQueryParameters();
        Response response = this.client().doGetRequest(auth, params);
        gremlinInputHistogram.update(query.length());
        gremlinOutputHistogram.update(response.getLength());
        return transformResponseIfNeeded(response);
    }

    private static Response transformResponseIfNeeded(Response response) {
        MediaType mediaType = response.getMediaType();
        if (mediaType != null) {
            // Append charset
            assert MediaType.APPLICATION_JSON_TYPE.equals(mediaType);
            response.getHeaders().putSingle(HttpHeaders.CONTENT_TYPE,
                                            mediaType.withCharset(CHARSET));
        }

        Response.StatusType status = response.getStatusInfo();
        if (status.getStatusCode() < 400) {
            // No need to transform if normal response without error
            return response;
        }

        if (mediaType == null || !JSON.equals(mediaType.getSubtype())) {
            String message = response.readEntity(String.class);
            throw new HugeGremlinException(status.getStatusCode(),
                                           ImmutableMap.of("message", message));
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> map = response.readEntity(Map.class);
        String exClassName = (String) map.get("Exception-Class");
        if (FORBIDDEN_REQUEST_EXCEPTIONS.contains(exClassName)) {
            status = Response.Status.FORBIDDEN;
        } else if (matchBadRequestException(exClassName)) {
            status = Response.Status.BAD_REQUEST;
        }
        throw new HugeGremlinException(status.getStatusCode(), map);
    }

    private static boolean matchBadRequestException(String exClass) {
        if (exClass == null) {
            return false;
        }
        if (BAD_REQUEST_EXCEPTIONS.contains(exClass)) {
            return true;
        }
        return BAD_REQUEST_EXCEPTIONS.stream().anyMatch(exClass::startsWith);
    }
}
