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

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Singleton;
import javax.json.Json;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.CompressInterceptor;
import com.baidu.hugegraph.api.filter.CompressInterceptor.Compress;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.metrics.MetricsUtil;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableSet;

@Path("gremlin")
@Singleton
public class GremlinAPI extends API {

    private static final Histogram gremlinInputHistogram =
            MetricsUtil.registerHistogram(GremlinAPI.class, "gremlin-input");
    private static final Histogram gremlinOutputHistogram =
            MetricsUtil.registerHistogram(GremlinAPI.class, "gremlin-output");

    private static final Set<String> INNER_EXCEPTIONS = ImmutableSet.of(
            "java.lang.SecurityException"
    );

    private Client client = ClientBuilder.newClient();

    private Response doGetRequest(String location, String auth, String query) {
        String url = String.format("%s?%s", location, query);
        Response r = this.client.target(url)
                                .request()
                                .header(HttpHeaders.AUTHORIZATION, auth)
                                .accept(MediaType.APPLICATION_JSON)
                                .acceptEncoding(CompressInterceptor.GZIP)
                                .get();
        if (r.getMediaType() != null) {
            // Append charset
            assert MediaType.APPLICATION_JSON_TYPE.equals(r.getMediaType());
            r.getHeaders().putSingle(HttpHeaders.CONTENT_TYPE,
                                     r.getMediaType().withCharset(CHARSET));
        }
        gremlinInputHistogram.update(query.length());
        gremlinOutputHistogram.update(r.getLength());
        return r;
    }

    private Response doPostRequest(String location, String auth, String req) {
        Entity<?> body = Entity.entity(req, MediaType.APPLICATION_JSON);
        Response r = this.client.target(location)
                                .request()
                                .header(HttpHeaders.AUTHORIZATION, auth)
                                .accept(MediaType.APPLICATION_JSON)
                                .acceptEncoding(CompressInterceptor.GZIP)
                                .post(body);
        if (r.getMediaType() != null) {
            // Append charset
            assert MediaType.APPLICATION_JSON_TYPE.equals(r.getMediaType());
            r.getHeaders().putSingle(HttpHeaders.CONTENT_TYPE,
                                     r.getMediaType().withCharset(CHARSET));
        }
        gremlinInputHistogram.update(req.length());
        gremlinOutputHistogram.update(r.getLength());
        return r;
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
        String location = conf.get(ServerOptions.GREMLIN_SERVER_URL);
        String auth = headers.getHeaderString(HttpHeaders.AUTHORIZATION);
        Response response = doPostRequest(location, auth, request);
        return transformResponseIfNeed(response);
    }

    @GET
    @Timed
    @Compress(buffer=(1024 * 40))
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Response get(@Context HugeConfig conf,
                        @Context HttpHeaders headers,
                        @Context UriInfo uriInfo) {
        String location = conf.get(ServerOptions.GREMLIN_SERVER_URL);
        String auth = headers.getHeaderString(HttpHeaders.AUTHORIZATION);
        String query = uriInfo.getRequestUri().getRawQuery();
        Response response = doGetRequest(location, auth, query);
        return transformResponseIfNeed(response);
    }

    private static Response transformResponseIfNeed(Response response) {
        int code = response.getStatusInfo().getStatusCode();
        // No need to transform
        if (code != Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()) {
            return response;
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> map = response.readEntity(Map.class);
        String message = (String) map.get("message");
        String exClassName = (String) map.get("Exception-Class");
        @SuppressWarnings("unchecked")
        List<String> exceptions = (List<String>) map.get("exceptions");
        if (message == null || exClassName == null || exceptions == null) {
            throw new IllegalStateException(String.format(
                      "Invalid response for inner exception, should contains " +
                      "Exception-Class, but got %s", map));
        }
        if (INNER_EXCEPTIONS.contains(exClassName)) {
            String cause = !exceptions.isEmpty() ? exceptions.get(0) : "";
            String json = Json.createObjectBuilder()
                              .add("exception", exClassName)
                              .add("message", message)
                              .add("cause", cause)
                              .build().toString();
            return Response.status(400)
                           .type(MediaType.APPLICATION_JSON)
                           .entity(json)
                           .build();
        } else {
            return response;
        }
    }
}
