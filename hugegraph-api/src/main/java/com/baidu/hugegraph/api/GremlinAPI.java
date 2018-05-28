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

package com.baidu.hugegraph.api;

import java.util.Map;

import javax.inject.Singleton;
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

import com.baidu.hugegraph.api.filter.CompressInterceptor;
import com.baidu.hugegraph.api.filter.CompressInterceptor.Compress;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.codahale.metrics.annotation.Timed;

@Path("gremlin")
@Singleton
public class GremlinAPI extends API {

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
        return r;
    }

    private Response doPostRequest(String location, String auth, Object req) {
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
        return doPostRequest(location, auth, request);
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
        return doGetRequest(location, auth, query);
    }

    static class GremlinRequest {
        // See org.apache.tinkerpop.gremlin.server.channel.HttpChannelizer
        public String gremlin;
        public Map<String, Object> bindings;
        public String language;
        public Map<String, String> aliases;
    }
}
