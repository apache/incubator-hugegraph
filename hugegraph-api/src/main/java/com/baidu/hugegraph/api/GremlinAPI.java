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
 * License for the specific language governing permissions and limitations under
 * the License.
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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import com.baidu.hugegraph.api.filter.CompressInterceptor.Compress;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;

@Path("gremlin")
@Singleton
public class GremlinAPI extends API {

    private Client client = ClientBuilder.newClient();

    private Response doGetRequest(String location, String query) {
        return this.client.target(String.format("%s?%s", location, query))
                   .request()
                   .accept(MediaType.APPLICATION_JSON)
                   .acceptEncoding("gzip")
                   .get();
    }

    private Response doPostRequest(String location, Object request) {
        return this.client.target(location)
                   .request()
                   .accept(MediaType.APPLICATION_JSON)
                   .acceptEncoding("gzip")
                   .post(Entity.entity(request, MediaType.APPLICATION_JSON));
    }

    private Response doPostRequest(String location, String request) {
        return doPostRequest(location, (Object) request);
    }

    @POST
    @Compress
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response post(@Context HugeConfig conf, String request) {
        /* The following code is reserved for forwarding request */
        // context.getRequestDispatcher(location).forward(request, response);
        // return Response.seeOther(UriBuilder.fromUri(location).build())
        // .build();
        // Response.temporaryRedirect(UriBuilder.fromUri(location).build())
        // .build();
        String location = conf.get(ServerOptions.GREMLIN_SERVER_URL);
        return doPostRequest(location, request);
    }

    @GET
    @Compress(buffer=(1024 * 40))
    @Produces(MediaType.APPLICATION_JSON)
    public Response get(@Context HugeConfig conf,
                        @Context UriInfo uriInfo) {
        String location = conf.get(ServerOptions.GREMLIN_SERVER_URL);
        return doGetRequest(location, uriInfo.getRequestUri().getRawQuery());
    }

    static class GremlinRequest {
        // See org.apache.tinkerpop.gremlin.server.channel.HttpChannelizer
        public String gremlin;
        public Map<String, Object> bindings;
        public String language;
        public Map<String, String> aliases;
    }
}
