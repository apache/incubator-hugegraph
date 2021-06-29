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

package com.baidu.hugegraph.api.auth;

import javax.inject.Singleton;
import javax.security.sasl.AuthenticationException;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.AuthenticationFilter;
import com.baidu.hugegraph.api.filter.StatusFilter;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.auth.AuthConstant;
import com.baidu.hugegraph.auth.UserWithRole;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.define.Checkable;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

@Path("graphs/{graph}/auth")
@Singleton
public class LoginAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Path("login")
    @Status(StatusFilter.Status.OK)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String login(@Context GraphManager manager,
                        @PathParam("graph") String graph,
                        JsonLogin jsonLogin) {
        LOG.debug("Graph [{}] user login: {}", graph, jsonLogin);
        checkCreatingBody(jsonLogin);

        try {
            String token = manager.authManager()
                                  .loginUser(jsonLogin.name, jsonLogin.password);
            HugeGraph g = graph(manager, graph);
            return manager.serializer(g)
                          .writeMap(ImmutableMap.of("token", token));
        } catch (AuthenticationException e) {
            throw new NotAuthorizedException(e.getMessage(), e);
        }
    }

    @DELETE
    @Timed
    @Path("logout")
    @Status(StatusFilter.Status.OK)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public void logout(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @HeaderParam(HttpHeaders.AUTHORIZATION) String auth) {
        E.checkArgument(StringUtils.isNotEmpty(auth),
                        "Request header Authorization must not be null");
        LOG.debug("Graph [{}] user logout: {}", graph, auth);

        if (!auth.startsWith(AuthenticationFilter.BEARER_TOKEN_PREFIX)) {
            throw new BadRequestException(
                  "Only HTTP Bearer authentication is supported");
        }

        String token = auth.substring(AuthenticationFilter.BEARER_TOKEN_PREFIX
                                                          .length());

        manager.authManager().logoutUser(token);
    }

    @GET
    @Timed
    @Path("verify")
    @Status(StatusFilter.Status.OK)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String verifyToken(@Context GraphManager manager,
                              @PathParam("graph") String graph,
                              @HeaderParam(HttpHeaders.AUTHORIZATION)
                              String token) {
        E.checkArgument(StringUtils.isNotEmpty(token),
                        "Request header Authorization must not be null");
        LOG.debug("Graph [{}] get user: {}", graph, token);

        if (!token.startsWith(AuthenticationFilter.BEARER_TOKEN_PREFIX)) {
            throw new BadRequestException(
                      "Only HTTP Bearer authentication is supported");
        }

        token = token.substring(AuthenticationFilter.BEARER_TOKEN_PREFIX
                                                    .length());
        UserWithRole userWithRole = manager.authManager().validateUser(token);

        HugeGraph g = graph(manager, graph);
        return manager.serializer(g)
                      .writeMap(ImmutableMap.of(AuthConstant.TOKEN_USER_NAME,
                                                userWithRole.username(),
                                                AuthConstant.TOKEN_USER_ID,
                                                userWithRole.userId()));
    }

    private static class JsonLogin implements Checkable {

        @JsonProperty("user_name")
        private String name;
        @JsonProperty("user_password")
        private String password;

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgument(!StringUtils.isEmpty(this.name),
                            "The name of user can't be null");
            E.checkArgument(!StringUtils.isEmpty(this.password),
                            "The password of user can't be null");
        }

        @Override
        public void checkUpdate() {
            // pass
        }
    }
}
