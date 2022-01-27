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
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;

import com.baidu.hugegraph.auth.AuthManager;
import com.baidu.hugegraph.auth.HugeUser;
import com.baidu.hugegraph.util.StringEncoding;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

@Path("auth")
@Singleton
public class LoginAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    private static final DateFormat DATE_FORMAT =
                                    new SimpleDateFormat("yyyyMMdd");

    @POST
    @Timed
    @Path("login")
    @Status(StatusFilter.Status.OK)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String login(@Context GraphManager manager,
                        JsonLogin jsonLogin) {
        LOG.debug("User login: {}", jsonLogin);
        checkCreatingBody(jsonLogin);

        try {
            AuthManager authManager = manager.authManager();
            String token = authManager.loginUser(jsonLogin.name,
                                                 jsonLogin.password,
                                                 jsonLogin.expire);
            return manager.serializer()
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
                       @HeaderParam(HttpHeaders.AUTHORIZATION) String auth) {
        E.checkArgument(StringUtils.isNotEmpty(auth),
                        "Request header Authorization must not be null");
        LOG.debug("User logout: {}", auth);

        if (!auth.startsWith(AuthenticationFilter.BEARER_TOKEN_PREFIX)) {
            throw new BadRequestException(
                  "Only HTTP Bearer authentication is supported");
        }

        String token = auth.substring(AuthenticationFilter.BEARER_TOKEN_PREFIX
                                                          .length());
        AuthManager authManager = manager.authManager();
        authManager.logoutUser(token);
    }

    @GET
    @Timed
    @Path("verify")
    @Status(StatusFilter.Status.OK)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String verifyToken(@Context GraphManager manager,
                              @HeaderParam(HttpHeaders.AUTHORIZATION)
                              String token) {
        E.checkArgument(StringUtils.isNotEmpty(token),
                        "Request header Authorization must not be null");
        LOG.debug("verify token: {}", token);

        if (!token.startsWith(AuthenticationFilter.BEARER_TOKEN_PREFIX)) {
            throw new BadRequestException(
                      "Only HTTP Bearer authentication is supported");
        }

        token = token.substring(AuthenticationFilter.BEARER_TOKEN_PREFIX
                                                    .length());
        AuthManager authManager = manager.authManager();
        UserWithRole userWithRole = authManager.validateUser(token);

        return manager.serializer()
                      .writeMap(ImmutableMap.of(AuthConstant.TOKEN_USER_NAME,
                                                userWithRole.username(),
                                                AuthConstant.TOKEN_USER_ID,
                                                userWithRole.userId()));
    }

    @POST
    @Timed
    @Path("kglogin")
    @Status(StatusFilter.Status.OK)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String kgLogin(@Context GraphManager manager,
                          KgJsonLogin jsonLogin) {
        LOG.debug("Kg user login: {}", jsonLogin);
        checkCreatingBody(jsonLogin);
        String content = String.format("%s:%s", jsonLogin.name,
                                       DATE_FORMAT.format(new Date()));
        String sign = DigestUtils.md5Hex(content).toLowerCase();
        E.checkArgument(sign.equals(jsonLogin.sign.toLowerCase()),
                        "Invalid signature");

        AuthManager authManager = manager.authManager();
        HugeUser user = authManager.findUser(jsonLogin.name, false);
        if (user == null) {
            LOG.debug("Kg user not exist: {}, try to register.", jsonLogin);
            user = new HugeUser(jsonLogin.name);
            user.password(StringEncoding.hashPassword(user.name()));
            user.description("KG user");
            authManager.createKgUser(user);
        }
        String token = authManager.createToken(jsonLogin.name, jsonLogin.expire);
        return manager.serializer()
                      .writeMap(ImmutableMap.of("token", token));
    }

    private static class JsonLogin implements Checkable {

        @JsonProperty("user_name")
        private String name;
        @JsonProperty("user_password")
        private String password;
        @JsonProperty("token_expire")
        private long expire;

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgument(!StringUtils.isEmpty(this.name) &&
                            this.name.matches(USER_NAME_PATTERN),
                            "The name is 5-16 characters " +
                            "and can only contain letters, " +
                            "numbers or underscores");
            E.checkArgument(!StringUtils.isEmpty(this.password) &&
                            this.password.matches(USER_PASSWORD_PATTERN),
                            "The password is 5-16 characters, " +
                            "which can be letters, numbers or " +
                            "special symbols");
            E.checkArgument(this.expire >= 0 &&
                            this.expire <= Long.MAX_VALUE,
                            "The token_expire should be in " +
                            "[0, Long.MAX_VALUE]");
        }

        @Override
        public void checkUpdate() {}
    }

    private static class KgJsonLogin implements Checkable {

        @JsonProperty("user_name")
        private String name;
        @JsonProperty("token_expire")
        private long expire = 31536000;
        @JsonProperty("sign")
        private String sign;

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgument(!StringUtils.isEmpty(this.name) &&
                            this.name.matches(USER_NAME_PATTERN),
                            "The name is 5-16 characters " +
                            "and can only contain letters, " +
                            "numbers or underscores");
            E.checkArgument(this.expire >= 0 &&
                            this.expire <= Long.MAX_VALUE,
                            "The token_expire should be in " +
                            "[0, Long.MAX_VALUE]");
        }

        @Override
        public void checkUpdate() {}
    }
}
