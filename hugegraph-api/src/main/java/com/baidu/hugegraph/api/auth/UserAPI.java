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

import java.util.List;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.auth.HugeUser;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.define.Checkable;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.baidu.hugegraph.util.StringEncoding;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@Path("graphs/auth/users")
@Singleton
public class UserAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         JsonUser jsonUser) {
        LOG.debug("Graph [{}] create user: {}", SYSTEM_GRAPH, jsonUser);
        checkCreatingBody(jsonUser);

        HugeGraph g = graph(manager, SYSTEM_GRAPH);
        HugeUser user = jsonUser.build();
        user.id(manager.authManager().createUser(user));
        return manager.serializer(g).writeAuthElement(user);
    }

    @PUT
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context GraphManager manager,
                         @PathParam("id") String id,
                         JsonUser jsonUser) {
        LOG.debug("Graph [{}] update user: {}", SYSTEM_GRAPH, jsonUser);
        checkUpdatingBody(jsonUser);

        HugeGraph g = graph(manager, SYSTEM_GRAPH);
        HugeUser user;
        try {
            user = manager.authManager().getUser(UserAPI.parseId(id));
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid user id: " + id);
        }
        user = jsonUser.build(user);
        manager.authManager().updateUser(user);
        return manager.serializer(g).writeAuthElement(user);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOG.debug("Graph [{}] list users", SYSTEM_GRAPH);

        HugeGraph g = graph(manager, SYSTEM_GRAPH);
        List<HugeUser> users = manager.authManager().listAllUsers(limit);
        return manager.serializer(g).writeAuthElements("users", users);
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("id") String id) {
        LOG.debug("Graph [{}] get user: {}", SYSTEM_GRAPH, id);

        HugeGraph g = graph(manager, SYSTEM_GRAPH);
        HugeUser user = manager.authManager().getUser(IdGenerator.of(id));
        return manager.serializer(g).writeAuthElement(user);
    }

    @GET
    @Timed
    @Path("{id}/role")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String role(@Context GraphManager manager,
                       @PathParam("id") String id) {
        LOG.debug("Graph [{}] get user role: {}", SYSTEM_GRAPH, id);

        @SuppressWarnings("unused") // just check if the graph exists
        HugeGraph g = graph(manager, SYSTEM_GRAPH);
        HugeUser user = manager.authManager().getUser(IdGenerator.of(id));
        return manager.authManager().rolePermission(user).toJson();
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("id") String id) {
        LOG.debug("Graph [{}] delete user: {}", SYSTEM_GRAPH, id);

        @SuppressWarnings("unused") // just check if the graph exists
        HugeGraph g = graph(manager, SYSTEM_GRAPH);
        try {
            manager.authManager().deleteUser(IdGenerator.of(id));
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid user id: " + id);
        }
    }

    protected static Id parseId(String id) {
        return IdGenerator.of(id);
    }

    @JsonIgnoreProperties(value = {"id", "user_creator",
                                   "user_create", "user_update"})
    private static class JsonUser implements Checkable {

        @JsonProperty("user_name")
        private String name;
        @JsonProperty("user_password")
        private String password;
        @JsonProperty("user_phone")
        private String phone;
        @JsonProperty("user_email")
        private String email;
        @JsonProperty("user_avatar")
        private String avatar;
        @JsonProperty("user_description")
        private String description;

        public HugeUser build(HugeUser user) {
            E.checkArgument(this.name == null || user.name().equals(this.name),
                            "The name of user can't be updated");
            if (this.password != null) {
                user.password(StringEncoding.hashPassword(this.password));
            }
            if (this.phone != null) {
                user.phone(this.phone);
            }
            if (this.email != null) {
                user.email(this.email);
            }
            if (this.avatar != null) {
                user.avatar(this.avatar);
            }
            if (this.description != null) {
                user.description(this.description);
            }
            return user;
        }

        public HugeUser build() {
            HugeUser user = new HugeUser(this.name);
            user.password(StringEncoding.hashPassword(this.password));
            user.phone(this.phone);
            user.email(this.email);
            user.avatar(this.avatar);
            user.description(this.description);
            return user;
        }

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgument(!StringUtils.isEmpty(this.name) &&
                            this.name.matches(USER_NAME_PATTERN),
                            "The name is 6-18 characters and can only contain letters, numbers or underscores");
            E.checkArgument(!StringUtils.isEmpty(this.password) &&
                            this.password.matches(USER_PASSWORD_PATTERN),
                            "The password is 8-16 characters, which can be letters, numbers or special symbols");
        }

        @Override
        public void checkUpdate() {
            E.checkArgument(!StringUtils.isEmpty(this.password) ||
                            this.phone != null ||
                            this.email != null ||
                            this.avatar != null,
                            "Expect one of user password/phone/email/avatar]");
        }
    }
}
