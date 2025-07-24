/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.api.auth;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.API;
import org.apache.hugegraph.api.filter.StatusFilter.Status;
import org.apache.hugegraph.auth.HugeUser;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.define.Checkable;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.util.StringEncoding;
import org.slf4j.Logger;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

@Path("graphspaces/{graphspace}/graphs/{graph}/auth/users")
@Singleton
@Tag(name = "UserAPI")
public class UserAPI extends API {

    private static final Logger LOG = Log.logger(UserAPI.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         @PathParam("graph") String graph,
                         JsonUser jsonUser) {
        LOG.debug("Graph [{}] create user: {}", graph, jsonUser);
        checkCreatingBody(jsonUser);

        HugeGraph g = graph(manager, graphSpace, graph);
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
                         @PathParam("graphspace") String graphSpace,
                         @PathParam("graph") String graph,
                         @PathParam("id") String id,
                         JsonUser jsonUser) {
        LOG.debug("Graph [{}] update user: {}", graph, jsonUser);
        checkUpdatingBody(jsonUser);

        HugeGraph g = graph(manager, graphSpace, graph);
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
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("graph") String graph,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOG.debug("Graph [{}] list users", graph);

        HugeGraph g = graph(manager, graphSpace, graph);
        List<HugeUser> users = manager.authManager().listAllUsers(limit);
        return manager.serializer(g).writeAuthElements("users", users);
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graphspace") String graphSpace,
                      @PathParam("graph") String graph,
                      @PathParam("id") String id) {
        LOG.debug("Graph [{}] get user: {}", graph, id);

        HugeGraph g = graph(manager, graphSpace, graph);
        HugeUser user = manager.authManager().getUser(IdGenerator.of(id));
        return manager.serializer(g).writeAuthElement(user);
    }

    @GET
    @Timed
    @Path("{id}/role")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String role(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("graph") String graph,
                       @PathParam("id") String id) {
        LOG.debug("Graph [{}] get user role: {}", graph, id);

        @SuppressWarnings("unused") // just check if the graph exists
        HugeGraph g = graph(manager, graphSpace, graph);
        HugeUser user = manager.authManager().getUser(IdGenerator.of(id));
        return manager.authManager().rolePermission(user).toJson();
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("graph") String graph,
                       @PathParam("id") String id) {
        LOG.debug("Graph [{}] delete user: {}", graph, id);

        @SuppressWarnings("unused") // just check if the graph exists
        HugeGraph g = graph(manager, graphSpace, graph);
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
            E.checkArgument(!StringUtils.isEmpty(this.name),
                            "The name of user can't be null");
            E.checkArgument(!StringUtils.isEmpty(this.password),
                            "The password of user can't be null");
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
