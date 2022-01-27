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

import com.baidu.hugegraph.auth.AuthManager;

import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.auth.HugeBelong;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.define.Checkable;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.logger.HugeGraphLogger;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@Path("graphspaces/{graphspace}/auth/belongs")
@Singleton
public class BelongAPI extends API {

    private static final HugeGraphLogger LOGGER
            = Log.getLogger(RestServer.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         JsonBelong jsonBelong) {
        LOGGER.logCustomDebug("Graph space [{}] create belong: {}",
                  graphSpace, jsonBelong);
        checkCreatingBody(jsonBelong);
        E.checkArgument(manager.graphSpace(graphSpace) != null,
                        "The graph space '%s' is not exist", graphSpace);

        HugeBelong belong = jsonBelong.build(graphSpace);
        AuthManager authManager = manager.authManager();
        belong.id(authManager.createBelong(graphSpace, belong, true));
        return manager.serializer().writeAuthElement(belong);
    }

    @PUT
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         @PathParam("id") String id,
                         JsonBelong jsonBelong) {
        LOGGER.logCustomDebug("Graph space [{}] update belong: {}",
                    RestServer.EXECUTOR,
                    graphSpace, jsonBelong);
        checkUpdatingBody(jsonBelong);
        E.checkArgument(manager.graphSpace(graphSpace) != null,
                        "The graph space '%s' is not exist", graphSpace);

        HugeBelong belong;
        AuthManager authManager = manager.authManager();
        try {
            belong = authManager.getBelong(graphSpace,
                                           UserAPI.parseId(id),
                                           true);
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid belong id: " + id);
        }
        belong = jsonBelong.build(belong);
        belong = authManager.updateBelong(graphSpace, belong, true);
        return manager.serializer().writeAuthElement(belong);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @QueryParam("user") String user,
                       @QueryParam("group") String group,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOGGER.logCustomDebug("Graph space [{}] list belongs by user {} or group {}",
                    RestServer.EXECUTOR,
                    graphSpace, user, group);
        E.checkArgument(user == null || group == null,
                        "Can't pass both user and group at the same time");

        List<HugeBelong> belongs;
        AuthManager authManager = manager.authManager();
        if (user != null) {
            Id id = UserAPI.parseId(user);
            belongs = authManager.listBelongByUser(graphSpace, id,
                                                   limit, true);
        } else if (group != null) {
            Id id = UserAPI.parseId(group);
            belongs = authManager.listBelongByGroup(graphSpace, id,
                                                    limit, true);
        } else {
            belongs = authManager.listAllBelong(graphSpace, limit, true);
        }
        return manager.serializer().writeAuthElements("belongs", belongs);
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graphspace") String graphSpace,
                      @PathParam("id") String id) {
        LOGGER.logCustomDebug("Graph space [{}] get belong: {}",
                    RestServer.EXECUTOR,
                    graphSpace, id);

        AuthManager authManager = manager.authManager();
        HugeBelong belong = authManager.getBelong(graphSpace,
                                                  UserAPI.parseId(id),
                                                  true);
        return manager.serializer().writeAuthElement(belong);
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("id") String id) {
        LOGGER.logCustomDebug("Graph space [{}] delete belong: {}",
                    RestServer.EXECUTOR,
                    graphSpace, id);

        try {
            AuthManager authManager = manager.authManager();
            authManager.deleteBelong(graphSpace, UserAPI.parseId(id), true);
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid belong id: " + id);
        }
    }

    @JsonIgnoreProperties(value = {"id", "belong_creator",
                                   "belong_create", "belong_update"})
    private static class JsonBelong implements Checkable {

        @JsonProperty("user")
        private String user;
        @JsonProperty("group")
        private String group;
        @JsonProperty("belong_description")
        private String description;

        public HugeBelong build(HugeBelong belong) {
            E.checkArgument(this.user == null ||
                            belong.source().equals(UserAPI.parseId(this.user)),
                            "The user of belong can't be updated");
            E.checkArgument(this.group == null ||
                            belong.target().equals(UserAPI.parseId(this.group)),
                            "The group of belong can't be updated");
            if (this.description != null) {
                belong.description(this.description);
            }
            return belong;
        }

        public HugeBelong build(String graphSpace) {
            HugeBelong belong = new HugeBelong(graphSpace,
                                               UserAPI.parseId(this.user),
                                               UserAPI.parseId(this.group));
            belong.description(this.description);
            return belong;
        }

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.user,
                                   "The user of belong can't be null");
            E.checkArgumentNotNull(this.group,
                                   "The group of belong can't be null");
        }

        @Override
        public void checkUpdate() {}
    }
}
