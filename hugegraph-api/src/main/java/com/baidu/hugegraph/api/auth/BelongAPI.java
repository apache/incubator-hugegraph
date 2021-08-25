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

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.auth.HugeBelong;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.define.Checkable;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@Path("graphs/auth/belongs")
@Singleton
public class BelongAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         JsonBelong jsonBelong) {
        LOG.debug("Graph [{}] create belong: {}", SYSTEM_GRAPH, jsonBelong);
        checkCreatingBody(jsonBelong);

        HugeGraph g = graph(manager, SYSTEM_GRAPH);
        HugeBelong belong = jsonBelong.build();
        belong.id(manager.authManager().createBelong(belong));
        return manager.serializer(g).writeAuthElement(belong);
    }

    @PUT
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context GraphManager manager,
                         @PathParam("id") String id,
                         JsonBelong jsonBelong) {
        LOG.debug("Graph [{}] update belong: {}", SYSTEM_GRAPH, jsonBelong);
        checkUpdatingBody(jsonBelong);

        HugeGraph g = graph(manager, SYSTEM_GRAPH);
        HugeBelong belong;
        try {
            belong = manager.authManager().getBelong(UserAPI.parseId(id));
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid belong id: " + id);
        }
        belong = jsonBelong.build(belong);
        manager.authManager().updateBelong(belong);
        return manager.serializer(g).writeAuthElement(belong);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @QueryParam("user") String user,
                       @QueryParam("group") String group,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOG.debug("Graph [{}] list belongs by user {} or group {}",
                SYSTEM_GRAPH, user, group);
        E.checkArgument(user == null || group == null,
                        "Can't pass both user and group at the same time");

        HugeGraph g = graph(manager, SYSTEM_GRAPH);
        List<HugeBelong> belongs;
        if (user != null) {
            Id id = UserAPI.parseId(user);
            belongs = manager.authManager().listBelongByUser(id, limit);
        } else if (group != null) {
            Id id = UserAPI.parseId(group);
            belongs = manager.authManager().listBelongByGroup(id, limit);
        } else {
            belongs = manager.authManager().listAllBelong(limit);
        }
        return manager.serializer(g).writeAuthElements("belongs", belongs);
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("id") String id) {
        LOG.debug("Graph [{}] get belong: {}", SYSTEM_GRAPH, id);

        HugeGraph g = graph(manager, SYSTEM_GRAPH);
        HugeBelong belong = manager.authManager().getBelong(UserAPI.parseId(id));
        return manager.serializer(g).writeAuthElement(belong);
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("id") String id) {
        LOG.debug("Graph [{}] delete belong: {}", SYSTEM_GRAPH, id);

        @SuppressWarnings("unused") // just check if the graph exists
        HugeGraph g = graph(manager, SYSTEM_GRAPH);
        try {
            manager.authManager().deleteBelong(UserAPI.parseId(id));
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

        public HugeBelong build() {
            HugeBelong belong = new HugeBelong(UserAPI.parseId(this.user),
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
