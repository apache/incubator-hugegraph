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

import jakarta.inject.Singleton;
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

@Path("graphs/{graph}/auth/belongs")
@Singleton
public class BelongAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         JsonBelong jsonBelong) {
        LOG.debug("Graph [{}] create belong: {}", graph, jsonBelong);
        checkCreatingBody(jsonBelong);

        HugeGraph g = graph(manager, graph);
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
                         @PathParam("graph") String graph,
                         @PathParam("id") String id,
                         JsonBelong jsonBelong) {
        LOG.debug("Graph [{}] update belong: {}", graph, jsonBelong);
        checkUpdatingBody(jsonBelong);

        HugeGraph g = graph(manager, graph);
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
                       @PathParam("graph") String graph,
                       @QueryParam("user") String user,
                       @QueryParam("group") String group,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOG.debug("Graph [{}] list belongs by user {} or group {}",
                  graph, user, group);
        E.checkArgument(user == null || group == null,
                        "Can't pass both user and group at the same time");

        HugeGraph g = graph(manager, graph);
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
                      @PathParam("graph") String graph,
                      @PathParam("id") String id) {
        LOG.debug("Graph [{}] get belong: {}", graph, id);

        HugeGraph g = graph(manager, graph);
        HugeBelong belong = manager.authManager().getBelong(UserAPI.parseId(id));
        return manager.serializer(g).writeAuthElement(belong);
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("id") String id) {
        LOG.debug("Graph [{}] delete belong: {}", graph, id);

        @SuppressWarnings("unused") // just check if the graph exists
        HugeGraph g = graph(manager, graph);
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
        public void checkUpdate() {
            E.checkArgumentNotNull(this.description,
                                   "The description of belong can't be null");
        }
    }
}
