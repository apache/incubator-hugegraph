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
import com.baidu.hugegraph.auth.HugeGroup;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.define.Checkable;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@Path("graphs/{graph}/auth/groups")
@Singleton
public class GroupAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         JsonGroup jsonGroup) {
        LOG.debug("Graph [{}] create group: {}", graph, jsonGroup);
        checkCreatingBody(jsonGroup);

        HugeGraph g = graph(manager, graph);
        HugeGroup group = jsonGroup.build();
        group.id(manager.authManager().createGroup(group));
        return manager.serializer(g).writeAuthElement(group);
    }

    @PUT
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         @PathParam("id") String id,
                         JsonGroup jsonGroup) {
        LOG.debug("Graph [{}] update group: {}", graph, jsonGroup);
        checkUpdatingBody(jsonGroup);

        HugeGraph g = graph(manager, graph);
        HugeGroup group;
        try {
            group = manager.authManager().getGroup(UserAPI.parseId(id));
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid group id: " + id);
        }
        group = jsonGroup.build(group);
        manager.authManager().updateGroup(group);
        return manager.serializer(g).writeAuthElement(group);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOG.debug("Graph [{}] list groups", graph);

        HugeGraph g = graph(manager, graph);
        List<HugeGroup> groups = manager.authManager().listAllGroups(limit);
        return manager.serializer(g).writeAuthElements("groups", groups);
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @PathParam("id") String id) {
        LOG.debug("Graph [{}] get group: {}", graph, id);

        HugeGraph g = graph(manager, graph);
        HugeGroup group = manager.authManager().getGroup(IdGenerator.of(id));
        return manager.serializer(g).writeAuthElement(group);
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("id") String id) {
        LOG.debug("Graph [{}] delete group: {}", graph, id);

        @SuppressWarnings("unused") // just check if the graph exists
        HugeGraph g = graph(manager, graph);
        try {
            manager.authManager().deleteGroup(IdGenerator.of(id));
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid group id: " + id);
        }
    }

    @JsonIgnoreProperties(value = {"id", "group_creator",
                                   "group_create", "group_update"})
    private static class JsonGroup implements Checkable {

        @JsonProperty("group_name")
        private String name;
        @JsonProperty("group_description")
        private String description;

        public HugeGroup build(HugeGroup group) {
            E.checkArgument(this.name == null || group.name().equals(this.name),
                            "The name of group can't be updated");
            if (this.description != null) {
                group.description(this.description);
            }
            return group;
        }

        public HugeGroup build() {
            HugeGroup group = new HugeGroup(this.name);
            group.description(this.description);
            return group;
        }

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.name,
                                   "The name of group can't be null");
        }

        @Override
        public void checkUpdate() {
            // E.checkArgumentNotNull(this.description,
            //                       "The description of group can't be null");
        }
    }
}
