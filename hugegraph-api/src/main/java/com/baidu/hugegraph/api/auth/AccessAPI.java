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
import com.baidu.hugegraph.auth.HugeAccess;
import com.baidu.hugegraph.auth.HugePermission;
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

@Path("graphs/{graph}/auth/accesses")
@Singleton
public class AccessAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         JsonAccess jsonAccess) {
        LOG.debug("Graph [{}] create access: {}", graph, jsonAccess);
        checkCreatingBody(jsonAccess);

        HugeGraph g = graph(manager, graph);
        HugeAccess access = jsonAccess.build();
        access.id(manager.authManager().createAccess(access));
        return manager.serializer(g).writeUserElement(access);
    }

    @PUT
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         @PathParam("id") String id,
                         JsonAccess jsonAccess) {
        LOG.debug("Graph [{}] update access: {}", graph, jsonAccess);
        checkUpdatingBody(jsonAccess);

        HugeGraph g = graph(manager, graph);
        HugeAccess access;
        try {
            access = manager.authManager().getAccess(UserAPI.parseId(id));
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid access id: " + id);
        }
        access = jsonAccess.build(access);
        manager.authManager().updateAccess(access);
        return manager.serializer(g).writeUserElement(access);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @QueryParam("group") String group,
                       @QueryParam("target") String target,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOG.debug("Graph [{}] list belongs by group {} or target {}",
                  graph, group, target);
        E.checkArgument(group == null || target == null,
                        "Can't pass both group and target at the same time");

        HugeGraph g = graph(manager, graph);
        List<HugeAccess> belongs;
        if (group != null) {
            Id id = UserAPI.parseId(group);
            belongs = manager.authManager().listAccessByGroup(id, limit);
        } else if (target != null) {
            Id id = UserAPI.parseId(target);
            belongs = manager.authManager().listAccessByTarget(id, limit);
        } else {
            belongs = manager.authManager().listAllAccess(limit);
        }
        return manager.serializer(g).writeUserElements("accesses", belongs);
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @PathParam("id") String id) {
        LOG.debug("Graph [{}] get access: {}", graph, id);

        HugeGraph g = graph(manager, graph);
        HugeAccess access = manager.authManager().getAccess(UserAPI.parseId(id));
        return manager.serializer(g).writeUserElement(access);
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("id") String id) {
        LOG.debug("Graph [{}] delete access: {}", graph, id);

        @SuppressWarnings("unused") // just check if the graph exists
        HugeGraph g = graph(manager, graph);
        try {
            manager.authManager().deleteAccess(UserAPI.parseId(id));
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid access id: " + id);
        }
    }

    @JsonIgnoreProperties(value = {"id", "access_creator",
                                   "access_create", "access_update"})
    private static class JsonAccess implements Checkable {

        @JsonProperty("group")
        private String group;
        @JsonProperty("target")
        private String target;
        @JsonProperty("access_permission")
        private HugePermission permission;
        @JsonProperty("access_description")
        private String description;

        public HugeAccess build(HugeAccess access) {
            E.checkArgument(this.group == null ||
                            access.source().equals(UserAPI.parseId(this.group)),
                            "The group of access can't be updated");
            E.checkArgument(this.target == null ||
                            access.target().equals(UserAPI.parseId(this.target)),
                            "The target of access can't be updated");
            E.checkArgument(this.permission == null ||
                            access.permission().equals(this.permission),
                            "The permission of access can't be updated");
            if (this.description != null) {
                access.description(this.description);
            }
            return access;
        }

        public HugeAccess build() {
            HugeAccess access = new HugeAccess(UserAPI.parseId(this.group),
                                               UserAPI.parseId(this.target));
            access.permission(this.permission);
            access.description(this.description);
            return access;
        }

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.group,
                                   "The group of access can't be null");
            E.checkArgumentNotNull(this.target,
                                   "The target of access can't be null");
            E.checkArgumentNotNull(this.permission,
                                   "The permission of access can't be null");
        }

        @Override
        public void checkUpdate() {
            E.checkArgumentNotNull(this.description,
                                   "The description of access can't be null");
        }
    }
}
