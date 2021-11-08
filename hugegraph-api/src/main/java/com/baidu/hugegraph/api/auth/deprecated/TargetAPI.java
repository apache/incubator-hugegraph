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

package com.baidu.hugegraph.api.auth.deprecated;

import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.auth.AuthManager;
import com.baidu.hugegraph.auth.HugeTarget;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.define.Checkable;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;

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
import java.util.List;
import java.util.Map;

@Path("graphspaces/{graphspace}/graphs/auth/targets")
@Singleton
public class TargetAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         JsonTarget jsonTarget) {
        LOG.debug("Graph space [{}] create target: {}", graphSpace, jsonTarget);
        checkCreatingBody(jsonTarget);

        HugeTarget target = jsonTarget.build(graphSpace);
        AuthManager authManager = manager.authManager();
        target.id(authManager.createTarget(graphSpace, target, true));
        return manager.serializer().writeAuthElement(target);
    }

    @PUT
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context GraphManager manager,
                         @PathParam("graphspace") String graphSpace,
                         @PathParam("id") String id,
                         JsonTarget jsonTarget) {
        LOG.debug("Graph space [{}] update target: {}", graphSpace, jsonTarget);
        checkUpdatingBody(jsonTarget);

        HugeTarget target;
        AuthManager authManager = manager.authManager();
        try {
            target = authManager.getTarget(graphSpace, UserAPI.parseId(id),
                                           true);
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid target id: " + id);
        }
        target = jsonTarget.build(target);
        authManager.updateTarget(graphSpace, target, true);
        return manager.serializer().writeAuthElement(target);
    }

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOG.debug("Graph space [{}] list targets", graphSpace);

        AuthManager authManager = manager.authManager();
        List<HugeTarget> targets = authManager.listAllTargets(graphSpace,
                                                              limit, true);
        return manager.serializer().writeAuthElements("targets", targets);
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graphspace") String graphSpace,
                      @PathParam("id") String id) {
        LOG.debug("Graph space [{}] get target: {}", graphSpace, id);

        AuthManager authManager = manager.authManager();
        HugeTarget target = authManager.getTarget(graphSpace,
                            UserAPI.parseId(id), true);
        return manager.serializer().writeAuthElement(target);
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graphspace") String graphSpace,
                       @PathParam("id") String id) {
        LOG.debug("Graph space [{}] delete target: {}", graphSpace, id);

        try {
            AuthManager authManager = manager.authManager();
            authManager.deleteTarget(graphSpace, UserAPI.parseId(id), true);
        } catch (NotFoundException e) {
            throw new IllegalArgumentException("Invalid target id: " + id);
        }
    }

    @JsonIgnoreProperties(value = {"id", "target_creator",
                                   "target_create", "target_update"})
    private static class JsonTarget implements Checkable {

        @JsonProperty("target_name")
        private String name;
        @JsonProperty("target_graph")
        private String graph;
        @JsonProperty("target_url")
        private String url;
        @JsonProperty("target_resources") // error when List<HugeResource>
        private List<Map<String, Object>> resources;

        public HugeTarget build(HugeTarget target) {
            E.checkArgument(this.name == null ||
                            target.name().equals(this.name),
                            "The name of target can't be updated");
            E.checkArgument(this.graph == null ||
                            target.graph().equals(this.graph),
                            "The graph of target can't be updated");
            if (this.url != null) {
                target.url(this.url);
            }
            if (this.resources != null) {
                target.resources(JsonUtil.toJson(this.resources));
            }
            return target;
        }

        public HugeTarget build(String graphSpace) {
            HugeTarget target = new HugeTarget(this.name, graphSpace,
                                               this.graph, this.url);
            if (this.resources != null) {
                target.resources(JsonUtil.toJson(this.resources));
            }
            return target;
        }

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.name,
                                   "The name of target can't be null");
            E.checkArgumentNotNull(this.graph,
                                   "The graph of target can't be null");
            E.checkArgumentNotNull(this.url,
                                   "The url of target can't be null");
        }

        @Override
        public void checkUpdate() {
            E.checkArgument(this.url != null ||
                            this.resources != null,
                            "Expect one of target url/resources");

        }
    }
}
