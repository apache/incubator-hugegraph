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

package com.baidu.hugegraph.api.profile;

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.security.RolesAllowed;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.NotSupportedException;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.SecurityContext;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.auth.HugeAuthenticator.RequiredPerm;
import com.baidu.hugegraph.auth.HugePermission;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.type.define.GraphMode;
import com.baidu.hugegraph.type.define.GraphReadMode;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;

@Path("graphs")
@Singleton
public class GraphsAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    private static final String CONFIRM_CLEAR = "I'm sure to delete all data";

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$dynamic"})
    public Object list(@Context GraphManager manager,
                       @Context SecurityContext sc) {
        Set<String> graphs = manager.graphs();
        // Filter by user role
        Set<String> filterGraphs = new HashSet<>();
        for (String graph : graphs) {
            String role = RequiredPerm.roleFor(graph, HugePermission.READ);
            if (sc.isUserInRole(role)) {
                try {
                    HugeGraph g = graph(manager, graph);
                    filterGraphs.add(g.name());
                } catch (ForbiddenException ignored) {
                    // ignore
                }
            }
        }
        return ImmutableMap.of("graphs", filterGraphs);
    }

    @GET
    @Timed
    @Path("{graph}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph"})
    public Object get(@Context GraphManager manager,
                      @PathParam("graph") String graph) {
        LOG.debug("Get graph by name '{}'", graph);

        HugeGraph g = graph(manager, graph);
        return ImmutableMap.of("name", g.name(), "backend", g.backend());
    }

    @GET
    @Timed
    @Path("{graph}/conf")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    public File getConf(@Context GraphManager manager,
                        @PathParam("graph") String graph) {
        LOG.debug("Get graph configuration by name '{}'", graph);

        HugeGraph g = graph4admin(manager, graph);

        HugeConfig config = (HugeConfig) g.configuration();
        File file = config.getFile();
        if (file == null) {
            throw new NotSupportedException("Can't access the api in " +
                      "a node which started with non local file config.");
        }
        return file;
    }

    @DELETE
    @Timed
    @Path("{graph}/clear")
    @Consumes(APPLICATION_JSON)
    @RolesAllowed("admin")
    public void clear(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @QueryParam("confirm_message") String message) {
        LOG.debug("Clear graph by name '{}'", graph);

        HugeGraph g = graph(manager, graph);

        if (!CONFIRM_CLEAR.equals(message)) {
            throw new IllegalArgumentException(String.format(
                      "Please take the message: %s", CONFIRM_CLEAR));
        }
        g.truncateBackend();
        // truncateBackend() will open tx, so must close here(commit)
        g.tx().commit();
    }

    @PUT
    @Timed
    @Path("{graph}/snapshot_create")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph"})
    public Object createSnapshot(@Context GraphManager manager,
                                 @PathParam("graph") String graph) {
        LOG.debug("Create snapshot for graph '{}'", graph);

        HugeGraph g = graph(manager, graph);
        g.createSnapshot();
        return ImmutableMap.of(graph, "snapshot_created");
    }

    @PUT
    @Timed
    @Path("{graph}/snapshot_resume")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph"})
    public Object resumeSnapshot(@Context GraphManager manager,
                                 @PathParam("graph") String graph) {
        LOG.debug("Resume snapshot for graph '{}'", graph);

        HugeGraph g = graph(manager, graph);
        g.resumeSnapshot();
        return ImmutableMap.of(graph, "snapshot_resumed");
    }

    @PUT
    @Timed
    @Path("{graph}/compact")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public String compact(@Context GraphManager manager,
                          @PathParam("graph") String graph) {
        LOG.debug("Manually compact graph '{}'", graph);

        HugeGraph g = graph(manager, graph);
        return JsonUtil.toJson(g.metadata(null, "compact"));
    }

    @PUT
    @Timed
    @Path("{graph}/mode")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph"})
    public Map<String, GraphMode> mode(@Context GraphManager manager,
                                       @PathParam("graph") String graph,
                                       GraphMode mode) {
        LOG.debug("Set mode to: '{}' of graph '{}'", mode, graph);

        E.checkArgument(mode != null, "Graph mode can't be null");
        HugeGraph g = graph(manager, graph);
        g.mode(mode);
        // mode(m) might trigger tx open, must close(commit)
        g.tx().commit();
        return ImmutableMap.of("mode", mode);
    }

    @GET
    @Timed
    @Path("{graph}/mode")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph"})
    public Map<String, GraphMode> mode(@Context GraphManager manager,
                                       @PathParam("graph") String graph) {
        LOG.debug("Get mode of graph '{}'", graph);

        HugeGraph g = graph(manager, graph);
        return ImmutableMap.of("mode", g.mode());
    }

    @PUT
    @Timed
    @Path("{graph}/graph_read_mode")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    public Map<String, GraphReadMode> graphReadMode(
                                      @Context GraphManager manager,
                                      @PathParam("graph") String graph,
                                      GraphReadMode readMode) {
        LOG.debug("Set graph-read-mode to: '{}' of graph '{}'",
                  readMode, graph);

        E.checkArgument(readMode != null,
                        "Graph-read-mode can't be null");
        HugeGraph g = graph(manager, graph);
        g.readMode(readMode);
        return ImmutableMap.of("graph_read_mode", readMode);
    }

    @GET
    @Timed
    @Path("{graph}/graph_read_mode")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=$graph"})
    public Map<String, GraphReadMode> graphReadMode(
                                      @Context GraphManager manager,
                                      @PathParam("graph") String graph) {
        LOG.debug("Get graph-read-mode of graph '{}'", graph);

        HugeGraph g = graph(manager, graph);
        return ImmutableMap.of("graph_read_mode", g.readMode());
    }
}
