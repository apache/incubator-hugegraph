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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.security.RolesAllowed;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.auth.HugeAuthenticator.RequiredPerm;
import com.baidu.hugegraph.auth.HugePermission;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.type.define.GraphMode;
import com.baidu.hugegraph.type.define.GraphReadMode;
import com.baidu.hugegraph.util.ConfigUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;

@Path("graphs")
@Singleton
public class GraphsAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    private static final String GRAPH_ACTION = "action";
    private static final String CONFIRM_MESSAGE = "confirm_message";
    private static final String GRAPH_ACTION_CLEAR = "clear";

    private static final String CONFIRM_CLEAR = "I'm sure to delete all data";
    private static final String CONFIRM_DROP = "I'm sure to drop the graph";

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

    @POST
    @Timed
    @Path("{name}")
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public Object create(@Context GraphManager manager,
                         @PathParam("name") String name,
                         String configText) {
        LOG.debug("Create graph {} with config options '{}'", name, configText);
        HugeGraph graph = manager.createGraph(name, configText, true);
        graph.tx().close();
        return ImmutableMap.of("name", name, "backend", graph.backend());
    }

    @GET
    @Timed
    @Path("{graph}/conf")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    public String getConf(@Context GraphManager manager,
                          @PathParam("graph") String graph) {
        LOG.debug("Get graph configuration by name '{}'", graph);

        HugeGraph g = graph4admin(manager, graph);

        HugeConfig config = (HugeConfig) g.configuration();
        return ConfigUtil.writeConfigToString(config);
    }

    @PUT
    @Timed
    @Path("{name}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    public Map<String, String> clear(
                               @Context GraphManager manager,
                               @PathParam("name") String name,
                               Map<String, String> actionMap) {
        LOG.debug("Clear graph by name '{}'", name);
        E.checkArgument(actionMap != null && actionMap.size() == 2 &&
                        actionMap.containsKey(GRAPH_ACTION) &&
                        actionMap.containsKey(CONFIRM_MESSAGE),
                        "Please pass '%s' and '%s' for graph clear",
                        GRAPH_ACTION, CONFIRM_MESSAGE);
        String action = actionMap.get(GRAPH_ACTION);
        E.checkArgument(GRAPH_ACTION_CLEAR.equals(action),
                        "Not support graph action: '%s'", action);
        String message = actionMap.get(CONFIRM_MESSAGE);
        E.checkArgument(CONFIRM_CLEAR.equals(message),
                        "Please take the message: %s", CONFIRM_CLEAR);
        HugeGraph g = graph(manager, name);
        g.truncateBackend();
        // truncateBackend() will open tx, so must close here(commit)
        g.tx().commit();
        return ImmutableMap.of(name, "cleared");
    }

    @DELETE
    @Timed
    @Path("{name}")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin"})
    public void delete(@Context GraphManager manager,
                       @PathParam("name") String name,
                       @QueryParam("confirm_message") String message) {
        LOG.debug("Remove graph by name '{}'", name);
        E.checkArgument(CONFIRM_DROP.equals(message),
                        "Please take the message: %s", CONFIRM_DROP);
        manager.dropGraph(name, true);
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
