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

package com.baidu.hugegraph.api;

import java.io.File;
import java.util.Set;

import javax.annotation.security.RolesAllowed;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.NotSupportedException;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.SecurityContext;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.util.Log;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@Path("graphs")
@Singleton
public class GraphsAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    private static final String CONFIRM_CLEAR = "I'm sure to delete all data";

    @GET
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$dynamic"})
    public Object list(@Context GraphManager manager,
                       @Context SecurityContext sc) {
        Set<String> graphs = manager.graphs().keySet();
        String role = sc.getUserPrincipal().getName();
        if (role.equals("admin")) {
            return ImmutableMap.of("graphs", graphs);
        } else {
            // Filter by user role
            String graph = role;
            if (graphs.contains(graph)) {
                return ImmutableMap.of("graphs", ImmutableList.of(graph));
            } else {
                return ImmutableMap.of("graphs", ImmutableList.of());
            }
        }
    }

    @GET
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed({"admin", "$owner=name"})
    public Object get(@Context GraphManager manager,
                      @PathParam("name") String name) {
        LOG.debug("Get graph by name '{}'", name);

        HugeGraph g = graph(manager, name);
        return ImmutableMap.of("name", g.name(), "backend", g.backend());
    }

    @GET
    @Path("{name}/conf")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    public File getConf(@Context GraphManager manager,
                        @PathParam("name") String name) {
        LOG.debug("Get graph configuration by name '{}'", name);

        HugeGraph g = graph(manager, name);

        File file = g.configuration().getFile();
        if (file == null) {
            throw new NotSupportedException("Can't access the api in " +
                      "a node which started with non local file config.");
        }
        return file;
    }

    @DELETE
    @Path("{name}/clear")
    @Consumes(APPLICATION_JSON)
    @RolesAllowed("admin")
    public void clear(@Context GraphManager manager,
                      @PathParam("name") String name,
                      @QueryParam("confirm_message") String message) {
        LOG.debug("Clear graph by name '{}'", name);

        HugeGraph g = graph(manager, name);

        if (!CONFIRM_CLEAR.equals(message)) {
            throw new IllegalArgumentException(String.format(
                      "Please take the message: %s", CONFIRM_CLEAR));
        }

        // Clear vertex and edge
        commit(g, () -> {
            g.traversal().E().toStream().forEach(Edge::remove);
            g.traversal().V().toStream().forEach(Vertex::remove);
        });

        // Schema operation will auto commit
        SchemaManager schema = g.schema();
        schema.getIndexLabels().forEach(elem -> {
            schema.indexLabel(elem.name()).remove();
        });
        schema.getEdgeLabels().forEach(elem -> {
            schema.edgeLabel(elem.name()).remove();
        });
        schema.getVertexLabels().forEach(elem -> {
            schema.vertexLabel(elem.name()).remove();
        });
        schema.getPropertyKeys().forEach(elem -> {
            schema.propertyKey(elem.name()).remove();
        });
    }

    @PUT
    @Path("{name}/restoring")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    public Object restoring(@Context GraphManager manager,
                            @PathParam("name") String name,
                            JsonRestoring jsonRestoring) {
        LOG.debug("Set restoring status to: '{}' of graph '{}'",
                  jsonRestoring, name);

        HugeGraph g = graph(manager, name);
        g.restoring(jsonRestoring.restoring);
        return jsonRestoring;
    }

    @GET
    @Path("{name}/restoring")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @RolesAllowed("admin")
    public Object restoring(@Context GraphManager manager,
                            @PathParam("name") String name) {
        LOG.debug("Get restoring status of graph '{}'", name);

        HugeGraph g = graph(manager, name);
        return ImmutableMap.of("restoring", g.restoring());
    }

    private static class JsonRestoring {

        @JsonProperty("restoring")
        public boolean restoring;
    }
}
