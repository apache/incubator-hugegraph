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

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotSupportedException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.ImmutableMap;

@Path("graphs")
@Singleton
public class GraphsAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    private static final String CONFIRM_CLEAR = "I'm sure to delete all data";

    @GET
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Object list(@Context GraphManager manager) {
        return ImmutableMap.of("graphs", manager.graphs().keySet());
    }

    @GET
    @Path("{name}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Object get(@Context GraphManager manager,
                      @PathParam("name") String name) {
        LOG.debug("Graphs [{}] get graph by name '{}'", name);

        HugeGraph g = graph(manager, name);
        return ImmutableMap.of("name", g.name());
    }

    @GET
    @Path("{name}/conf")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public File getConf(@Context GraphManager manager,
                        @PathParam("name") String name,
                        @QueryParam("token") String token) {
        LOG.debug("Graphs [{}] get graph by name '{}'", name);

        HugeGraph g = graph(manager, name);

        if (!verifyToken(g, token)) {
            throw new NotAuthorizedException("Invalid token");
        }

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
    public void clear(@Context GraphManager manager,
                      @PathParam("name") String name,
                      @QueryParam("token") String token,
                      @QueryParam("confirm_message") String message) {
        LOG.debug("Graphs [{}] clear graph by name '{}'", name);

        HugeGraph g = graph(manager, name);

        if (!verifyToken(g, token)) {
            throw new NotAuthorizedException("Invalid token");
        }
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

    private boolean verifyToken(HugeGraph graph, String token) {
        String expected = graph.configuration().get(ServerOptions.ADMIN_TOKEN);
        return expected.equals(token);
    }
}
