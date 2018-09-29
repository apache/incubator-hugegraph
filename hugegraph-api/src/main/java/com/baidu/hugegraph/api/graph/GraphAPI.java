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

package com.baidu.hugegraph.api.graph;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.function.TriFunction;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.filter.DecompressInterceptor.Decompress;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.api.schema.Checkable;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

@Path("graphs/{graph}/graph")
@Singleton
public class GraphAPI extends BatchAPI {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Decompress
    @Path("batch")
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public Map<String, List<Object>> create(@Context HugeConfig config,
                                            @Context GraphManager manager,
                                            @PathParam("graph") String graph,
                                            @QueryParam("check_vertex")
                                            @DefaultValue("true")
                                            boolean checkVertex,
                                            JsonGraph jsonGraph) {
        List<VertexAPI.JsonVertex> jsonVertices = jsonGraph.vertices;
        LOG.debug("Graph [{}] create vertices: {}", graph, jsonVertices);
        checkCreatingBody(jsonVertices);

        List<EdgeAPI.JsonEdge> jsonEdges = jsonGraph.edges;
        LOG.debug("Graph [{}] create edges: {}", graph, jsonEdges);
        checkCreatingBody(jsonEdges);

        HugeGraph g = graph(manager, graph);
        VertexAPI.checkBatchSize(config, jsonVertices);
        EdgeAPI.checkBatchSize(config, jsonEdges);

        TriFunction<HugeGraph, Object, String, Vertex> getVertex =
                    checkVertex ? EdgeAPI::getVertex : EdgeAPI::newVertex;

        final int size = jsonVertices.size() + jsonEdges.size();
        return this.commit(config, g, size, () -> {
            List<Object> vertexIds = new ArrayList<>(jsonVertices.size());
            for (VertexAPI.JsonVertex jsonVertex : jsonVertices) {
                Vertex vertex = g.addVertex(jsonVertex.properties());
                vertexIds.add(vertex.id().toString());
            }

            List<Object> edgeIds = new ArrayList<>(jsonEdges.size());
            for (EdgeAPI.JsonEdge jsonEdge : jsonEdges) {
                /*
                 * NOTE: If the query param 'checkVertex' is false,
                 * then the label is correct and not matched id,
                 * it will be allowed currently
                 */
                Vertex srcVertex = getVertex.apply(g, jsonEdge.source,
                                                   jsonEdge.sourceLabel);
                Vertex tgtVertex = getVertex.apply(g, jsonEdge.target,
                                                   jsonEdge.targetLabel);
                Edge edge = srcVertex.addEdge(jsonEdge.label, tgtVertex,
                                              jsonEdge.properties());
                edgeIds.add(edge.id().toString());
            }
            return ImmutableMap.of("vertices", vertexIds, "edges", edgeIds);
        });
    }

    public static class JsonGraph implements Checkable {

        @JsonProperty("vertices")
        public List<VertexAPI.JsonVertex> vertices;
        @JsonProperty("edges")
        public List<EdgeAPI.JsonEdge> edges;

        @Override
        public void checkCreate(boolean isBatch) {

        }
    }
}
