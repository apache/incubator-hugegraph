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

import static com.baidu.hugegraph.config.ServerOptions.MAX_VERTICES_PER_BATCH;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.CompressInterceptor.Compress;
import com.baidu.hugegraph.api.filter.DecompressInterceptor.Decompress;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.server.HugeServer;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@Path("graphs/{graph}/graph/vertices")
@Singleton
public class VertexAPI extends API {

    private static final Logger LOG = Log.logger(HugeServer.class);

    @POST
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         JsonVertex jsonVertex) {
        E.checkArgumentNotNull(jsonVertex, "The request body can't be empty");

        LOG.debug("Graph [{}] create vertex: {}", graph, jsonVertex);

        Graph g = graph(manager, graph);
        Vertex vertex = g.addVertex(jsonVertex.properties());
        return manager.serializer(g).writeVertex(vertex);
    }

    @POST
    @Decompress
    @Path("batch")
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public List<String> create(@Context GraphManager manager,
                               @PathParam("graph") String graph,
                               List<JsonVertex> jsonVertices) {
        E.checkArgumentNotNull(jsonVertices,
                               "The request body can't be empty");

        HugeGraph g = (HugeGraph) graph(manager, graph);

        final int maxVertices = g.configuration().get(MAX_VERTICES_PER_BATCH);
        if (jsonVertices.size() > maxVertices) {
            throw new HugeException(
                      "Too many counts of vertices for one time post, " +
                      "the maximum number is '%s'", maxVertices);
        }

        LOG.debug("Graph [{}] create vertices: {}", graph, jsonVertices);

        List<String> ids = new ArrayList<>(jsonVertices.size());
        g.tx().open();
        try {
            for (JsonVertex vertex : jsonVertices) {
                ids.add(g.addVertex(vertex.properties()).id().toString());
            }
            g.tx().commit();
        } catch (Exception e1) {
            LOG.error("Failed to add vertices", e1);
            try {
                g.tx().rollback();
            } catch (Exception e2) {
                LOG.error("Failed to rollback vertices", e2);
            }
            throw new HugeException("Failed to add vertices", e1);
        } finally {
            g.tx().close();
        }
        return ids;
    }

    @GET
    @Compress
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @DefaultValue("100") @QueryParam("limit") long limit) {
        LOG.debug("Graph [{}] get vertices", graph);

        Graph g = graph(manager, graph);
        List<Vertex> rs = g.traversal().V().limit(limit).toList();
        return manager.serializer(g).writeVertices(rs);
    }

    @GET
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @PathParam("id") String id) {
        LOG.debug("Graph [{}] get vertex by id '{}'", graph, id);

        Graph g = graph(manager, graph);
        return manager.serializer(g).writeVertex(g.vertices(id).next());
    }

    @DELETE
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("id") String id) {
        LOG.debug("Graph [{}] remove vertex by id '{}'", graph, id);

        Graph g = graph(manager, graph);
        // TODO: add removeVertex(id) to improve
        g.vertices(id).next().remove();
    }

    @JsonIgnoreProperties(value = {"type"})
    private static class JsonVertex {

        @JsonProperty("id")
        public Object id;
        @JsonProperty("label")
        public String label;
        @JsonProperty("properties")
        public Map<String, Object> properties;
        @JsonProperty("type")
        public String type;

        public Object[] properties() {
            E.checkArgumentNotNull(this.label,
                                   "The label of vertex can't be null");
            E.checkArgumentNotNull(this.properties,
                                   "The properties of vertex can't be null");
            Object[] props = API.properties(this.properties);
            List<Object> list = new ArrayList<>(Arrays.asList(props));
            list.add(T.label);
            list.add(this.label);
            if (this.id != null) {
                list.add(T.id);
                list.add(this.id);
            }
            return list.toArray();
        }

        @Override
        public String toString() {
            return String.format("JsonVertex{label=%s, properties=%s}",
                                 this.label, this.properties);
        }
    }
}
