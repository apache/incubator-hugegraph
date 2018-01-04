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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.CompressInterceptor.Compress;
import com.baidu.hugegraph.api.filter.DecompressInterceptor.Decompress;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.api.schema.Checkable;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@Path("graphs/{graph}/graph/vertices")
@Singleton
public class VertexAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         JsonVertex jsonVertex) {
        LOG.debug("Graph [{}] create vertex: {}", graph, jsonVertex);
        checkBody(jsonVertex);

        HugeGraph g = graph(manager, graph);
        Vertex vertex = commit(g, () -> g.addVertex(jsonVertex.properties()));

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
        LOG.debug("Graph [{}] create vertices: {}", graph, jsonVertices);
        checkBody(jsonVertices);

        HugeGraph g = graph(manager, graph);
        checkBatchCount(g, jsonVertices);

        return commit(g, () -> {
            List<String> ids = new ArrayList<>(jsonVertices.size());
            for (JsonVertex vertex : jsonVertices) {
                ids.add(g.addVertex(vertex.properties()).id().toString());
            }
            return ids;
        });
    }

    @PUT
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         @PathParam("id") String id,
                         @QueryParam("action") String action,
                         JsonVertex jsonVertex) {
        LOG.debug("Graph [{}] update vertex: {}", graph, jsonVertex);
        checkBody(jsonVertex);

        if (jsonVertex.id != null) {
            E.checkArgument(id.equals(jsonVertex.id),
                            "The ids are different between url('%s') and " +
                            "request body('%s')", id, jsonVertex.id);
        }

        // Parse action param
        boolean append = checkAndParseAction(action);

        HugeGraph g = graph(manager, graph);
        HugeVertex vertex = (HugeVertex) g.vertices(id).next();
        VertexLabel vertexLabel = vertex.schemaLabel();

        for (String key : jsonVertex.properties.keySet()) {
            PropertyKey pkey = g.propertyKey(key);
            E.checkArgument(vertexLabel.properties().contains(pkey.id()),
                            "Can't update property for vertex '%s' because " +
                            "there is no property key '%s' in its vertex label",
                            id, key);
        }

        commit(g, () -> {
            for (String key : jsonVertex.properties.keySet()) {
                if (append) {
                    vertex.property(key, jsonVertex.properties.get(key));
                } else {
                    vertex.property(key).remove();
                }
            }
        });

        return manager.serializer(g).writeVertex(vertex);
    }

    @GET
    @Compress
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @QueryParam("label") String label,
                       @QueryParam("properties") String properties,
                       @DefaultValue("100") @QueryParam("limit") long limit) {
        LOG.debug("Graph [{}] query vertices by label: {}, properties: {}",
                  graph, label, properties);

        Map<String, Object> props = parseProperties(properties);

        HugeGraph g = graph(manager, graph);

        GraphTraversal<Vertex, Vertex> traversal = g.traversal().V();
        if (label != null) {
            traversal = traversal.hasLabel(label);
        }

        for (Map.Entry<String, Object> entry : props.entrySet()) {
            traversal = traversal.has(entry.getKey(), entry.getValue());
        }

        List<Vertex> vertices = traversal.limit(limit).toList();
        return manager.serializer(g).writeVertices(vertices);
    }

    @GET
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @PathParam("id") String id) {
        LOG.debug("Graph [{}] get vertex by id '{}'", graph, id);

        HugeGraph g = graph(manager, graph);
        Iterator<Vertex> vertices = g.vertices(id);
        checkExist(vertices, HugeType.VERTEX, id);
        return manager.serializer(g).writeVertex(vertices.next());
    }

    @DELETE
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("id") String id) {
        LOG.debug("Graph [{}] remove vertex by id '{}'", graph, id);

        HugeGraph g = graph(manager, graph);
        // TODO: add removeVertex(id) to improve
        commit(g, () -> g.vertices(id).next().remove());
    }

    private static void checkBatchCount(HugeGraph g,
                                        List<JsonVertex> jsonVertices) {
        int max = g.configuration().get(ServerOptions.MAX_VERTICES_PER_BATCH);
        if (jsonVertices.size() > max) {
            throw new IllegalArgumentException(String.format(
                      "Too many counts of vertices for one time post, " +
                      "the maximum number is '%s'", max));
        }
    }

    @JsonIgnoreProperties(value = {"type"})
    private static class JsonVertex implements Checkable {

        @JsonProperty("id")
        public Object id;
        @JsonProperty("label")
        public String label;
        @JsonProperty("properties")
        public Map<String, Object> properties;
        @JsonProperty("type")
        public String type;

        @Override
        public void check(boolean isBatch) {
            E.checkArgumentNotNull(this.label,
                                   "The label of vertex can't be null");
            E.checkArgumentNotNull(this.properties,
                                   "The properties of vertex can't be null");

            for (String key : this.properties.keySet()) {
                Object value = this.properties.get(key);
                E.checkArgumentNotNull(value, "Not allowed to set value of " +
                                       "property '%s' to null for vertex '%s'",
                                       key, id);
            }
        }

        public Object[] properties() {
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
