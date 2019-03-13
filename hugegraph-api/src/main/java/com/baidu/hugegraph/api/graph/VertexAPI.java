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
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@Path("graphs/{graph}/graph/vertices")
@Singleton
public class VertexAPI extends BatchAPI {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         JsonVertex jsonVertex) {
        LOG.debug("Graph [{}] create vertex: {}", graph, jsonVertex);
        checkCreatingBody(jsonVertex);

        HugeGraph g = graph(manager, graph);
        Vertex vertex = commit(g, () -> g.addVertex(jsonVertex.properties()));

        return manager.serializer(g).writeVertex(vertex);
    }

    @POST
    @Timed
    @Decompress
    @Path("batch")
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public List<String> create(@Context HugeConfig config,
                               @Context GraphManager manager,
                               @PathParam("graph") String graph,
                               List<JsonVertex> jsonVertices) {
        LOG.debug("Graph [{}] create vertices: {}", graph, jsonVertices);
        checkCreatingBody(jsonVertices);

        HugeGraph g = graph(manager, graph);
        checkBatchSize(config, jsonVertices);

        return this.commit(config, g, jsonVertices.size(), () -> {
            List<String> ids = new ArrayList<>(jsonVertices.size());
            for (JsonVertex vertex : jsonVertices) {
                ids.add(g.addVertex(vertex.properties()).id().toString());
            }
            return ids;
        });
    }

    @PUT
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         @PathParam("id") String idValue,
                         @QueryParam("action") String action,
                         JsonVertex jsonVertex) {
        LOG.debug("Graph [{}] update vertex: {}", graph, jsonVertex);
        checkUpdatingBody(jsonVertex);

        Id id = checkAndParseVertexId(idValue);
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
            for (Map.Entry<String, Object> e :
                 jsonVertex.properties.entrySet()) {
                String key = e.getKey();
                Object value = e.getValue();
                if (append) {
                    vertex.property(key, value);
                } else {
                    vertex.property(key).remove();
                }
            }
        });

        return manager.serializer(g).writeVertex(vertex);
    }

    @GET
    @Timed
    @Compress
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @QueryParam("label") String label,
                       @QueryParam("properties") String properties,
                       @QueryParam("offset") @DefaultValue("0") long offset,
                       @QueryParam("page") String page,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOG.debug("Graph [{}] query vertices by label: {}, properties: {}, " +
                  "offset: {}, page: {}, limit: {}",
                  graph, label, properties, offset, page, limit);

        Map<String, Object> props = parseProperties(properties);
        if (page != null) {
            E.checkArgument(offset == 0,
                            "Not support querying vertices based on paging " +
                            "and offset together");
            E.checkArgument(props.size() <= 1,
                            "Not support querying vertices based on paging " +
                            "and more than one property");
        }

        HugeGraph g = graph(manager, graph);

        GraphTraversal<Vertex, Vertex> traversal = g.traversal().V();
        if (label != null) {
            traversal = traversal.hasLabel(label);
        }

        for (Map.Entry<String, Object> entry : props.entrySet()) {
            traversal = traversal.has(entry.getKey(), entry.getValue());
        }

        if (page == null) {
            traversal = traversal.range(offset, offset + limit);
        } else {
            traversal = traversal.has("~page", page).limit(limit);
        }

        return manager.serializer(g).writeVertices(traversal, page != null);
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @PathParam("id") String idValue) {
        LOG.debug("Graph [{}] get vertex by id '{}'", graph, idValue);

        Id id = checkAndParseVertexId(idValue);
        HugeGraph g = graph(manager, graph);
        Iterator<Vertex> vertices = g.vertices(id);
        checkExist(vertices, HugeType.VERTEX, idValue);
        return manager.serializer(g).writeVertex(vertices.next());
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("id") String idValue) {
        LOG.debug("Graph [{}] remove vertex by id '{}'", graph, idValue);

        Id id = checkAndParseVertexId(idValue);
        HugeGraph g = graph(manager, graph);
        // TODO: add removeVertex(id) to improve
        commit(g, () -> {
            Iterator<Vertex> itor = g.vertices(id);
            E.checkArgument(itor.hasNext(),
                            "No such vertex with id: '%s'", idValue);
            itor.next().remove();
        });
    }

    public static Id checkAndParseVertexId(String idValue) {
        if (idValue == null) {
            return null;
        }
        try {
            Object id = JsonUtil.fromJson(idValue, Object.class);
            return HugeVertex.getIdValue(id);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format(
                      "The vertex id must be formatted as String or " +
                      "Number, but got '%s'", idValue));
        }
    }

    private static void checkBatchSize(HugeConfig config,
                                       List<JsonVertex> vertices) {
        int max = config.get(ServerOptions.MAX_VERTICES_PER_BATCH);
        if (vertices.size() > max) {
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
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.label,
                                   "The label of vertex can't be null");
            this.checkUpdate();
        }

        @Override
        public void checkUpdate() {
            E.checkArgumentNotNull(this.properties,
                                   "The properties of vertex can't be null");

            for (Map.Entry<String, Object> e : this.properties.entrySet()) {
                String key = e.getKey();
                Object value = e.getValue();
                E.checkArgumentNotNull(value, "Not allowed to set value of " +
                                       "property '%s' to null for vertex '%s'",
                                       key, this.id);
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
