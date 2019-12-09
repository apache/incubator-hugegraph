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
import java.util.HashMap;
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
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.define.UpdateStrategy;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.traversal.optimize.Text;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.IdStrategy;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;

@Path("graphs/{graph}/graph/vertices")
@Singleton
public class VertexAPI extends BatchAPI {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed(name = "single-create")
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
    @Timed(name = "batch-create")
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
        checkBatchSize(config, jsonVertices);

        HugeGraph g = graph(manager, graph);

        return this.commit(config, g, jsonVertices.size(), () -> {
            List<String> ids = new ArrayList<>(jsonVertices.size());
            for (JsonVertex vertex : jsonVertices) {
                ids.add(g.addVertex(vertex.properties()).id().toString());
            }
            return ids;
        });
    }

    /**
     * Batch update steps like:
     * 1. Get all newVertices' ID & combine first
     * 2. Get all oldVertices & update
     * 3. Add the final vertex together
     */
    @PUT
    @Timed(name = "batch-update")
    @Decompress
    @Path("batch")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context HugeConfig config,
                         @Context GraphManager manager,
                         @PathParam("graph") String graph,
                         BatchVertexRequest req) {
        BatchVertexRequest.checkUpdate(req);
        LOG.debug("Graph [{}] update vertices: {}", graph, req);
        checkUpdatingBody(req.jsonVertices);
        checkBatchSize(config, req.jsonVertices);

        HugeGraph g = graph(manager, graph);
        Map<Id, JsonVertex> map = new HashMap<>(req.jsonVertices.size());

        return this.commit(config, g, map.size(), () -> {
            /*
             * 1.Put all newVertices' properties into map (combine first)
             * - Consider primary-key & user-define ID mode first
             */
            req.jsonVertices.forEach(newVertex -> {
                Id newVertexId = getVertexId(g, newVertex);
                JsonVertex oldVertex = map.get(newVertexId);
                this.updateExistElement(oldVertex, newVertex,
                                        req.updateStrategies);
                map.put(newVertexId, newVertex);
            });

            // 2.Get all oldVertices and update with new vertices
            Object[] ids = map.keySet().toArray();
            Iterator<Vertex> oldVertices = g.vertices(ids);
            oldVertices.forEachRemaining(oldVertex -> {
                JsonVertex newVertex = map.get(oldVertex.id());
                this.updateExistElement(g, oldVertex, newVertex,
                                        req.updateStrategies);
            });

            // 3.Add finalVertices and return them
            List<Vertex> vertices = new ArrayList<>(map.size());
            map.values().forEach(finalVertex -> {
                vertices.add(g.addVertex(finalVertex.properties()));
            });

            // If return ids, the ids.size() maybe different with the origins'
            return manager.serializer(g)
                          .writeVertices(vertices.iterator(), false);
        });
    }

    @PUT
    @Timed(name = "single-update")
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

        commit(g, () -> updateProperties(vertex, jsonVertex, append));

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
            Iterator<Vertex> iter = g.vertices(id);
            E.checkArgument(iter.hasNext(),
                            "No such vertex with id: '%s'", idValue);
            iter.next().remove();
        });
    }

    public static Id checkAndParseVertexId(String idValue) {
        if (idValue == null) {
            return null;
        }
        boolean uuid = idValue.startsWith("U\"");
        if (uuid) {
            idValue = idValue.substring(1);
        }
        try {
            Object id = JsonUtil.fromJson(idValue, Object.class);
            return uuid ? Text.uuid((String) id) : HugeVertex.getIdValue(id);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format(
                      "The vertex id must be formatted as Number/String/UUID" +
                      ", but got '%s'", idValue));
        }
    }

    private static void checkBatchSize(HugeConfig config,
                                       List<JsonVertex> vertices) {
        int max = config.get(ServerOptions.MAX_VERTICES_PER_BATCH);
        if (vertices.size() > max) {
            throw new IllegalArgumentException(String.format(
                      "Too many vertices for one time post, " +
                      "the maximum number is '%s'", max));
        }
        if (vertices.size() == 0) {
            throw new IllegalArgumentException(
                      "The number of vertices can't be 0");
        }
    }

    private static Id getVertexId(HugeGraph g, JsonVertex vertex) {
        VertexLabel vertexLabel = g.vertexLabel(vertex.label);
        String labelId = vertexLabel.id().asString();
        IdStrategy idStrategy = vertexLabel.idStrategy();
        E.checkArgument(idStrategy != IdStrategy.AUTOMATIC,
                        "Automatic Id strategy is not supported now");

        if (idStrategy == IdStrategy.PRIMARY_KEY) {
            List<Id> pkIds = vertexLabel.primaryKeys();
            List<Object> pkValues = new ArrayList<>(pkIds.size());
            for (Id pkId : pkIds) {
                String propertyKey = g.propertyKey(pkId).name();
                Object propertyValue = vertex.properties.get(propertyKey);
                E.checkArgument(propertyValue != null,
                                "The value of primary key '%s' can't be null",
                                propertyKey);
                pkValues.add(propertyValue);
            }

            String value = ConditionQuery.concatValues(pkValues);
            return SplicingIdGenerator.splicing(labelId, value);
        } else if (idStrategy == IdStrategy.CUSTOMIZE_UUID) {
            return Text.uuid(String.valueOf(vertex.id));
        } else {
            assert idStrategy == IdStrategy.CUSTOMIZE_NUMBER ||
                   idStrategy == IdStrategy.CUSTOMIZE_STRING;
            return HugeVertex.getIdValue(vertex.id);
        }
    }

    private static class BatchVertexRequest {

        @JsonProperty("vertices")
        public List<JsonVertex> jsonVertices;
        @JsonProperty("update_strategies")
        public Map<String, UpdateStrategy> updateStrategies;
        @JsonProperty("create_if_not_exist")
        public boolean createIfNotExist = true;

        private static void checkUpdate(BatchVertexRequest req) {
            E.checkArgumentNotNull(req, "BatchVertexRequest can't be null");
            E.checkArgumentNotNull(req.jsonVertices,
                                   "Parameter 'vertices' can't be null");
            E.checkArgument(req.updateStrategies != null &&
                            !req.updateStrategies.isEmpty(),
                            "Parameter 'update_strategies' can't be empty");
            E.checkArgument(req.createIfNotExist == true,
                            "Parameter 'create_if_not_exist' " +
                            "dose not support false now");
        }

        @Override
        public String toString() {
            return String.format("BatchVertexRequest{jsonVertices=%s," +
                                 "updateStrategies=%s,createIfNotExist=%s}",
                                 this.jsonVertices, this.updateStrategies,
                                 this.createIfNotExist);
        }
    }

    private static class JsonVertex extends JsonElement {

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

        @Override
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
