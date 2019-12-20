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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

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
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.function.TriFunction;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.filter.CompressInterceptor.Compress;
import com.baidu.hugegraph.api.filter.DecompressInterceptor.Decompress;
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.define.UpdateStrategy;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.traversal.optimize.QueryHolder;
import com.baidu.hugegraph.traversal.optimize.TraversalUtil;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;

@Path("graphs/{graph}/graph/edges")
@Singleton
public class EdgeAPI extends BatchAPI {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed(name = "single-create")
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String create(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         JsonEdge jsonEdge) {
        LOG.debug("Graph [{}] create edge: {}", graph, jsonEdge);
        checkCreatingBody(jsonEdge);

        HugeGraph g = graph(manager, graph);

        if (jsonEdge.sourceLabel != null && jsonEdge.targetLabel != null) {
            // NOTE: Not use SchemaManager because it will throw 404
            SchemaTransaction schema = g.schemaTransaction();
            /*
             * NOTE: If the vertex id is correct but label not match with id,
             * we allow to create it here
             */
            E.checkArgumentNotNull(schema.getVertexLabel(jsonEdge.sourceLabel),
                                   "Invalid source vertex label '%s'",
                                   jsonEdge.sourceLabel);
            E.checkArgumentNotNull(schema.getVertexLabel(jsonEdge.targetLabel),
                                   "Invalid target vertex label '%s'",
                                   jsonEdge.targetLabel);
        }

        Vertex srcVertex = getVertex(g, jsonEdge.source, null);
        Vertex tgtVertex = getVertex(g, jsonEdge.target, null);

        Edge edge = commit(g, () -> {
            return srcVertex.addEdge(jsonEdge.label, tgtVertex,
                                     jsonEdge.properties());
        });

        return manager.serializer(g).writeEdge(edge);
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
                               @QueryParam("check_vertex")
                               @DefaultValue("true") boolean checkVertex,
                               List<JsonEdge> jsonEdges) {
        LOG.debug("Graph [{}] create edges: {}", graph, jsonEdges);
        checkCreatingBody(jsonEdges);
        checkBatchSize(config, jsonEdges);

        HugeGraph g = graph(manager, graph);

        TriFunction<HugeGraph, Object, String, Vertex> getVertex =
                    checkVertex ? EdgeAPI::getVertex : EdgeAPI::newVertex;

        return this.commit(config, g, jsonEdges.size(), () -> {
            List<String> ids = new ArrayList<>(jsonEdges.size());
            for (JsonEdge jsonEdge : jsonEdges) {
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
                ids.add(edge.id().toString());
            }
            return ids;
        });
    }

    /**
     * Batch update steps are same like vertices
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
                         BatchEdgeRequest req) {
        BatchEdgeRequest.checkUpdate(req);
        LOG.debug("Graph [{}] update edges: {}", graph, req);
        checkUpdatingBody(req.jsonEdges);
        checkBatchSize(config, req.jsonEdges);

        HugeGraph g = graph(manager, graph);
        Map<Id, JsonEdge> map = new HashMap<>(req.jsonEdges.size());
        TriFunction<HugeGraph, Object, String, Vertex> getVertex =
                    req.checkVertex ? EdgeAPI::getVertex : EdgeAPI::newVertex;

        return this.commit(config, g, map.size(), () -> {
            // 1.Put all newEdges' properties into map (combine first)
            req.jsonEdges.forEach(newEdge -> {
                Id newEdgeId = getEdgeId(g, newEdge);
                JsonEdge oldEdge = map.get(newEdgeId);
                this.updateExistElement(oldEdge, newEdge,
                                        req.updateStrategies);
                map.put(newEdgeId, newEdge);
            });

            // 2.Get all oldEdges and update with new ones
            Object[] ids = map.keySet().toArray();
            Iterator<Edge> oldEdges = g.edges(ids);
            oldEdges.forEachRemaining(oldEdge -> {
                JsonEdge newEdge = map.get(oldEdge.id());
                this.updateExistElement(g, oldEdge, newEdge,
                                        req.updateStrategies);
            });

            // 3.Add all finalEdges
            List<Edge> edges = new ArrayList<>(map.size());
            map.values().forEach(finalEdge -> {
                Vertex srcVertex = getVertex.apply(g, finalEdge.source,
                                                   finalEdge.sourceLabel);
                Vertex tgtVertex = getVertex.apply(g, finalEdge.target,
                                                   finalEdge.targetLabel);
                edges.add(srcVertex.addEdge(finalEdge.label, tgtVertex,
                                            finalEdge.properties()));
            });

            // If return ids, the ids.size() maybe different with the origins'
            return manager.serializer(g).writeEdges(edges.iterator(), false);
        });
    }

    @PUT
    @Timed(name = "single-update")
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String update(@Context GraphManager manager,
                         @PathParam("graph") String graph,
                         @PathParam("id") String id,
                         @QueryParam("action") String action,
                         JsonEdge jsonEdge) {
        LOG.debug("Graph [{}] update edge: {}", graph, jsonEdge);
        checkUpdatingBody(jsonEdge);

        if (jsonEdge.id != null) {
            E.checkArgument(id.equals(jsonEdge.id),
                            "The ids are different between url('%s') and " +
                            "request body('%s')", id, jsonEdge.id);
        }

        // Parse action param
        boolean append = checkAndParseAction(action);

        HugeGraph g = graph(manager, graph);
        HugeEdge edge = (HugeEdge) g.edges(id).next();
        EdgeLabel edgeLabel = edge.schemaLabel();

        for (String key : jsonEdge.properties.keySet()) {
            PropertyKey pkey = g.propertyKey(key);
            E.checkArgument(edgeLabel.properties().contains(pkey.id()),
                            "Can't update property for edge '%s' because " +
                            "there is no property key '%s' in its edge label",
                            id, key);
        }

        commit(g, () -> updateProperties(edge, jsonEdge, append));

        return manager.serializer(g).writeEdge(edge);
    }

    @GET
    @Timed
    @Compress
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String list(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @QueryParam("vertex_id") String vertexId,
                       @QueryParam("direction") String direction,
                       @QueryParam("label") String label,
                       @QueryParam("properties") String properties,
                       @QueryParam("keep_start_p")
                       @DefaultValue("false") boolean keepStartP,
                       @QueryParam("offset") @DefaultValue("0") long offset,
                       @QueryParam("page") String page,
                       @QueryParam("limit") @DefaultValue("100") long limit) {
        LOG.debug("Graph [{}] query edges by vertex: {}, direction: {}, " +
                  "label: {}, properties: {}, offset: {}, page: {}, limit: {}",
                  vertexId, direction, label, properties, offset, page, limit);

        Map<String, Object> props = parseProperties(properties);
        if (page != null) {
            E.checkArgument(offset == 0,
                            "Not support querying edges based on paging " +
                            "and offset together");
        }

        Id vertex = VertexAPI.checkAndParseVertexId(vertexId);
        Direction dir = parseDirection(direction);

        HugeGraph g = graph(manager, graph);

        GraphTraversal<?, Edge> traversal;
        if (vertex != null) {
            if (label != null) {
                traversal = g.traversal().V(vertex).toE(dir, label);
            } else {
                traversal = g.traversal().V(vertex).toE(dir);
            }
        } else {
            if (label != null) {
                traversal = g.traversal().E().hasLabel(label);
            } else {
                traversal = g.traversal().E();
            }
        }

        // Convert relational operator like P.gt()/P.lt()
        for (Map.Entry<String, Object> prop : props.entrySet()) {
            Object value = prop.getValue();
            if (!keepStartP && value instanceof String &&
                ((String) value).startsWith(TraversalUtil.P_CALL)) {
                prop.setValue(TraversalUtil.parsePredicate((String) value));
            }
        }

        for (Map.Entry<String, Object> entry : props.entrySet()) {
            traversal = traversal.has(entry.getKey(), entry.getValue());
        }

        if (page == null) {
            traversal = traversal.range(offset, offset + limit);
        } else {
            traversal = traversal.has(QueryHolder.SYSPROP_PAGE, page)
                                 .limit(limit);
        }

        return manager.serializer(g).writeEdges(traversal, page != null);
    }

    @GET
    @Timed
    @Path("{id}")
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @PathParam("id") String id) {
        LOG.debug("Graph [{}] get edge by id '{}'", graph, id);

        HugeGraph g = graph(manager, graph);
        Iterator<Edge> edges = g.edges(id);
        checkExist(edges, HugeType.EDGE, id);
        return manager.serializer(g).writeEdge(edges.next());
    }

    @DELETE
    @Timed
    @Path("{id}")
    @Consumes(APPLICATION_JSON)
    public void delete(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       @PathParam("id") String id) {
        LOG.debug("Graph [{}] remove vertex by id '{}'", graph, id);

        HugeGraph g = graph(manager, graph);
        // TODO: add removeEdge(id) to improve
        commit(g, () -> {
            Edge edge;
            try {
                edge = g.edges(id).next();
            } catch (NotFoundException e) {
                throw new IllegalArgumentException(e.getMessage());
            } catch (NoSuchElementException e) {
                throw new IllegalArgumentException(String.format(
                          "No such edge with id: '%s'", id));
            }
            edge.remove();
        });
    }

    private static void checkBatchSize(HugeConfig config,
                                       List<JsonEdge> edges) {
        int max = config.get(ServerOptions.MAX_EDGES_PER_BATCH);
        if (edges.size() > max) {
            throw new IllegalArgumentException(String.format(
                      "Too many edges for one time post, " +
                      "the maximum number is '%s'", max));
        }
        if (edges.size() == 0) {
            throw new IllegalArgumentException(
                      "The number of edges can't be 0");
        }
    }

    private static Vertex getVertex(HugeGraph graph, Object id, String label) {
        HugeVertex vertex;
        try {
            vertex = (HugeVertex) graph.vertices(id).next();
        } catch (NoSuchElementException e) {
            throw new IllegalArgumentException(String.format(
                      "Invalid vertex id '%s'", id));
        }
        // Clone a new vertex to support multi-thread access
        return vertex.copy().resetTx();
    }

    private static Vertex newVertex(HugeGraph graph, Object id, String label) {
        // NOTE: Not use SchemaManager because it will throw 404
        VertexLabel vl = graph.schemaTransaction().getVertexLabel(label);
        E.checkArgumentNotNull(vl, "Invalid vertex label '%s'", label);
        Id idValue = HugeVertex.getIdValue(id);
        return new HugeVertex(graph, idValue, vl);
    }

    public static Direction parseDirection(String direction) {
        if (direction == null || direction.isEmpty()) {
            return Direction.BOTH;
        }
        try {
            return Direction.valueOf(direction);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format(
                      "Direction value must be in [OUT, IN, BOTH], " +
                      "but got '%s'", direction));
        }
    }

    private Id getEdgeId(HugeGraph g, JsonEdge newEdge) {
        String sortKeys = "";
        Id labelId = g.edgeLabel(newEdge.label).id();
        List<Id> sortKeyIds = g.edgeLabel(labelId).sortKeys();
        if (!sortKeyIds.isEmpty()) {
            List<Object> sortKeyValues = new ArrayList<>(sortKeyIds.size());
            sortKeyIds.forEach(skId -> {
                PropertyKey pk = g.propertyKey(skId);
                String sortKey = pk.name();
                Object sortKeyValue = newEdge.properties.get(sortKey);
                E.checkArgument(sortKeyValue != null,
                                "The value of sort key '%s' can't be null",
                                sortKey);
                sortKeyValue = pk.convValue(sortKeyValue, true);
                sortKeyValues.add(sortKeyValue);
            });
            sortKeys = ConditionQuery.concatValues(sortKeyValues);
        }
        EdgeId edgeId = new EdgeId(HugeVertex.getIdValue(newEdge.source),
                                   Directions.OUT, labelId, sortKeys,
                                   HugeVertex.getIdValue(newEdge.target));
        if (newEdge.id != null) {
            E.checkArgument(edgeId.equals(newEdge.id),
                            "The sort key values either be null " +
                            "or equal to origin when specified edge id");
        }
        return edgeId;
    }

    protected static class BatchEdgeRequest {

        @JsonProperty("edges")
        public List<JsonEdge> jsonEdges;
        @JsonProperty("update_strategies")
        public Map<String, UpdateStrategy> updateStrategies;
        @JsonProperty("check_vertex")
        public boolean checkVertex = false;
        @JsonProperty("create_if_not_exist")
        public boolean createIfNotExist = true;

        private static void checkUpdate(BatchEdgeRequest req) {
            E.checkArgumentNotNull(req, "BatchEdgeRequest can't be null");
            E.checkArgumentNotNull(req.jsonEdges,
                                   "Parameter 'edges' can't be null");
            E.checkArgument(req.updateStrategies != null &&
                            !req.updateStrategies.isEmpty(),
                            "Parameter 'update_strategies' can't be empty");
            E.checkArgument(req.createIfNotExist == true,
                            "Parameter 'create_if_not_exist' " +
                            "dose not support false now");
        }

        @Override
        public String toString() {
            return String.format("BatchEdgeRequest{jsonEdges=%s," +
                                 "updateStrategies=%s," +
                                 "checkVertex=%s,createIfNotExist=%s}",
                                 this.jsonEdges, this.updateStrategies,
                                 this.checkVertex, this.createIfNotExist);
        }
    }

    private static class JsonEdge extends JsonElement {

        @JsonProperty("outV")
        public Object source;
        @JsonProperty("outVLabel")
        public String sourceLabel;
        @JsonProperty("inV")
        public Object target;
        @JsonProperty("inVLabel")
        public String targetLabel;

        @Override
        public void checkCreate(boolean isBatch) {
            E.checkArgumentNotNull(this.label, "Expect the label of edge");
            E.checkArgumentNotNull(this.source, "Expect source vertex id");
            E.checkArgumentNotNull(this.target, "Expect target vertex id");
            if (isBatch) {
                E.checkArgumentNotNull(this.sourceLabel,
                                       "Expect source vertex label");
                E.checkArgumentNotNull(this.targetLabel,
                                       "Expect target vertex label");
            } else {
                E.checkArgument(this.sourceLabel == null &&
                                this.targetLabel == null ||
                                this.sourceLabel != null &&
                                this.targetLabel != null,
                                "The both source and target vertex label " +
                                "are either passed in, or not passed in");
            }
            this.checkUpdate();
        }

        @Override
        public void checkUpdate() {
            E.checkArgumentNotNull(this.properties,
                                   "The properties of edge can't be null");

            for (Map.Entry<String, Object> entry : this.properties.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                E.checkArgumentNotNull(value, "Not allowed to set value of " +
                                       "property '%s' to null for edge '%s'",
                                       key, this.id);
            }
        }

        @Override
        public Object[] properties() {
            return API.properties(this.properties);
        }

        @Override
        public String toString() {
            return String.format("JsonEdge{label=%s, " +
                                 "source-vertex=%s, source-vertex-label=%s, " +
                                 "target-vertex=%s, target-vertex-label=%s, " +
                                 "properties=%s}",
                                 this.label, this.source, this.sourceLabel,
                                 this.target, this.targetLabel,
                                 this.properties);
        }
    }
}
