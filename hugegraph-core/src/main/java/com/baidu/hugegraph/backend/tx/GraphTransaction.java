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

package com.baidu.hugegraph.backend.tx;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.ConditionQueryFlatten;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.tx.GraphIndexTransaction.OptimizedType;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.exception.LimitExceedException;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.iterator.ExtendableIterator;
import com.baidu.hugegraph.iterator.FilterIterator;
import com.baidu.hugegraph.iterator.FlatMapperFilterIterator;
import com.baidu.hugegraph.iterator.MapperIterator;
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeEdgeProperty;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeFeatures.HugeVertexFeatures;
import com.baidu.hugegraph.structure.HugeProperty;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.structure.HugeVertexProperty;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.Indexfiable;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.type.define.IdStrategy;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.baidu.hugegraph.util.LockUtil;
import com.google.common.collect.ImmutableList;

public class GraphTransaction extends IndexableTransaction {

    private final GraphIndexTransaction indexTx;

    private Map<Id, HugeVertex> addedVertexes;
    private Map<Id, HugeVertex> removedVertexes;

    private Map<Id, HugeEdge> addedEdges;
    private Map<Id, HugeEdge> removedEdges;

    /*
     * These are used to rollback state
     * NOTE: props updates will be put into mutation directly(due to difficult)
     */
    private Map<Id, HugeVertex> updatedVertexes;
    private Map<Id, HugeEdge> updatedEdges;
    private Set<HugeProperty<?>> updatedProps; // Oldest props

    private final int vertexesCapacity;
    private final int edgesCapacity;

    public GraphTransaction(HugeGraph graph, BackendStore store) {
        super(graph, store);

        this.indexTx = new GraphIndexTransaction(graph, store);
        assert !this.indexTx.autoCommit();

        final HugeConfig conf = graph.configuration();
        this.vertexesCapacity = conf.get(CoreOptions.VERTEX_TX_CAPACITY);
        this.edgesCapacity = conf.get(CoreOptions.EDGE_TX_CAPACITY);
    }

    @Override
    public boolean hasUpdates() {
        boolean empty = (this.addedVertexes.isEmpty() &&
                         this.removedVertexes.isEmpty() &&
                         this.updatedVertexes.isEmpty() &&
                         this.addedEdges.isEmpty() &&
                         this.removedEdges.isEmpty() &&
                         this.updatedEdges.isEmpty());
        return !empty || super.hasUpdates();
    }

    @Override
    public int mutationSize() {
        int size = (this.addedVertexes.size() +
                    this.removedVertexes.size() +
                    this.updatedVertexes.size() +
                    this.addedEdges.size() +
                    this.removedEdges.size() +
                    this.updatedEdges.size());
        return size;
    }

    @Override
    protected void reset() {
        super.reset();

        // Clear mutation
        this.addedVertexes = InsertionOrderUtil.newMap();
        this.removedVertexes = InsertionOrderUtil.newMap();
        this.updatedVertexes = InsertionOrderUtil.newMap();

        this.addedEdges = InsertionOrderUtil.newMap();
        this.removedEdges = InsertionOrderUtil.newMap();
        this.updatedEdges = InsertionOrderUtil.newMap();

        this.updatedProps = InsertionOrderUtil.newSet();
    }

    @Override
    protected AbstractTransaction indexTransaction() {
        return this.indexTx;
    }

    @Override
    protected void beforeWrite() {
        this.checkTxVerticesCapacity();
        this.checkTxEdgesCapacity();

        super.beforeWrite();
    }

    protected Set<Id> verticesInTx() {
        Set<Id> ids = new HashSet<>(this.addedVertexes.keySet());
        ids.addAll(this.updatedVertexes.keySet());
        ids.addAll(this.removedVertexes.keySet());
        return ids;
    }

    protected Set<Id> edgesInTx() {
        Set<Id> ids = new HashSet<>(this.addedEdges.keySet());
        ids.addAll(this.removedEdges.keySet());
        ids.addAll(this.updatedEdges.keySet());
        return ids;
    }

    protected boolean removingEdgeOwner(HugeEdge edge) {
        for (HugeVertex vertex : this.removedVertexes.values()) {
            if (edge.belongToVertex(vertex)) {
                return true;
            }
        }
        return false;
    }

    @Watched(prefix = "tx")
    @Override
    protected BackendMutation prepareCommit() {
        // Serialize and add updates into super.deletions
        if (this.removedVertexes.size() > 0 || this.removedEdges.size() > 0) {
            this.prepareDeletions(this.removedVertexes, this.removedEdges);
        }
        // Serialize and add updates into super.additions
        if (this.addedVertexes.size() > 0 || this.addedEdges.size() > 0) {
            this.prepareAdditions(this.addedVertexes, this.addedEdges);
        }
        return this.mutation();
    }

    protected void prepareAdditions(Map<Id, HugeVertex> addedVertexes,
                                    Map<Id, HugeEdge> addedEdges) {
        // Do vertex update
        for (HugeVertex v : addedVertexes.values()) {
            assert !v.removed();
            v.committed();
            // Add vertex entry
            this.doInsert(this.serializer.writeVertex(v));
            // Update index of vertex(only include props)
            this.indexTx.updateVertexIndex(v, false);
            this.indexTx.updateLabelIndex(v, false);
        }

        // Do edge update
        for (HugeEdge e : addedEdges.values()) {
            assert !e.removed();
            e.committed();
            // Skip edge if its owner has been removed
            if (this.removingEdgeOwner(e)) {
                continue;
            }
            // Add edge entry of OUT and IN
            this.doInsert(this.serializer.writeEdge(e));
            this.doInsert(this.serializer.writeEdge(e.switchOwner()));
            // Update index of edge
            this.indexTx.updateEdgeIndex(e, false);
            this.indexTx.updateLabelIndex(e, false);
        }
    }

    protected void prepareDeletions(Map<Id, HugeVertex> removedVertexes,
                                    Map<Id, HugeEdge> removedEdges) {
        // Remove related edges of each vertex
        for (HugeVertex v : removedVertexes.values()) {
            // Query all edges of the vertex and remove them
            Query query = constructEdgesQuery(v.id(), Directions.BOTH);
            Iterator<HugeEdge> vedges = this.queryEdgesFromBackend(query);
            while (vedges.hasNext()) {
                this.checkTxEdgesCapacity();
                HugeEdge edge = vedges.next();
                removedEdges.put(edge.id(), edge);
            }
        }

        // Remove vertexes
        for (HugeVertex v : removedVertexes.values()) {
            /*
             * If the backend stores vertex together with edges, it's edges
             * would be removed after removing vertex. Otherwise, if the
             * backend stores vertex which is separated from edges, it's
             * edges should be removed manually when removing vertex.
             */
            this.doRemove(this.serializer.writeVertex(v.prepareRemoved()));
            this.indexTx.updateVertexIndex(v, true);
            this.indexTx.updateLabelIndex(v, true);
        }

        // Remove edges
        for (HugeEdge e : removedEdges.values()) {
            // Update edge index
            this.indexTx.updateEdgeIndex(e, true);
            this.indexTx.updateLabelIndex(e, true);
            // Remove edge of OUT and IN
            e = e.prepareRemoved();
            this.doRemove(this.serializer.writeEdge(e));
            this.doRemove(this.serializer.writeEdge(e.switchOwner()));
        }
    }

    @Override
    public void rollback() throws BackendException {
        // Rollback properties changes
        for (HugeProperty<?> prop : this.updatedProps) {
            prop.element().setProperty(prop);
        }
        super.rollback();
    }

    @Override
    public Iterator<BackendEntry> query(Query query) {
        List<Query> queries = new ArrayList<>();
        if (query instanceof ConditionQuery) {
            for (ConditionQuery cq: ConditionQueryFlatten.flatten(
                                    (ConditionQuery) query)) {
                Query q = this.optimizeQuery(cq);
                /*
                 * NOTE: There are two possibilities for this query:
                 * 1.sysprop-query, which would not be empty.
                 * 2.index-query result(ids after optimize), which may be empty.
                 */
                if (!q.empty()) {
                    // Return empty if there is no result after index-query
                    queries.add(q);
                }
            }
        } else {
            queries.add(query);
        }

        ExtendableIterator<BackendEntry> rs = new ExtendableIterator<>();
        for (Query q : queries) {
            rs.extend(super.query(q));
        }
        return rs;
    }

    @Watched(prefix = "graph")
    public HugeVertex addVertex(Object... keyValues) {
        return this.addVertex(this.constructVertex(true, keyValues));
    }

    @Watched("graph.addVertex-instance")
    public HugeVertex addVertex(HugeVertex vertex) {
        this.checkOwnerThread();

        // Override vertexes in local `removedVertexes`
        this.removedVertexes.remove(vertex.id());

        LockUtil.Locks locks = new LockUtil.Locks();
        try {
            locks.lockReads(LockUtil.VERTEX_LABEL_DELETE, vertex.schemaLabel().id());
            locks.lockReads(LockUtil.INDEX_LABEL_DELETE,
                            vertex.schemaLabel().indexLabels());
            /*
             * No need to lock VERTEX_LABEL_ADD_UPDATE, because vertex label
             * update only can add nullable properties and user data, which is
             * unconcerned with add vertex
             */
            this.beforeWrite();
            this.addedVertexes.put(vertex.id(), vertex);
            this.afterWrite();
        } finally {
            locks.unlock();
        }
        return vertex;
    }

    @Watched(prefix = "graph")
    public HugeVertex constructVertex(boolean verifyVL, Object... keyValues) {
        HugeElement.ElementKeys elemKeys = HugeElement.classifyKeys(keyValues);

        VertexLabel vertexLabel = this.checkVertexLabel(elemKeys.label(),
                                                        verifyVL);
        Id id = HugeVertex.getIdValue(elemKeys.id());
        List<Id> keys = this.graph().mapPkName2Id(elemKeys.keys());

        // Check whether id match with id strategy
        this.checkId(id, keys, vertexLabel);

        // Check whether passed all non-null property
        this.checkNonnullProperty(keys, vertexLabel);

        // Create HugeVertex
        HugeVertex vertex = new HugeVertex(this, null, vertexLabel);

        // Set properties
        ElementHelper.attachProperties(vertex, keyValues);

        // Assign vertex id
        if (this.graph().restoring() &&
            vertexLabel.idStrategy() == IdStrategy.AUTOMATIC) {
            // Resume id for AUTOMATIC id strategy in restoring mode
            vertex.assignId(id, true);
        } else {
            vertex.assignId(id);
        }

        return vertex;
    }

    @Watched(prefix = "graph")
    public void removeVertex(HugeVertex vertex) {
        this.checkOwnerThread();

        this.beforeWrite();

        // Override vertexes in local `addedVertexes`
        this.addedVertexes.remove(vertex.id());

        // Collect the removed vertex
        this.removedVertexes.put(vertex.id(), vertex);

        this.afterWrite();
    }

    public Iterator<Vertex> queryAdjacentVertices(Iterator<Edge> edges) {
        if (!edges.hasNext()) {
            return ImmutableList.<Vertex>of().iterator();
        }

        IdQuery query = new IdQuery(HugeType.VERTEX);
        while (edges.hasNext()) {
            HugeEdge edge = (HugeEdge) edges.next();
            query.query(edge.otherVertex().id());
        }

        return this.queryVertices(query);
    }

    public Iterator<Vertex> queryVertices(Object... vertexIds) {
        // NOTE: it will allow duplicated vertices if query by duplicated ids
        List<Vertex> results = new ArrayList<>(vertexIds.length);

        for (Object vertexId : vertexIds) {
            Vertex vertex;
            Id id = HugeVertex.getIdValue(vertexId);
            if (id == null || this.removedVertexes.containsKey(id)) {
                // The record has been deleted
                continue;
            } else if ((vertex = this.addedVertexes.get(id)) != null ||
                       (vertex = this.updatedVertexes.get(id)) != null) {
                // Find in memory
                results.add(vertex);
            } else {
                // Query from backend store
                try {
                    BackendEntry entry = this.get(HugeType.VERTEX, id);
                    vertex = this.serializer.readVertex(this.graph(), entry);
                } catch (NotFoundException ignored) {
                    continue;
                }
                assert vertex != null;
                results.add(vertex);
            }
        }

        return results.iterator();
    }

    public Iterator<Vertex> queryVertices() {
        Query q = new Query(HugeType.VERTEX);
        return this.queryVertices(q);
    }

    public Iterator<Vertex> queryVertices(Query query) {
        Iterator<HugeVertex> results = this.queryVerticesFromBackend(query);
        @SuppressWarnings("unchecked")
        Iterator<Vertex> r = (Iterator<Vertex>) joinTxVertices(query, results);
        return r;
    }

    protected Iterator<HugeVertex> queryVerticesFromBackend(Query query) {
        assert query.resultType().isVertex();

        Iterator<BackendEntry> entries = this.query(query);

        Iterator<HugeVertex> results = new MapperIterator<>(entries, entry -> {
            HugeVertex vertex = this.serializer.readVertex(graph(), entry);
            assert vertex != null;
            return vertex;
        });

        return new FilterIterator<>(results, vertex -> {
            assert vertex.schemaLabel() != VertexLabel.NONE;
            // Filter hidden results
            if (!query.showHidden() && Graph.Hidden.isHidden(vertex.label())) {
                return false;
            }
            // Process results that query from left index or primary-key
            if (query.resultType().isVertex() &&
                !filterResultFromIndexQuery(query, vertex)) {
                return false;
            }
            return true;
        });
    }

    @Watched(prefix = "graph")
    public HugeEdge addEdge(HugeEdge edge) {
        this.checkOwnerThread();

        // Override edges in local `removedEdges`
        this.removedEdges.remove(edge.id());

        LockUtil.Locks locks = new LockUtil.Locks();
        try {
            locks.lockReads(LockUtil.EDGE_LABEL_DELETE, edge.schemaLabel().id());
            locks.lockReads(LockUtil.INDEX_LABEL_DELETE,
                            edge.schemaLabel().indexLabels());
            /*
             * No need to lock EDGE_LABEL_ADD_UPDATE, because edge label
             * update only can add nullable properties and user data, which is
             * unconcerned with add edge
             */
            this.beforeWrite();
            this.addedEdges.put(edge.id(), edge);
            this.afterWrite();
        } finally {
            locks.unlock();
        }
        return edge;
    }

    @Watched(prefix = "graph")
    public void removeEdge(HugeEdge edge) {
        this.checkOwnerThread();

        this.beforeWrite();

        // Override edges in local `addedEdges`
        this.addedEdges.remove(edge.id());

        // Collect the removed edge
        this.removedEdges.put(edge.id(), edge);

        this.afterWrite();
    }

    public Iterator<Edge> queryEdgesByVertex(Id id) {
        return queryEdges(constructEdgesQuery(id, Directions.BOTH));
    }

    public Iterator<Edge> queryEdges(Object... edgeIds) {
        // NOTE: it will allow duplicated edges if query by duplicated ids
        List<Edge> results = new ArrayList<>(edgeIds.length);

        for (Object edgeId : edgeIds) {
            Edge edge;
            Id id = HugeEdge.getIdValue(edgeId);
            if (id == null || this.removedEdges.containsKey(id)) {
                // The record has been deleted
                continue;
            } else if ((edge = this.addedEdges.get(id)) != null ||
                       (edge = this.updatedEdges.get(id)) != null) {
                // Find in memory
                results.add(edge);
            } else {
                // Query from backend store
                BackendEntry entry;
                try {
                    entry = this.get(HugeType.EDGE, id);
                } catch (NotFoundException ignored) {
                    continue;
                }
                HugeVertex vertex = this.serializer.readVertex(graph(), entry);
                assert vertex != null;
                assert vertex.getEdges().size() == 1;
                results.addAll(vertex.getEdges());
            }
        }

        return results.iterator();
    }

    public Iterator<Edge> queryEdges() {
        Query q = new Query(HugeType.EDGE);
        return this.queryEdges(q);
    }

    public Iterator<Edge> queryEdges(Query query) {
        Iterator<HugeEdge> results = this.queryEdgesFromBackend(query);
        @SuppressWarnings("unchecked")
        Iterator<Edge> r = (Iterator<Edge>) joinTxEdges(query, results,
                                                        this.removedVertexes);
        return r;
    }

    protected Iterator<HugeEdge> queryEdgesFromBackend(Query query) {
        assert query.resultType().isEdge();

        Iterator<BackendEntry> entries = this.query(query);

        Function<BackendEntry, Iterator<HugeEdge>> mapper = entry -> {
            // Edges are in a vertex
            HugeVertex vertex = this.serializer.readVertex(graph(), entry);
            assert vertex != null;
            // Copy to avoid ConcurrentModificationException when removing edge
            return ImmutableList.copyOf(vertex.getEdges()).iterator();
        };

        Set<Id> returnedEdges = new HashSet<>();
        Function<HugeEdge, Boolean> filter = edge -> {
            // Filter hidden results
            if (!query.showHidden() && Graph.Hidden.isHidden(edge.label())) {
                return false;
            }
            // Process results that query from left index
            if (!this.filterResultFromIndexQuery(query, edge)) {
                return false;
            }

            // Filter repeat edges (TODO: split edges table into OUT&IN table)
            if (!returnedEdges.contains(edge.id())) {
                /*
                 * NOTE: Maybe some edges are IN and others are OUT if
                 * querying edges both directions, perhaps it would look
                 * better if we convert all edges in results to OUT, but
                 * that would break the logic when querying IN edges.
                 */
                returnedEdges.add(edge.id());
                return true;
            } else {
                LOG.debug("Results contains edge: {}", edge);
                return false;
            }
        };

        return new FlatMapperFilterIterator<>(entries, mapper, filter);
    }

    @Watched(prefix = "graph")
    public <V> void addVertexProperty(HugeVertexProperty<V> prop) {
        // NOTE: this method can also be used to update property

        HugeVertex vertex = prop.element();
        E.checkState(vertex != null,
                     "No owner for updating property '%s'", prop.key());

        // Add property in memory for new created vertex
        if (vertex.fresh()) {
            // The owner will do property update
            vertex.setProperty(prop);
            return;
        }
        // Check is updating property of added/removed vertex
        E.checkArgument(!this.addedVertexes.containsKey(vertex.id()) ||
                        this.updatedVertexes.containsKey(vertex.id()),
                        "Can't update property '%s' for adding-state vertex",
                        prop.key());
        E.checkArgument(!vertex.removed() &&
                        !this.removedVertexes.containsKey(vertex.id()),
                        "Can't update property '%s' for removing-state vertex",
                        prop.key());
        // Check is updating primary key
        List<Id> primaryKeyIds = vertex.schemaLabel().primaryKeys();
        E.checkArgument(!primaryKeyIds.contains(prop.propertyKey().id()),
                        "Can't update primary key: '%s'", prop.key());

        // Do property update
        Set<Id> lockNames = relatedIndexLabels(prop, vertex.schemaLabel());
        this.lockForUpdateProperty(lockNames, (locks) -> {
            // Update old vertex to remove index (without new property)
            this.indexTx.updateVertexIndex(vertex, true);

            // Update index of current vertex (with new property)
            this.propertyUpdated(vertex, vertex.setProperty(prop));
            this.indexTx.updateVertexIndex(vertex, false);

            if (this.store().features().supportsUpdateVertexProperty()) {
                // Append new property(OUT and IN owner edge)
                this.doAppend(this.serializer.writeVertexProperty(prop));
            } else {
                // Override vertex
                this.addVertex(vertex);
            }
        });
    }

    @Watched(prefix = "graph")
    public <V> void removeVertexProperty(HugeVertexProperty<V> prop) {
        HugeVertex vertex = prop.element();
        PropertyKey propKey = prop.propertyKey();
        E.checkState(vertex != null,
                     "No owner for removing property '%s'", prop.key());

        // Maybe have ever been removed (compatible with tinkerpop)
        if (!vertex.hasProperty(propKey.id())) {
            // PropertyTest shouldAllowRemovalFromVertexWhenAlreadyRemoved()
            return;
        }
        // Check is removing primary key
        List<Id> primaryKeyIds = vertex.schemaLabel().primaryKeys();
        E.checkArgument(!primaryKeyIds.contains(propKey.id()),
                        "Can't remove primary key '%s'", prop.key());
        // Remove property in memory for new created vertex
        if (vertex.fresh()) {
            // The owner will do property update
            vertex.removeProperty(propKey.id());
            return;
        }
        // Check is updating property of added/removed vertex
        E.checkArgument(!this.addedVertexes.containsKey(vertex.id()) ||
                        this.updatedVertexes.containsKey(vertex.id()),
                        "Can't remove property '%s' for adding-state vertex",
                        prop.key());
        E.checkArgument(!this.removedVertexes.containsKey(vertex.id()),
                        "Can't remove property '%s' for removing-state vertex",
                        prop.key());

        // Do property update
        Set<Id> lockIds = relatedIndexLabels(prop, vertex.schemaLabel());
        this.lockForUpdateProperty(lockIds, (locks) -> {
            // Update old vertex to remove index (with the property)
            this.indexTx.updateVertexIndex(vertex, true);

            // Update index of current vertex (without the property)
            this.propertyUpdated(vertex, vertex.removeProperty(propKey.id()));
            this.indexTx.updateVertexIndex(vertex, false);

            if (this.store().features().supportsUpdateVertexProperty()) {
                // Eliminate the property(OUT and IN owner edge)
                this.doEliminate(this.serializer.writeVertexProperty(prop));
            } else {
                // Override vertex
                this.addVertex(vertex);
            }
        });
    }

    @Watched(prefix = "graph")
    public <V> void addEdgeProperty(HugeEdgeProperty<V> prop) {
        // NOTE: this method can also be used to update property

        HugeEdge edge = prop.element();
        E.checkState(edge != null,
                     "No owner for updating property '%s'", prop.key());

        // Add property in memory for new created edge
        if (edge.fresh()) {
            // The owner will do property update
            edge.setProperty(prop);
            return;
        }
        // Check is updating property of added/removed edge
        E.checkArgument(!this.addedEdges.containsKey(edge.id()) ||
                        this.updatedEdges.containsKey(edge.id()),
                        "Can't update property '%s' for adding-state edge",
                        prop.key());
        E.checkArgument(!edge.removed() &&
                        !this.removedEdges.containsKey(edge.id()),
                        "Can't update property '%s' for removing-state edge",
                        prop.key());
        // Check is updating sort key
        List<Id> sortKeys = edge.schemaLabel().sortKeys();
        E.checkArgument(!sortKeys.contains(prop.propertyKey().id()),
                        "Can't update sort key '%s'", prop.key());

        // Do property update
        Set<Id> lockIds = relatedIndexLabels(prop, edge.schemaLabel());
        this.lockForUpdateProperty(lockIds, (locks) -> {
            // Update old edge to remove index (without new property)
            this.indexTx.updateEdgeIndex(edge, true);

            // Update index of current edge (with new property)
            this.propertyUpdated(edge, edge.setProperty(prop));
            this.indexTx.updateEdgeIndex(edge, false);

            if (this.store().features().supportsUpdateEdgeProperty()) {
                // Append new property(OUT and IN owner edge)
                this.doAppend(this.serializer.writeEdgeProperty(prop));
                this.doAppend(this.serializer.writeEdgeProperty(
                              prop.switchEdgeOwner()));
            } else {
                // Override edge(the edge will be in addedEdges & updatedEdges)
                this.addEdge(edge);
            }
        });
    }

    @Watched(prefix = "graph")
    public <V> void removeEdgeProperty(HugeEdgeProperty<V> prop) {
        HugeEdge edge = prop.element();
        PropertyKey propKey = prop.propertyKey();
        E.checkState(edge != null,
                     "No owner for removing property '%s'", prop.key());

        // Maybe have ever been removed
        if (!edge.hasProperty(propKey.id())) {
            return;
        }
        // Check is removing sort key
        List<Id> sortKeyIds = edge.schemaLabel().sortKeys();
        E.checkArgument(!sortKeyIds.contains(prop.propertyKey().id()),
                        "Can't remove sort key '%s'", prop.key());
        // Remove property in memory for new created edge
        if (edge.fresh()) {
            // The owner will do property update
            edge.removeProperty(propKey.id());
            return;
        }
        // Check is updating property of added/removed edge
        E.checkArgument(!this.addedEdges.containsKey(edge.id()) ||
                        this.updatedEdges.containsKey(edge.id()),
                        "Can't remove property '%s' for adding-state edge",
                        prop.key());
        E.checkArgument(!this.removedEdges.containsKey(edge.id()),
                        "Can't remove property '%s' for removing-state edge",
                        prop.key());

        // Do property update
        Set<Id> lockIds = relatedIndexLabels(prop, edge.schemaLabel());
        this.lockForUpdateProperty(lockIds, (locks) -> {
            // Update old edge to remove index (with the property)
            this.indexTx.updateEdgeIndex(edge, true);

            // Update index of current edge (without the property)
            this.propertyUpdated(edge, edge.removeProperty(propKey.id()));
            this.indexTx.updateEdgeIndex(edge, false);

            if (this.store().features().supportsUpdateEdgeProperty()) {
                // Eliminate the property(OUT and IN owner edge)
                this.doEliminate(this.serializer.writeEdgeProperty(prop));
                this.doEliminate(this.serializer.writeEdgeProperty(
                                 prop.switchEdgeOwner()));
            } else {
                // Override edge(the edge will be in addedEdges & updatedEdges)
                this.addEdge(edge);
            }
        });
    }

    /**
     * Construct one edge condition query based on source vertex, direction and
     * edge labels
     * @param sourceVertex source vertex of edge
     * @param direction only be "IN", "OUT" or "BOTH"
     * @param edgeLabels edge labels of queried edges
     * @return constructed condition query
     */
    public static ConditionQuery constructEdgesQuery(Id sourceVertex,
                                                     Directions direction,
                                                     Id... edgeLabels) {
        E.checkState(sourceVertex != null,
                     "The edge query must contain source vertex");
        E.checkState(direction != null,
                     "The edge query must contain direction");

        ConditionQuery query = new ConditionQuery(HugeType.EDGE);

        // Edge source vertex
        query.eq(HugeKeys.OWNER_VERTEX, sourceVertex);

        // Edge direction
        if (direction == Directions.BOTH) {
            query.query(Condition.or(
                        Condition.eq(HugeKeys.DIRECTION, Directions.OUT),
                        Condition.eq(HugeKeys.DIRECTION, Directions.IN)));
        } else {
            assert direction == Directions.OUT || direction == Directions.IN;
            query.eq(HugeKeys.DIRECTION, direction);
        }

        // Edge labels
        if (edgeLabels.length == 1) {
            query.eq(HugeKeys.LABEL, edgeLabels[0]);
        } else if (edgeLabels.length > 1) {
            query.query(Condition.in(HugeKeys.LABEL,
                                     Arrays.asList(edgeLabels)));
        } else {
            assert edgeLabels.length == 0;
        }

        return query;
    }

    public static boolean matchEdgeSortKeys(ConditionQuery query,
                                            HugeGraph graph) {
        assert query.resultType().isEdge();

        Id label = (Id) query.condition(HugeKeys.LABEL);
        if (label == null) {
            return false;
        }
        List<Id> keys = graph.edgeLabel(label).sortKeys();
        return !keys.isEmpty() && query.matchUserpropKeys(keys);
    }

    public static void verifyEdgesConditionQuery(ConditionQuery query) {
        assert query.resultType().isEdge();

        int total = query.conditions().size();
        if (total == 1) {
            // Supported: 1.query just by edge label, 2.query with scan
            if (query.containsCondition(HugeKeys.LABEL) ||
                query.containsScanCondition()) {
                return;
            }
        }

        int matched = 0;
        for (HugeKeys key : EdgeId.KEYS) {
            Object value = query.condition(key);
            if (value == null) {
                break;
            }
            matched++;
        }
        if (matched != total) {
            throw new BackendException(
                      "Not supported querying edges by %s, expect %s",
                      query.conditions(), EdgeId.KEYS[matched]);
        }
    }

    protected Query optimizeQuery(ConditionQuery query) {
        Id label = (Id) query.condition(HugeKeys.LABEL);

        // Optimize vertex query
        if (label != null && query.resultType().isVertex()) {
            VertexLabel vertexLabel = this.graph().vertexLabel(label);
            if (vertexLabel.idStrategy() == IdStrategy.PRIMARY_KEY) {
                List<Id> keys = vertexLabel.primaryKeys();
                E.checkState(!keys.isEmpty(),
                             "The primary keys can't be empty when using " +
                             "'%s' id strategy for vertex label '%s'",
                             IdStrategy.PRIMARY_KEY, vertexLabel.name());
                if (query.matchUserpropKeys(keys)) {
                    // Query vertex by label + primary-values
                    query.optimized(OptimizedType.PRIMARY_KEY.ordinal());
                    String primaryValues = query.userpropValuesString(keys);
                    LOG.debug("Query vertices by primaryKeys: {}", query);
                    // Convert {vertex-label + primary-key} to vertex-id
                    Id id = SplicingIdGenerator.splicing(
                                                vertexLabel.id().asString(),
                                                primaryValues);
                    /*
                     * Just query by primary-key(id), ignore other userprop(if
                     * exists) that it will be filtered by queryVertices(Query)
                     */
                    return new IdQuery(query, id);
                }
            }
        }

        // Optimize edge query
        if (label != null && query.resultType().isEdge()) {
            List<Id> keys = this.graph().edgeLabel(label).sortKeys();
            if (query.condition(HugeKeys.OWNER_VERTEX) != null &&
                query.condition(HugeKeys.DIRECTION) != null &&
                !keys.isEmpty() && query.matchUserpropKeys(keys)) {
                // Query edge by sourceVertex + direction + label + sort-values
                query.optimized(OptimizedType.SORT_KEY.ordinal());
                query = query.copy();
                query.eq(HugeKeys.SORT_VALUES,
                         query.userpropValuesString(keys));
                query.resetUserpropConditions();
                LOG.debug("Query edges by sortKeys: {}", query);
                return query;
            }
        }

        /*
         * Query only by sysprops, like: by vertex label, by edge label.
         * NOTE: we assume sysprops would be indexed by backend store
         * but we don't support query edges only by direction/target-vertex.
         */
        if (query.allSysprop()) {
            if (query.resultType().isEdge()) {
                verifyEdgesConditionQuery(query);
            }
            // Query by label & store supports feature or not query by label
            boolean byLabel = (label != null && query.conditions().size() == 1);
            if (this.store().features().supportsQueryByLabel() || !byLabel) {
                return query;
            }
        }

        /*
         * Optimize by index-query
         * It will return a list of id (maybe empty) if success,
         * or throw exception if there is no any index for query properties.
         */
        this.beforeRead();
        try {
            return this.indexTx.query(query);
        } finally {
            this.afterRead();
        }
    }

    private VertexLabel checkVertexLabel(Object label, boolean verifyLabel) {
        HugeVertexFeatures features = graph().features().vertex();

        // Check Vertex label
        if (label == null && features.supportsDefaultLabel()) {
            label = features.defaultLabel();
        }

        if (label == null) {
            throw Element.Exceptions.labelCanNotBeNull();
        }

        E.checkArgument(label instanceof String || label instanceof VertexLabel,
                        "Expect a string or a VertexLabel object " +
                        "as the vertex label argument, but got: '%s'", label);
        // The label must be an instance of String or VertexLabel
        if (label instanceof String) {
            if (verifyLabel) {
                ElementHelper.validateLabel((String) label);
            }
            label = graph().vertexLabel((String) label);
        }

        assert (label instanceof VertexLabel);
        return (VertexLabel) label;
    }

    private void checkId(Id id, List<Id> keys, VertexLabel vertexLabel) {
        // Check whether id match with id strategy
        IdStrategy strategy = vertexLabel.idStrategy();
        switch (strategy) {
            case PRIMARY_KEY:
                E.checkArgument(id == null,
                                "Can't customize vertex id when " +
                                "id strategy is '%s' for vertex label '%s'",
                                strategy, vertexLabel.name());
                // Check whether primaryKey exists
                List<Id> primaryKeys = vertexLabel.primaryKeys();
                E.checkArgument(keys.containsAll(primaryKeys),
                                "The primary keys: %s of vertex label '%s' " +
                                "must be set when using '%s' id strategy",
                                this.graph().mapPkId2Name(primaryKeys),
                                vertexLabel.name(), strategy);
                break;
            case AUTOMATIC:
                if (this.graph().restoring()) {
                    E.checkArgument(id != null && id.number(),
                                    "Must customize vertex number id when " +
                                    "id strategy is '%s' for vertex label " +
                                    "'%s' in restoring mode",
                                    strategy, vertexLabel.name());
                } else {
                    E.checkArgument(id == null,
                                    "Can't customize vertex id when " +
                                    "id strategy is '%s' for vertex label '%s'",
                                    strategy, vertexLabel.name());
                }
                break;
            case CUSTOMIZE_STRING:
                E.checkArgument(id != null && !id.number(),
                                "Must customize vertex string id when " +
                                "id strategy is '%s' for vertex label '%s'",
                                strategy, vertexLabel.name());
                break;
            case CUSTOMIZE_NUMBER:
                E.checkArgument(id != null && id.number(),
                                "Must customize vertex number id when " +
                                "id strategy is '%s' for vertex label '%s'",
                                strategy, vertexLabel.name());
                break;
            default:
                throw new AssertionError("Unknown id strategy: " + strategy);
        }
    }

    private void checkNonnullProperty(List<Id> keys, VertexLabel vertexLabel) {
        // Check whether passed all non-null property
        @SuppressWarnings("unchecked")
        Collection<Id> nonNullKeys = CollectionUtils.subtract(
                                     vertexLabel.properties(),
                                     vertexLabel.nullableKeys());
        if (!keys.containsAll(nonNullKeys)) {
            @SuppressWarnings("unchecked")
            Collection<Id> missed = CollectionUtils.subtract(nonNullKeys, keys);
            HugeGraph graph = this.graph();
            E.checkArgument(false, "All non-null property keys %s of " +
                            "vertex label '%s' must be setted, missed keys %s",
                            graph.mapPkId2Name(nonNullKeys), vertexLabel.name(),
                            graph.mapPkId2Name(missed));
        }
    }

    private Set<Id> relatedIndexLabels(HugeProperty<?> prop,
                                       Indexfiable indexfiable) {
        Id pkeyId = prop.propertyKey().id();
        return indexfiable.indexLabels().stream().filter(id ->
            graph().indexLabel(id).indexFields().contains(pkeyId)
        ).collect(Collectors.toSet());
    }

    private void lockForUpdateProperty(Set<Id> lockIds,
                                       Consumer<LockUtil.Locks> callback) {
        this.checkOwnerThread();

        LockUtil.Locks locks = new LockUtil.Locks();
        try {
            locks.lockReads(LockUtil.INDEX_LABEL_DELETE, lockIds);
            /*
             * No need to lock INDEX_LABEL_ADD_UPDATE, because index label
             * update only can add  user data, which is unconcerned with
             * update property
             */
            this.beforeWrite();
            callback.accept(locks);
            this.afterWrite();
        } finally {
            locks.unlock();
        }
    }

    private boolean filterResultFromIndexQuery(Query query, HugeElement elem) {
        if (!(query instanceof ConditionQuery)) {
            return true;
        }

        ConditionQuery cq = (ConditionQuery) query;
        if (cq.optimized() == 0 || cq.test(elem)) {
            /* Return true if:
             * 1.not query by index or by primary-key/sort-key (just by sysprop)
             * 2.the result match all conditions
             */
            return true;
        }

        if (cq.optimized() == OptimizedType.INDEX.ordinal()) {
            LOG.info("Remove left index: {}, query: {}", elem, cq);
            this.indexTx.removeIndexLeft(cq, elem);
        }
        return false;
    }

    private Iterator<?> joinTxVertices(Query query,
                                       Iterator<HugeVertex> vertices) {
        assert query.resultType().isVertex();
        return this.joinTxRecords(query, vertices,
                                  (q, v) -> q.test(v) ? v : null,
                                  this.addedVertexes, this.removedVertexes,
                                  this.updatedVertexes);
    }

    private Iterator<?> joinTxEdges(Query query, Iterator<HugeEdge> edges,
                                    Map<Id, HugeVertex> removingVertexes) {
        assert query.resultType().isEdge();
        final BiFunction<Query, HugeEdge, HugeEdge> matchTxEdges = (q, e) -> {
            assert q.resultType() == HugeType.EDGE;
            return q.test(e) ? e : q.test(e = e.switchOwner()) ? e : null;
        };
        edges = this.joinTxRecords(query, edges, matchTxEdges,
                                   this.addedEdges, this.removedEdges,
                                   this.updatedEdges);
        if (removingVertexes.isEmpty()) {
            return edges;
        }
        // Filter edges that belong to deleted vertex
        return new FilterIterator<HugeEdge>(edges, edge -> {
            for (HugeVertex v : removingVertexes.values()) {
                if (edge.belongToVertex(v)) {
                    return false;
                }
            }
            return true;
        });
    }

    private <V extends HugeElement> Iterator<V> joinTxRecords(
                                    Query query,
                                    Iterator<V> records,
                                    BiFunction<Query, V, V> match,
                                    Map<Id, V> addedTxRecords,
                                    Map<Id, V> removedTxRecords,
                                    Map<Id, V> updatedTxRecords) {
        // Return the origin results if there is no change in tx
        if (addedTxRecords.isEmpty() &&
            removedTxRecords.isEmpty() &&
            updatedTxRecords.isEmpty()) {
            return records;
        }

        Set<V> txResults = InsertionOrderUtil.newSet();

        /*
         * Collect added/updated records
         * Records in memory have higher priority than query from backend store
         */
        for (V elem : addedTxRecords.values()) {
            if (query.reachLimit(txResults.size())) {
                break;
            }
            if ((elem = match.apply(query, elem)) != null) {
                txResults.add(elem);
            }
        }
        for (V elem : updatedTxRecords.values()) {
            if (query.reachLimit(txResults.size())) {
                break;
            }
            if ((elem = match.apply(query, elem)) != null) {
                txResults.add(elem);
            }
        }

        // Filter backend record if it's updated in memory
        Iterator<V> backendResults = new FilterIterator<>(records, elem -> {
            return (!txResults.contains(elem) &&
                    !removedTxRecords.containsKey(elem.id()));
        });

        return new ExtendableIterator<V>(txResults.iterator(), backendResults);
    }

    private void checkTxVerticesCapacity() throws LimitExceedException {
        int size = this.addedVertexes.size() +
                   this.removedVertexes.size() +
                   this.updatedVertexes.size();
        if (size >= this.vertexesCapacity) {
            throw new LimitExceedException(
                      "Vertices size has reached tx capacity %d",
                      this.vertexesCapacity);
        }
    }

    private void checkTxEdgesCapacity() throws LimitExceedException {
        int size = this.addedEdges.size() +
                   this.removedEdges.size() +
                   this.updatedEdges.size();
        if (size >= this.edgesCapacity) {
            throw new LimitExceedException(
                      "Edges size has reached tx capacity %d",
                      this.edgesCapacity);
        }
    }

    private void propertyUpdated(HugeVertex vertex, HugeProperty<?> property) {
        this.updatedVertexes.put(vertex.id(), vertex);
        if (property != null) {
            this.updatedProps.add(property);
        }
    }

    private void propertyUpdated(HugeEdge edge, HugeProperty<?> property) {
        this.updatedEdges.put(edge.id(), edge);
        if (property != null) {
            this.updatedProps.add(property);
        }
    }

    public void removeIndex(IndexLabel indexLabel) {
        // TODO: use event to replace direct call
        this.checkOwnerThread();

        this.beforeWrite();
        this.indexTx.removeIndex(indexLabel);
        this.afterWrite();
    }

    public void rebuildIndex(SchemaElement schema) {
        // TODO: use event to replace direct call
        this.checkOwnerThread();

        this.beforeWrite();
        this.indexTx.rebuildIndex(schema);
        this.afterWrite();
    }

    public void removeVertices(VertexLabel vertexLabel) {
        if (this.hasUpdates()) {
            throw new BackendException("There are still changes to commit");
        }

        boolean autoCommit = this.autoCommit();
        this.autoCommit(false);
        // Commit data already in tx firstly
        this.commit();
        try {
            Iterator<Vertex> vertices = this.queryVerticesByLabel(
                                        vertexLabel, Query.NO_LIMIT);
            int count = 0;
            while (vertices.hasNext()) {
                this.removeVertex((HugeVertex) vertices.next());
                // Avoid reaching tx limit
                if (++count == this.vertexesCapacity) {
                    this.commit();
                }
            }
            this.commit();
        } catch (Exception e) {
            LOG.error("Failed to remove vertices", e);
            throw new HugeException("Failed to remove vertices", e);
        } finally {
            this.autoCommit(autoCommit);
        }
    }

    public void removeEdges(EdgeLabel edgeLabel) {
        if (this.hasUpdates()) {
            throw new BackendException("There are still changes to commit");
        }

        boolean autoCommit = this.autoCommit();
        this.autoCommit(false);
        // Commit data already in tx firstly
        this.commit();
        try {
            if (this.store().features().supportsDeleteEdgeByLabel()) {
                // TODO: Need to change to writeQuery!
                this.doRemove(this.serializer.writeId(HugeType.EDGE_OUT,
                                                      edgeLabel.id()));
                this.doRemove(this.serializer.writeId(HugeType.EDGE_IN,
                                                      edgeLabel.id()));
            } else {
                Iterator<Edge> edges = this.queryEdgesByLabel(edgeLabel,
                                                              Query.NO_LIMIT);
                int count = 0;
                while (edges.hasNext()) {
                    this.removeEdge((HugeEdge) edges.next());
                    // Avoid reaching tx limit
                    if (++count == this.edgesCapacity) {
                        this.commit();
                    }
                }
            }
            this.commit();
        } catch (Exception e) {
            LOG.error("Failed to remove edges", e);
            throw new HugeException("Failed to remove edges", e);
        } finally {
            this.autoCommit(autoCommit);
        }
    }

    public Iterator<Vertex> queryVerticesByLabel(VertexLabel vertexLabel,
                                                 long limit) {
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.limit(limit);
        query.capacity(Query.NO_CAPACITY);
        if (vertexLabel.hidden()) {
            query.showHidden(true);
        }
        if (!vertexLabel.enableLabelIndex()) {
            return new FilterIterator<>(this.queryVertices(query), vertex -> {
                return vertex.label().equals(vertexLabel.name());
            });
        } else {
            query.eq(HugeKeys.LABEL, vertexLabel.id());
            return this.queryVertices(query);
        }
    }

    public Iterator<Edge> queryEdgesByLabel(EdgeLabel edgeLabel, long limit) {
        ConditionQuery query = new ConditionQuery(HugeType.EDGE);
        query.limit(limit);
        query.capacity(Query.NO_CAPACITY);
        if (edgeLabel.hidden()) {
            query.showHidden(true);
        }
        if (!edgeLabel.enableLabelIndex()) {
            return new FilterIterator<>(this.queryEdges(query), edge -> {
                return edge.label().equals(edgeLabel.name());
            });
        } else {
            query.eq(HugeKeys.LABEL, edgeLabel.id());
            return this.queryEdges(query);
        }
    }
}
