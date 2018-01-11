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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
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
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.exception.NotSupportException;
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
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;
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
     * NOTE: props updates will be put into entries directly(due to difficult)
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
    protected void reset() {
        super.reset();

        // Clear mutation
        this.addedVertexes = this.newMapWithInsertionOrder();
        this.removedVertexes = this.newMapWithInsertionOrder();
        this.updatedVertexes = this.newMapWithInsertionOrder();

        this.addedEdges = this.newMapWithInsertionOrder();
        this.removedEdges = this.newMapWithInsertionOrder();
        this.updatedEdges = this.newMapWithInsertionOrder();

        this.updatedProps = this.newSetWithInsertionOrder();
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

    protected <V> Map<Id, V> newMapWithInsertionOrder() {
        return new LinkedHashMap<Id, V>();
    }

    protected <V> Set<V> newSetWithInsertionOrder() {
        return new LinkedHashSet<V>();
    }

    @Watched(prefix = "tx")
    @Override
    protected BackendMutation prepareCommit() {
        // Serialize and add updates into super.deletions
        this.prepareDeletions(this.removedVertexes, this.removedEdges);
        // Serialize and add updates into super.additions
        this.prepareAdditions(this.addedVertexes, this.addedEdges);
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
            // Add edge entry of OUT and IN
            this.doInsert(this.serializer.writeEdge(e));
            this.doInsert(this.serializer.writeEdge(e.switchOwner()));
            // Update index of edge
            this.indexTx.updateEdgeIndex(e, false);
            this.indexTx.updateLabelIndex(e, false);
        }

        // Clear updates
        addedVertexes.clear();
        addedEdges.clear();
    }

    protected void prepareDeletions(Map<Id, HugeVertex> removedVertexes,
                                    Map<Id, HugeEdge> removedEdges) {

        Map<Id, HugeVertex> vertexes = this.newMapWithInsertionOrder();
        vertexes.putAll(removedVertexes);

        Map<Id, HugeEdge> edges = this.newMapWithInsertionOrder();
        edges.putAll(removedEdges);

        // Clear updates
        removedVertexes.clear();
        removedEdges.clear();

        // In order to remove edges of vertexes, query all edges first
        for (HugeVertex v : vertexes.values()) {
            Iterator<Edge> vedges = this.queryEdgesByVertex(v.id());
            while (vedges.hasNext()) {
                HugeEdge edge = (HugeEdge) vedges.next();
                edges.put(edge.id(), edge);
            }
        }

        // Remove vertexes
        for (HugeVertex v : vertexes.values()) {
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
        for (HugeEdge e : edges.values()) {
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
        if (query instanceof ConditionQuery) {
            query = this.optimizeQuery((ConditionQuery) query);
            /*
             * NOTE: There are two possibilities for this query:
             * 1.sysprop-query, which would not be empty.
             * 2.index-query result(ids after optimize), which may be empty.
             */
            if (query.empty()) {
                // Return empty if there is no result after index-query
                return ImmutableList.<BackendEntry>of().iterator();
            }
        }
        return super.query(query);
    }

    @Watched("graph.addVertex-with-instance")
    public Vertex addVertex(HugeVertex vertex) {
        this.checkOwnerThread();

        // Override vertexes in local `removedVertexes`
        this.removedVertexes.remove(vertex.id());

        LockUtil.Locks locks = new LockUtil.Locks();
        try {
            locks.lockReads(LockUtil.VERTEX_LABEL, vertex.schemaLabel().id());
            locks.lockReads(LockUtil.INDEX_LABEL,
                            vertex.schemaLabel().indexLabels());
            this.beforeWrite();
            this.addedVertexes.put(vertex.id(), vertex);
            this.afterWrite();
        } finally {
            locks.unlock();
        }
        return vertex;
    }

    @Watched(prefix = "graph")
    public Vertex addVertex(Object... keyValues) {
        HugeElement.ElementKeys elemKeys = HugeElement.classifyKeys(keyValues);

        VertexLabel vertexLabel = this.checkVertexLabel(elemKeys.label());
        Id id = HugeVertex.getIdValue(elemKeys.id());

        IdStrategy strategy = vertexLabel.idStrategy();
        // Check weather id strategy match with id
        strategy.checkId(id, vertexLabel.name());

        List<Id> keys = this.graph().mapPkName2Id(elemKeys.keys());
        // Check id strategy
        if (strategy == IdStrategy.PRIMARY_KEY) {
            // Check whether primaryKey exists
            List<Id> primaryKeys = vertexLabel.primaryKeys();
            E.checkArgument(CollectionUtil.containsAll(keys, primaryKeys),
                            "The primary keys: %s of vertex label '%s' " +
                            "must be set when using '%s' id strategy",
                            this.graph().mapPkId2Name(primaryKeys),
                            vertexLabel.name(), strategy);
        }

        // Check weather passed all non-null props
        @SuppressWarnings("unchecked")
        Collection<Id> nonNullKeys = CollectionUtils.subtract(
                                     vertexLabel.properties(),
                                     vertexLabel.nullableKeys());
        if (!keys.containsAll(nonNullKeys)) {
            @SuppressWarnings("unchecked")
            Collection<Id> missed = CollectionUtils.subtract(nonNullKeys, keys);
            E.checkArgument(false, "All non-null property keys: %s " +
                            "of vertex label '%s' must be setted, " +
                            "but missed keys: %s",
                            this.graph().mapPkId2Name(nonNullKeys),
                            vertexLabel.name(),
                            this.graph().mapPkId2Name(missed));
        }

        // Create HugeVertex
        HugeVertex vertex = new HugeVertex(this, null, vertexLabel);

        // Set properties
        ElementHelper.attachProperties(vertex, keyValues);

        vertex.assignId(id);

        return this.addVertex(vertex);
    }

    @Watched(prefix = "graph")
    public void removeVertex(HugeVertex vertex) {
        this.checkOwnerThread();

        this.beforeWrite();

        // Override vertexes in local `addedVertexes`
        this.addedVertexes.remove(vertex.id());

        // Get edges in local `addedEdges`, and remove it
        for (Iterator<Map.Entry<Id, HugeEdge>> itor =
             this.addedEdges.entrySet().iterator(); itor.hasNext();) {
            if (itor.next().getValue().belongToVertex(vertex)) {
                itor.remove();
            }
        }

        // Collect the removed vertex
        this.removedVertexes.put(vertex.id(), vertex);

        this.afterWrite();
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
        assert query.resultType() == HugeType.VERTEX;

        Iterator<BackendEntry> entries = this.query(query);

        Iterator<HugeVertex> results = new MapperIterator<>(entries, entry -> {
            HugeVertex vertex = this.serializer.readVertex(graph(), entry);
            assert vertex != null;
            return vertex;
        });

        results = new FilterIterator<>(results, vertex -> {
            // Filter hidden results
            if (!query.showHidden() && Graph.Hidden.isHidden(vertex.label())) {
                return false;
            }
            // Process results that query from left index
            if (query.resultType() == HugeType.VERTEX &&
                !filterResultFromIndexQuery(query, vertex)) {
                return false;
            }
            return true;
        });

        @SuppressWarnings("unchecked")
        Iterator<Vertex> r = (Iterator<Vertex>) joinTxVertices(query, results);
        return r;
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

    @Watched(prefix = "graph")
    public Edge addEdge(HugeEdge edge) {
        this.checkOwnerThread();

        // Override edges in local `removedEdges`
        this.removedEdges.remove(edge.id());

        LockUtil.Locks locks = new LockUtil.Locks();
        try {
            locks.lockReads(LockUtil.EDGE_LABEL, edge.schemaLabel().id());
            locks.lockReads(LockUtil.INDEX_LABEL,
                            edge.schemaLabel().indexLabels());
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
        assert query.resultType() == HugeType.EDGE;

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

        Iterator<HugeEdge> results = new FlatMapperFilterIterator<>(entries,
                                                                    mapper,
                                                                    filter);
        @SuppressWarnings("unchecked")
        Iterator<Edge> r = (Iterator<Edge>) joinTxEdges(query, results);
        return r;
    }

    public Iterator<Edge> queryEdgesByVertex(Id id) {
        return queryEdges(constructEdgesQuery(id, null));
    }

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
        E.checkArgument(!this.addedVertexes.containsKey(vertex.id()),
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

            // Append new property
            this.doAppend(this.serializer.writeVertexProperty(prop));
        });
    }

    public <V> void removeVertexProperty(HugeVertexProperty<V> prop) {
        HugeVertex vertex = prop.element();
        PropertyKey propertyKey = prop.propertyKey();
        E.checkState(vertex != null,
                     "No owner for removing property '%s'", prop.key());

        // Maybe have ever been removed (compatible with tinkerpop)
        if (!vertex.hasProperty(propertyKey.id())) {
            // PropertyTest shouldAllowRemovalFromVertexWhenAlreadyRemoved()
            return;
        }
        // Check is removing primary key
        List<Id> primaryKeyIds = vertex.schemaLabel().primaryKeys();
        E.checkArgument(!primaryKeyIds.contains(propertyKey.id()),
                        "Can't remove primary key '%s'", prop.key());
        // Remove property in memory for new created vertex
        if (vertex.fresh()) {
            // The owner will do property update
            vertex.removeProperty(propertyKey.id());
            return;
        }
        // Check is updating property of added/removed vertex
        E.checkArgument(!this.addedVertexes.containsKey(vertex.id()),
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
            this.propertyUpdated(vertex,
                                 vertex.removeProperty(propertyKey.id()));
            this.indexTx.updateVertexIndex(vertex, false);

            // Eliminate the property
            this.doEliminate(this.serializer.writeVertexProperty(prop));
        });
    }

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
        E.checkArgument(!edge.schemaLabel().sortKeys().contains(
                        prop.propertyKey().id()),
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

    public <V> void removeEdgeProperty(HugeEdgeProperty<V> prop) {
        HugeEdge edge = prop.element();
        PropertyKey propertyKey = prop.propertyKey();
        E.checkState(edge != null,
                     "No owner for removing property '%s'", prop.key());

        // Maybe have ever been removed
        if (!edge.hasProperty(propertyKey.id())) {
            return;
        }
        // Check is removing sort key
        List<Id> sortKeyIds = edge.schemaLabel().sortKeys();
        E.checkArgument(!sortKeyIds.contains(prop.propertyKey().id()),
                        "Can't remove sort key '%s'", prop.key());
        // Remove property in memory for new created edge
        if (edge.fresh()) {
            // The owner will do property update
            edge.removeProperty(propertyKey.id());
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
            this.propertyUpdated(edge, edge.removeProperty(propertyKey.id()));
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

    public static ConditionQuery constructEdgesQuery(Id sourceVertex,
                                                     Directions directions,
                                                     Id... edgeLabels) {
        E.checkState(sourceVertex != null,
                     "The edge query must contain source vertex");
        E.checkState((directions != null || edgeLabels.length == 0),
                     "The edge query must contain direction " +
                     "if it contains edge label");

        ConditionQuery query = new ConditionQuery(HugeType.EDGE);

        // Edge source vertex
        query.eq(HugeKeys.OWNER_VERTEX, sourceVertex);

        // Edge direction
        if (directions != null) {
            assert directions == Directions.OUT || directions == Directions.IN;
            query.eq(HugeKeys.DIRECTION, directions);
        }

        // Edge labels
        if (edgeLabels.length == 1) {
            query.eq(HugeKeys.LABEL, edgeLabels[0]);
        } else if (edgeLabels.length > 1) {
            // TODO: support query by multi edge labels like:
            // query.query(Condition.in(HugeKeys.LABEL, edgeLabels));
            throw new NotSupportException("querying by multi edge-labels");
        } else {
            assert edgeLabels.length == 0;
        }

        return query;
    }

    public static boolean matchEdgeSortKeys(ConditionQuery query,
                                            HugeGraph graph) {
        assert query.resultType() == HugeType.EDGE;

        Id label = (Id) query.condition(HugeKeys.LABEL);
        if (label == null) {
            return false;
        }
        List<Id> keys = graph.edgeLabel(label).sortKeys();
        return !keys.isEmpty() && query.matchUserpropKeys(keys);
    }

    public static void verifyEdgesConditionQuery(ConditionQuery query) {
        assert query.resultType() == HugeType.EDGE;

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
        if (label != null && query.resultType() == HugeType.VERTEX) {
            VertexLabel vertexLabel = this.graph().vertexLabel(label);
            if (vertexLabel.idStrategy() == IdStrategy.PRIMARY_KEY) {
                List<Id> keys = vertexLabel.primaryKeys();
                if (keys.isEmpty()) {
                    throw new BackendException(
                              "The primary keys can't be empty when using " +
                              "'%s' id strategy for vertex label '%s'",
                              IdStrategy.PRIMARY_KEY, vertexLabel.name());
                }
                if (query.matchUserpropKeys(keys)) {
                    // Query vertex by label + primary-values
                    String primaryValues = query.userpropValuesString(keys);
                    LOG.debug("Query vertices by primaryKeys: {}", query);
                    // Convert {vertex-label + primary-key} to vertex-id
                    Id id = SplicingIdGenerator.splicing(
                                                vertexLabel.id().asString(),
                                                primaryValues);
                    query.query(id);
                    query.resetConditions();

                    return query;
                }
            }
        }

        // Optimize edge query
        if (label != null && query.resultType() == HugeType.EDGE) {
            List<Id> keys = this.graph().edgeLabel(label).sortKeys();
            if (query.condition(HugeKeys.OWNER_VERTEX) != null &&
                query.condition(HugeKeys.DIRECTION) != null &&
                !keys.isEmpty() && query.matchUserpropKeys(keys)) {
                // Query edge by sourceVertex + direction + label + sort-values
                query.eq(HugeKeys.SORT_VALUES,
                         query.userpropValuesString(keys));
                query.resetUserpropConditions();
                LOG.debug("Query edges by sortKeys: {}", query);
                return query;
            }
        }

        /*
         * Query only by sysprops, like: vertex label, edge label.
         * NOTE: we assume sysprops would be indexed by backend store
         * but we don't support query edges only by direction/targetVertex.
         */
        if (query.allSysprop()) {
            if (query.resultType() == HugeType.EDGE) {
                verifyEdgesConditionQuery(query);
            }
            if (this.store().features().supportsQueryByLabel() ||
                !(label != null && query.conditions().size() == 1)) {
                return query;
            }
        }

        /*
         * Optimize by index-query
         * It will return a list of id(maybe empty) if success,
         * or throw exception if there is no any index for query properties.
         */
        this.beforeRead();
        Query result = this.indexTx.query(query);
        this.afterRead();
        return result;
    }

    private VertexLabel checkVertexLabel(Object label) {
        HugeVertexFeatures features = graph().features().vertex();

        // Check Vertex label
        if (label == null && features.supportsDefaultLabel()) {
            label = features.defaultLabel();
        }

        if (label == null) {
            throw Element.Exceptions.labelCanNotBeNull();
        }

        E.checkArgument(label instanceof String ||
                        label instanceof VertexLabel,
                        "Expect a string or a VertexLabel object " +
                        "as the vertex label argument, but got: '%s'", label);
        // The label must be an instance of String or VertexLabel
        if (label instanceof String) {
            ElementHelper.validateLabel((String) label);
            label = graph().vertexLabel((String) label);
        }

        assert (label instanceof VertexLabel);
        return (VertexLabel) label;
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
            locks.lockReads(LockUtil.INDEX_LABEL, lockIds);

            this.beforeWrite();
            callback.accept(locks);
            this.afterWrite();
        } finally {
            locks.unlock();
        }
    }

    private boolean filterResultFromIndexQuery(Query query, HugeElement elem) {
        if (!(query instanceof ConditionQuery) || query.originQuery() == null) {
            // Not query by index, query.originQuery() is not null when index
            return true;
        }
        ConditionQuery cq = (ConditionQuery) query;
        if (cq.test(elem)) {
            return true;
        } else {
            LOG.info("Remove left index: {}, query: {}", elem, cq);
            this.indexTx.removeIndexLeft(cq, elem);
            return false;
        }
    }

    private Iterator<?> joinTxVertices(Query query,
                                       Iterator<HugeVertex> vertices) {
        assert query.resultType() == HugeType.VERTEX;
        return this.joinTxRecords(query, vertices, (q, v) -> q.test(v),
                                  this.addedVertexes, this.removedVertexes,
                                  this.updatedVertexes);
    }

    private Iterator<?> joinTxEdges(Query query, Iterator<HugeEdge> edges) {
        assert query.resultType() == HugeType.EDGE;
        final BiFunction<Query, HugeEdge, Boolean> matchTxEdges = (q, e) -> {
            assert q.resultType() == HugeType.EDGE;
            return q.test(e) || q.test(e.switchOwner());
        };
        edges = this.joinTxRecords(query, edges, matchTxEdges,
                                   this.addedEdges, this.removedEdges,
                                   this.updatedEdges);
        if (this.removedVertexes.isEmpty()) {
            return edges;
        }
        // Filter edges that belong to deleted vertex
        return new FilterIterator<HugeEdge>(edges, edge -> {
            for (HugeVertex v : this.removedVertexes.values()) {
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
                                    BiFunction<Query, V, Boolean> match,
                                    Map<Id, V> addedTxRecords,
                                    Map<Id, V> removedTxRecords,
                                    Map<Id, V> updatedTxRecords) {
        // Return the origin results if there is no change in tx
        if (addedTxRecords.isEmpty() &&
            removedTxRecords.isEmpty() &&
            updatedTxRecords.isEmpty()) {
            return records;
        }

        Set<V> txResults = this.newSetWithInsertionOrder();

        /* Collect added records
         * Records in memory have higher priority than query from backend store
         */
        for (V elem : addedTxRecords.values()) {
            if (match.apply(query, elem)) {
                txResults.add(elem);
            }
        }
        for (V elem : updatedTxRecords.values()) {
            if (match.apply(query, elem)) {
                txResults.add(elem);
            }
        }

        // Filter removed/added records
        Iterator<V> backendResults = new FilterIterator<>(records, elem -> {
            return (!txResults.contains(elem) &&
                    !removedTxRecords.containsKey(elem.id()));
        });

        return new ExtendableIterator<V>(txResults.iterator(), backendResults);
    }

    private void checkTxVerticesCapacity() {
        int size = this.addedVertexes.size() +
                   this.removedVertexes.size() +
                   this.updatedVertexes.size();
        if (size >= this.vertexesCapacity) {
            throw new BackendException(
                      "Vertices in transaction have reached capacity %d",
                      this.vertexesCapacity);
        }
    }

    private void checkTxEdgesCapacity() {
        int size = this.addedEdges.size() +
                   this.removedEdges.size() +
                   this.updatedEdges.size();
        if (size >= this.edgesCapacity) {
            throw new BackendException(
                      "Edges in transaction have reached capacity %d",
                      this.edgesCapacity);
        }
    }

    private void propertyUpdated(HugeEdge edge, HugeProperty<?> property) {
        this.updatedEdges.put(edge.id(), edge);
        if (property != null) {
            this.updatedProps.add(property);
        }
    }

    private void propertyUpdated(HugeVertex vertex, HugeProperty<?> property) {
        this.updatedVertexes.put(vertex.id(), vertex);
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
        try {
            ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
            // TODO: use query.capacity(Query.NO_LIMIT);
            query.eq(HugeKeys.LABEL, vertexLabel.id());
            if (vertexLabel.hidden()) {
                query.showHidden(true);
            }

            for (Iterator<Vertex> vertices = queryVertices(query);
                 vertices.hasNext();) {
                this.removeVertex((HugeVertex) vertices.next());
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
        try {
            if (this.store().features().supportsDeleteEdgeByLabel()) {
                // TODO: Need to change to writeQuery!
                this.doRemove(this.serializer.writeId(HugeType.EDGE,
                                                      edgeLabel.id()));
            } else {
                ConditionQuery query = new ConditionQuery(HugeType.EDGE);
                // TODO: use query.capacity(Query.NO_LIMIT);
                query.eq(HugeKeys.LABEL, edgeLabel.id());
                if (edgeLabel.hidden()) {
                    query.showHidden(true);
                }
                for (Iterator<Edge> edges = queryEdges(query);
                     edges.hasNext();) {
                    this.removeEdge((HugeEdge) edges.next());
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
}
