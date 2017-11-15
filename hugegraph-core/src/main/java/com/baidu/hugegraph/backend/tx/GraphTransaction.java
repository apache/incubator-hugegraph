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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
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
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
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
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.type.define.IdStrategy;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.LockUtil;
import com.google.common.collect.ImmutableList;

public class GraphTransaction extends AbstractTransaction {

    private final IndexTransaction indexTx;

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

        this.indexTx = new IndexTransaction(graph, store);
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

        // It's null when called by super AbstractTransaction()
        if (this.indexTx != null) {
            this.indexTx.reset();
        }
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
            this.addEntry(this.serializer.writeVertex(v));
            // Update index of vertex(only include props)
            this.indexTx.updateVertexIndex(v, false);
        }

        // Do edge update
        for (HugeEdge e : addedEdges.values()) {
            assert !e.removed();
            e.committed();
            // Add edge entry of OUT and IN
            this.addEntry(this.serializer.writeEdge(e));
            this.addEntry(this.serializer.writeEdge(e.switchOwner()));
            // Update index of edge
            this.indexTx.updateEdgeIndex(e, false);
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
            Iterable<Edge> vedges = this.queryEdgesByVertex(v.id());
            for (Iterator<Edge> i = vedges.iterator(); i.hasNext();) {
                HugeEdge edge = (HugeEdge) i.next();
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
            this.removeEntry(this.serializer.writeVertex(v.prepareRemoved()));
            this.indexTx.updateVertexIndex(v, true);
        }

        // Remove edges
        for (HugeEdge edge : edges.values()) {
            // Update edge index
            this.indexTx.updateEdgeIndex(edge, true);
            // Remove edge of OUT and IN
            edge = edge.prepareRemoved();
            this.removeEntry(this.serializer.writeEdge(edge));
            this.removeEntry(this.serializer.writeEdge(edge.switchOwner()));
        }
    }

    @Override
    protected void commit2Backend(BackendMutation mutation) {
        // If an exception occurred, catch in the upper layer and roll back
        BackendStore store = this.store();
        store.beginTx();
        // Commit graph updates
        store.mutate(mutation);
        // Commit index updates with graph tx
        store.mutate(this.indexTx.prepareCommit());
        store.commitTx();
    }

    @Override
    public void rollback() throws BackendException {
        // Rollback properties changes
        for (HugeProperty<?> prop : this.updatedProps) {
            prop.element().setProperty(prop);
        }

        try {
            super.rollback();
        } finally {
            this.indexTx.rollback();
        }
    }

    @Override
    public void close() {
        try {
            this.indexTx.close();
        } finally {
            super.close();
        }
    }

    @Override
    public Iterable<BackendEntry> query(Query query) {
        if (query instanceof ConditionQuery) {
            query = this.optimizeQuery((ConditionQuery) query);
            /*
             * NOTE: There are two possibilities for this query:
             * 1.sysprop-query, which would not be empty.
             * 2.index-query result(ids after optimize), which may be empty.
             */
            if (query.empty()) {
                // Return empty if there is no result after index-query
                return ImmutableList.of();
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
            locks.lockReads(LockUtil.VERTEX_LABEL, vertex.label());
            locks.lockReads(LockUtil.INDEX_LABEL,
                            vertex.vertexLabel().indexNames());
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
        Id id = HugeElement.getIdValue(elemKeys.id());
        Set<String> keys = elemKeys.keys();

        IdStrategy strategy = vertexLabel.idStrategy();

        // Check weather id strategy match with id
        strategy.checkId(id, vertexLabel.name());

        // Check id strategy
        if (strategy == IdStrategy.PRIMARY_KEY) {
            // Check whether primaryKey exists
            List<String> primaryKeys = vertexLabel.primaryKeys();
            E.checkArgument(CollectionUtil.containsAll(keys, primaryKeys),
                            "The primary keys: %s of vertex label '%s' " +
                            "must be set when using '%s' id strategy",
                            primaryKeys, vertexLabel.name(), strategy);
        }

        // Check weather passed all non-null props
        Collection<?> nonNullKeys = CollectionUtils.subtract(
                                    vertexLabel.properties(),
                                    vertexLabel.nullableKeys());
        if (!keys.containsAll(nonNullKeys)) {
            E.checkArgument(false, "All non-null property keys: %s " +
                            "of vertex label '%s' must be setted, " +
                            "but missed keys: %s",
                            nonNullKeys, vertexLabel.name(),
                            CollectionUtils.subtract(nonNullKeys, keys));
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

    public Iterable<Vertex> queryVertices(Object... vertexIds) {
        // NOTE: it will allow duplicated vertices if query by duplicated ids
        List<Vertex> results = new ArrayList<>(vertexIds.length);

        for (Object vertexId : vertexIds) {
            Vertex vertex;
            Id id = HugeElement.getIdValue(vertexId);
            if (this.removedVertexes.containsKey(id)) {
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
                    vertex = this.serializer.readVertex(entry, this.graph());
                } catch (NotFoundException ignored) {
                    continue;
                }
                assert vertex != null;
                results.add(vertex);
            }
        }

        return results;
    }

    public Iterable<Vertex> queryVertices() {
        Query q = new Query(HugeType.VERTEX);
        return this.queryVertices(q);
    }

    public Iterable<Vertex> queryVertices(Query query) {
        assert Arrays.asList(HugeType.VERTEX, HugeType.EDGE)
                     .contains(query.resultType());
        Set<HugeVertex> results = this.newSetWithInsertionOrder();

        Iterator<BackendEntry> entries = this.query(query).iterator();
        while (entries.hasNext()) {
            BackendEntry entry = entries.next();
            HugeVertex vertex = this.serializer.readVertex(entry, graph());
            assert vertex != null;
            // Filter hidden results
            if (!query.showHidden() &&
                Graph.Hidden.isHidden(vertex.label())) {
                continue;
            }
            // Process results that query from left index
            if (query.resultType() == HugeType.VERTEX &&
                !this.filterResultFromIndexQuery(query, vertex)) {
                continue;
            }
            results.add(vertex);
        }

        @SuppressWarnings("unchecked")
        Iterable<Vertex> r = (Iterable<Vertex>) joinTxVertices(query, results);
        return r;
    }

    public Iterable<Vertex> queryAdjacentVertices(Iterator<Edge> edges) {
        if (!edges.hasNext()) {
            return ImmutableList.<Vertex>of();
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
            locks.lockReads(LockUtil.EDGE_LABEL, edge.label());
            locks.lockReads(LockUtil.INDEX_LABEL,
                            edge.edgeLabel().indexNames());
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

    public Iterable<Edge> queryEdges(Object... edgeIds) {
        // NOTE: it will allow duplicated edges if query by duplicated ids
        List<Edge> results = new ArrayList<>(edgeIds.length);

        for (Object edgeId : edgeIds) {
            Edge edge;
            Id id = HugeElement.getIdValue(edgeId);
            if (this.removedEdges.containsKey(id)) {
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
                HugeVertex vertex = this.serializer.readVertex(entry, graph());
                assert vertex != null;
                assert vertex.getEdges().size() == 1;
                results.addAll(vertex.getEdges());
            }
        }

        return results;
    }

    public Iterable<Edge> queryEdges() {
        Query q = new Query(HugeType.EDGE);
        return this.queryEdges(q);
    }

    public Iterable<Edge> queryEdges(Query query) {
        assert query.resultType() == HugeType.EDGE;
        Iterator<Vertex> vertices = this.queryVertices(query).iterator();

        Map<Id, HugeEdge> results = this.newMapWithInsertionOrder();
        while (vertices.hasNext()) {
            HugeVertex vertex = (HugeVertex) vertices.next();
            for (HugeEdge edge : vertex.getEdges()) {
                // Filter hidden results
                if (!query.showHidden() &&
                    Graph.Hidden.isHidden(edge.label())) {
                    continue;
                }
                // Process results that query from left index
                if (!this.filterResultFromIndexQuery(query, edge)) {
                    continue;
                }
                // Filter repeat results
                if (!results.containsKey(edge.id())) {
                    /*
                     * NOTE: Maybe some edges are IN and others are OUT if
                     * querying edges both directions, perhaps it would look
                     * better if we convert all edges in results to OUT, but
                     * that would break the logic when querying IN edges.
                     */
                    results.put(edge.id(), edge);
                } else {
                    LOG.debug("Results contains edge: {}", edge);
                }
            }
        }

        @SuppressWarnings("unchecked")
        Iterable<Edge> r = (Iterable<Edge>) joinTxEdges(query,
                                                        results.values());
        return r;
    }

    public Iterable<Edge> queryEdgesByVertex(Id id) {
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
        List<String> primaryKeys = vertex.vertexLabel().primaryKeys();
        E.checkArgument(!primaryKeys.contains(prop.key()),
                        "Can't update primary key: '%s'", prop.key());

        Set<String> lockNames = relatedIndexNames(prop.name(),
                                                  vertex.vertexLabel());
        this.lockForUpdateProperty(lockNames, (locks) -> {
            // Update old vertex to remove index (without new property)
            this.indexTx.updateVertexIndex(vertex, true);

            // Update index of current vertex (with new property)
            this.propertyUpdated(vertex, vertex.setProperty(prop));
            this.indexTx.updateVertexIndex(vertex, false);

            // Append new property
            this.appendEntry(this.serializer.writeVertexProperty(prop));
        });
    }

    public <V> void removeVertexProperty(HugeVertexProperty<V> prop) {
        HugeVertex vertex = prop.element();
        E.checkState(vertex != null,
                     "No owner for removing property '%s'", prop.key());

        // Maybe have ever been removed (compatible with tinkerpop)
        if (!vertex.hasProperty(prop.key())) {
            // PropertyTest shouldAllowRemovalFromVertexWhenAlreadyRemoved()
            return;
        }
        // Check is removing primary key
        List<String> primaryKeys = vertex.vertexLabel().primaryKeys();
        E.checkArgument(!primaryKeys.contains(prop.key()),
                        "Can't remove primary key '%s'", prop.key());
        // Remove property in memory for new created vertex
        if (vertex.fresh()) {
            // The owner will do property update
            vertex.removeProperty(prop.key());
            return;
        }
        // Check is updating property of added/removed vertex
        E.checkArgument(!this.addedVertexes.containsKey(vertex.id()),
                        "Can't remove property '%s' for adding-state vertex",
                        prop.key());
        E.checkArgument(!this.removedVertexes.containsKey(vertex.id()),
                        "Can't remove property '%s' for removing-state vertex",
                        prop.key());

        Set<String> lockNames = relatedIndexNames(prop.name(),
                                                  vertex.vertexLabel());
        this.lockForUpdateProperty(lockNames, (locks) -> {
            // Update old vertex to remove index (with the property)
            this.indexTx.updateVertexIndex(vertex, true);

            // Update index of current vertex (without the property)
            this.propertyUpdated(vertex, vertex.removeProperty(prop.key()));
            this.indexTx.updateVertexIndex(vertex, false);

            // Eliminate the property
            this.eliminateEntry(this.serializer.writeVertexProperty(prop));
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
        E.checkArgument(!this.addedEdges.containsKey(edge.id()),
                        "Can't update property '%s' for adding-state edge",
                        prop.key());
        E.checkArgument(!edge.removed() &&
                        !this.removedEdges.containsKey(edge.id()),
                        "Can't update property '%s' for removing-state edge",
                        prop.key());
        // Check is updating sort key
        E.checkArgument(!edge.edgeLabel().sortKeys().contains(prop.key()),
                        "Can't update sort key '%s'", prop.key());

        Set<String> lockNames = relatedIndexNames(prop.name(),
                                                  edge.edgeLabel());
        this.lockForUpdateProperty(lockNames, (locks) -> {
            // Update old edge to remove index (without new property)
            this.indexTx.updateEdgeIndex(edge, true);

            // Update index of current edge (with new property)
            this.propertyUpdated(edge, edge.setProperty(prop));
            this.indexTx.updateEdgeIndex(edge, false);

            // Append new property(OUT and IN owner edge)
            this.appendEntry(this.serializer.writeEdgeProperty(prop));
            this.appendEntry(this.serializer.writeEdgeProperty(
                             prop.switchEdgeOwner()));
        });
    }

    public <V> void removeEdgeProperty(HugeEdgeProperty<V> prop) {
        HugeEdge edge = prop.element();
        E.checkState(edge != null,
                     "No owner for removing property '%s'", prop.key());

        // Maybe have ever been removed
        if (!edge.hasProperty(prop.key())) {
            return;
        }
        // Check is removing sort key
        E.checkArgument(!edge.edgeLabel().sortKeys().contains(prop.key()),
                        "Can't remove sort key '%s'", prop.key());
        // Remove property in memory for new created edge
        if (edge.fresh()) {
            // The owner will do property update
            edge.removeProperty(prop.key());
            return;
        }
        // Check is updating property of added/removed edge
        E.checkArgument(!this.addedEdges.containsKey(edge.id()),
                        "Can't remove property '%s' for adding-state edge",
                        prop.key());
        E.checkArgument(!this.removedEdges.containsKey(edge.id()),
                        "Can't remove property '%s' for removing-state edge",
                        prop.key());

        Set<String> lockNames = relatedIndexNames(prop.name(),
                                                  edge.edgeLabel());
        this.lockForUpdateProperty(lockNames, (locks) -> {
            // Update old edge to remove index (with the property)
            this.indexTx.updateEdgeIndex(edge, true);

            // Update index of current edge (without the property)
            this.propertyUpdated(edge, edge.removeProperty(prop.key()));
            this.indexTx.updateEdgeIndex(edge, false);

            // Eliminate the property(OUT and IN owner edge)
            this.eliminateEntry(this.serializer.writeEdgeProperty(prop));
            this.eliminateEntry(this.serializer.writeEdgeProperty(
                                prop.switchEdgeOwner()));
        });
    }

    public static ConditionQuery constructEdgesQuery(Id sourceVertex,
                                                     Direction direction,
                                                     String... edgeLabels) {
        E.checkState(sourceVertex != null,
                     "The edge query must contain source vertex");
        E.checkState((direction != null ||
                     (direction == null && edgeLabels.length == 0)),
                     "The edge query must contain direction " +
                     "if it contains edge label");

        ConditionQuery query = new ConditionQuery(HugeType.EDGE);

        // Edge source vertex
        query.eq(HugeKeys.OWNER_VERTEX, sourceVertex);

        // Edge direction
        if (direction != null) {
            assert direction == Direction.OUT || direction == Direction.IN;
            query.eq(HugeKeys.DIRECTION, direction);
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

        String label = (String) query.condition(HugeKeys.LABEL);
        if (label == null) {
            return false;
        }
        List<String> keys = graph.edgeLabel(label).sortKeys();
        return !keys.isEmpty() && query.matchUserpropKeys(keys);
    }

    public static void verifyEdgesConditionQuery(ConditionQuery query) {
        assert query.resultType() == HugeType.EDGE;

        final HugeKeys[] keys = new HugeKeys[] {
                HugeKeys.OWNER_VERTEX,
                HugeKeys.DIRECTION,
                HugeKeys.LABEL,
                HugeKeys.SORT_VALUES,
                HugeKeys.OTHER_VERTEX
        };

        int total = query.conditions().size();
        if (total == 1) {
            // Supported: 1.query just by edge label, 2.query with scan
            if (query.containsCondition(HugeKeys.LABEL) ||
                query.containsScanCondition()) {
                return;
            }
        }

        int matched = 0;
        for (HugeKeys key : keys) {
            Object value = query.condition(key);
            if (value == null) {
                break;
            }
            matched++;
        }
        if (matched != total) {
            throw new BackendException(
                      "Not supported querying edges by %s, expect %s",
                      query.conditions(), keys[matched]);
        }
    }

    protected Query optimizeQuery(ConditionQuery query) {
        String label = (String) query.condition(HugeKeys.LABEL);

        // Optimize vertex query
        if (label != null && query.resultType() == HugeType.VERTEX) {
            VertexLabel vertexLabel = this.graph().vertexLabel(label);
            if (vertexLabel.idStrategy() == IdStrategy.PRIMARY_KEY) {
                List<String> keys = vertexLabel.primaryKeys();
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
                    Id id = SplicingIdGenerator.splicing(label, primaryValues);
                    query.query(id);
                    query.resetConditions();

                    return query;
                }
            }
        }

        // Optimize edge query
        if (label != null && query.resultType() == HugeType.EDGE) {
            List<String> keys = this.graph().edgeLabel(label).sortKeys();
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
            return query;
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

    private Set<String> relatedIndexNames(String prop,
                                          Indexfiable indexfiable) {
        return indexfiable.indexNames().stream().filter(index -> {
            return graph().indexLabel(index).indexFields().contains(prop);
        }).collect(Collectors.toSet());
    }

    private void lockForUpdateProperty(Set<String> lockNames,
                                       Consumer<LockUtil.Locks> callback) {
        this.checkOwnerThread();

        LockUtil.Locks locks = new LockUtil.Locks();
        try {
            locks.lockReads(LockUtil.INDEX_LABEL, lockNames);

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
        if (cq.test(elem)) {
            return true;
        } else {
            this.indexTx.removeIndexLeft(cq, elem);
            return false;
        }
    }

    private Iterable<?> joinTxVertices(Query query,
                                       Collection<HugeVertex> vertices) {
        if (query.resultType() != HugeType.VERTEX) {
            return vertices;
        }
        assert query.resultType() == HugeType.VERTEX;
        return this.joinTxRecords(query, vertices, (q, v) -> q.test(v),
                                  this.addedVertexes, this.removedVertexes,
                                  this.updatedVertexes);
    }

    private Iterable<?> joinTxEdges(Query query, Collection<HugeEdge> edges) {
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
        List<HugeEdge> results = new ArrayList<>(edges.size());
        Collection<HugeVertex> vertices = this.removedVertexes.values();
        filterloop: for (HugeEdge e : edges) {
            for (HugeVertex v : vertices) {
                if (e.belongToVertex(v)) {
                    continue filterloop;
                }
            }
            results.add(e);
        }
        return results;
    }

    private <V extends HugeElement> Collection<V> joinTxRecords(
                                    Query query,
                                    Collection<V> records,
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

        Set<V> results = this.newSetWithInsertionOrder();

        /* Collect added records
         * Records in memory have higher priority than query from backend store
         */
        for (V i : addedTxRecords.values()) {
            if (match.apply(query, i)) {
                results.add(i);
            }
        }
        for (V i : updatedTxRecords.values()) {
            if (match.apply(query, i)) {
                results.add(i);
            }
        }

        // Filter removed records
        if (removedTxRecords.size() < records.size()) {
            for (V i : removedTxRecords.values()) {
                // NOTE: this will change value of the input parameter
                records.remove(i);
            }
            results.addAll(records);
        } else {
            for (V i : records) {
                if (!removedTxRecords.containsKey(i.id())) {
                    results.add(i);
                }
            }
        }

        return results;
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
        boolean autoCommit = this.autoCommit();
        this.autoCommit(false);
        try {
            ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
            // TODO: use query.capacity(Query.NO_LIMIT);
            query.eq(HugeKeys.LABEL, vertexLabel.name());
            if (vertexLabel.hidden()) {
                query.showHidden(true);
            }
            Iterator<Vertex> vertices = this.queryVertices(query).iterator();

            while (vertices.hasNext()) {
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
        boolean autoCommit = this.autoCommit();
        this.autoCommit(false);
        try {
            if (this.store().features().supportsDeleteEdgeByLabel()) {
                // TODO: Need to change to writeQuery!
                Id id = IdGenerator.of(edgeLabel);
                this.removeEntry(this.serializer.writeId(HugeType.EDGE, id));
            } else {
                ConditionQuery query = new ConditionQuery(HugeType.EDGE);
                // TODO: use query.capacity(Query.NO_LIMIT);
                query.eq(HugeKeys.LABEL, edgeLabel.name());
                if (edgeLabel.hidden()) {
                    query.showHidden(true);
                }
                Iterator<Edge> edges = this.queryEdges(query).iterator();

                while (edges.hasNext()) {
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
