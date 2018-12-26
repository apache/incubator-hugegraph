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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

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
import com.baidu.hugegraph.iterator.ExtendableIterator;
import com.baidu.hugegraph.iterator.FilterIterator;
import com.baidu.hugegraph.iterator.FlatMapperIterator;
import com.baidu.hugegraph.iterator.MapperIterator;
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaLabel;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeEdgeProperty;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeFeatures.HugeVertexFeatures;
import com.baidu.hugegraph.structure.HugeProperty;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.structure.HugeVertexProperty;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.type.define.IdStrategy;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.baidu.hugegraph.util.LockUtil;
import com.google.common.collect.ImmutableList;

public class GraphTransaction extends IndexableTransaction {

    public static final int COMMIT_BATCH = 500;

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

    private LockUtil.LocksTable locksTable;

    private final boolean checkVertexExist;

    private final int vertexesCapacity;
    private final int edgesCapacity;

    public GraphTransaction(HugeGraph graph, BackendStore store) {
        super(graph, store);

        this.indexTx = new GraphIndexTransaction(graph, store);
        assert !this.indexTx.autoCommit();

        final HugeConfig conf = graph.configuration();
        this.checkVertexExist = conf.get(
                                CoreOptions.VERTEX_CHECK_CUSTOMIZED_ID_EXIST);
        this.vertexesCapacity = conf.get(CoreOptions.VERTEX_TX_CAPACITY);
        this.edgesCapacity = conf.get(CoreOptions.EDGE_TX_CAPACITY);
        this.locksTable = new LockUtil.LocksTable(graph.name());
    }

    @Override
    public boolean hasUpdates() {
        return this.mutationSize() > 0 || super.hasUpdates();
    }

    @Override
    public int mutationSize() {
        return this.verticesInTxSize() + this.edgesInTxSize();
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

    protected final int verticesInTxSize() {
        return this.addedVertexes.size() +
               this.removedVertexes.size() +
               this.updatedVertexes.size();
    }

    protected final int edgesInTxSize() {
        return this.addedEdges.size() +
               this.removedEdges.size() +
               this.updatedEdges.size();
    }

    protected final Collection<HugeVertex> verticesInTxUpdated() {
        int size = this.addedVertexes.size() + this.updatedVertexes.size();
        List<HugeVertex> vertices = new ArrayList<>(size);
        vertices.addAll(this.addedVertexes.values());
        vertices.addAll(this.updatedVertexes.values());
        return vertices;
    }

    protected final Collection<HugeVertex> verticesInTxRemoved() {
        return new ArrayList<>(this.removedVertexes.values());
    }

    protected final boolean removingEdgeOwner(HugeEdge edge) {
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
        if (this.checkVertexExist) {
            this.checkVertexExistIfCustomizedId(addedVertexes);
        }
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
                // NOTE: will change the input parameter
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
    public void commit() throws BackendException {
        try {
            super.commit();
        } finally {
            this.locksTable.unlock();
        }
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
            this.locksTable.unlock();
        }
    }

    @Override
    public Iterator<BackendEntry> query(Query query) {
        if (!(query instanceof ConditionQuery)) {
            return super.query(query);
        }

        List<Query> queries = new ArrayList<>();
        for (ConditionQuery cq: ConditionQueryFlatten.flatten(
                                (ConditionQuery) query)) {
            Query q = this.optimizeQuery(cq);
            /*
             * NOTE: There are two possibilities for this query:
             * 1.sysprop-query, which would not be empty.
             * 2.index-query result(ids after optimization), which may be empty.
             */
            if (!q.empty()) {
                // Return empty if there is no result after index-query
                queries.add(q);
            }
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
        try {
            this.locksTable.lockReads(LockUtil.VERTEX_LABEL_DELETE,
                                      vertex.schemaLabel().id());
            this.locksTable.lockReads(LockUtil.INDEX_LABEL_DELETE,
                                      vertex.schemaLabel().indexLabels());
            // Ensure vertex label still exists from vertex-construct to lock
            this.graph().vertexLabel(vertex.schemaLabel().id());
            /*
             * No need to lock VERTEX_LABEL_ADD_UPDATE, because vertex label
             * update only can add nullable properties and user data, which is
             * unconcerned with add vertex
             */
            this.beforeWrite();
            this.addedVertexes.put(vertex.id(), vertex);
            this.afterWrite();
        } catch (Throwable e){
            this.locksTable.unlock();
            throw e;
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
        if (this.graph().mode().maintaining() &&
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
            return Collections.emptyIterator();
        }

        List<Id> vertexIds = new ArrayList<>();
        while (edges.hasNext()) {
            HugeEdge edge = (HugeEdge) edges.next();
            vertexIds.add(edge.otherVertex().id());
        }

        return this.queryVertices(vertexIds.toArray());
    }

    public Iterator<Vertex> queryVertices(Object... vertexIds) {
        // NOTE: allowed duplicated vertices if query by duplicated ids
        List<Id> ids = InsertionOrderUtil.newList();
        Map<Id, Vertex> vertices = InsertionOrderUtil.newMap();

        IdQuery query = new IdQuery(HugeType.VERTEX);
        for (Object vertexId : vertexIds) {
            HugeVertex vertex;
            Id id = HugeVertex.getIdValue(vertexId);
            if (id == null || this.removedVertexes.containsKey(id)) {
                // The record has been deleted
                continue;
            } else if ((vertex = this.addedVertexes.get(id)) != null ||
                       (vertex = this.updatedVertexes.get(id)) != null) {
                // Found from local tx
                vertices.put(vertex.id(), vertex);
            } else {
                // Prepare query from backend store
                query.query(id);
            }
            ids.add(id);
        }

        if (!query.empty()) {
            // Query from backend store
            Iterator<HugeVertex> it = this.queryVerticesFromBackend(query);
            while (it.hasNext()) {
                HugeVertex vertex = it.next();
                vertices.put(vertex.id(), vertex);
            }
        }

        return new MapperIterator<>(ids.iterator(), id -> {
            return vertices.get(id);
        });
    }

    public Iterator<Vertex> queryVertices() {
        Query q = new Query(HugeType.VERTEX);
        return this.queryVertices(q);
    }

    public Iterator<Vertex> queryVertices(Query query) {
        Iterator<HugeVertex> results = this.queryVerticesFromBackend(query);

        // Filter unused or incorrect records
        results = new FilterIterator<>(results, vertex -> {
            assert vertex.schemaLabel() != VertexLabel.NONE;
            // Filter hidden results
            if (!query.showHidden() && Graph.Hidden.isHidden(vertex.label())) {
                return false;
            }
            // Process results that query from left index or primary-key
            if (query.resultType().isVertex() &&
                !filterResultFromIndexQuery(query, vertex)) {
                // Only index query will come here
                return false;
            }
            return true;
        });

        @SuppressWarnings("unchecked")
        Iterator<Vertex> r = (Iterator<Vertex>) joinTxVertices(query, results);
        return r;
    }

    protected Iterator<HugeVertex> queryVerticesFromBackend(Query query) {
        assert query.resultType().isVertex();

        Iterator<BackendEntry> entries = this.query(query);

        return new MapperIterator<>(entries, entry -> {
            HugeVertex vertex = this.serializer.readVertex(graph(), entry);
            assert vertex != null;
            return vertex;
        });
    }

    @Watched(prefix = "graph")
    public HugeEdge addEdge(HugeEdge edge) {
        this.checkOwnerThread();

        // Override edges in local `removedEdges`
        this.removedEdges.remove(edge.id());
        try {
            this.locksTable.lockReads(LockUtil.EDGE_LABEL_DELETE,
                                      edge.schemaLabel().id());
            this.locksTable.lockReads(LockUtil.INDEX_LABEL_DELETE,
                                      edge.schemaLabel().indexLabels());
            // Ensure edge label still exists from edge-construct to lock
            this.graph().edgeLabel(edge.schemaLabel().id());
            /*
             * No need to lock EDGE_LABEL_ADD_UPDATE, because edge label
             * update only can add nullable properties and user data, which is
             * unconcerned with add edge
             */
            this.beforeWrite();
            this.addedEdges.put(edge.id(), edge);
            this.afterWrite();
        } catch (Throwable e) {
            this.locksTable.unlock();
            throw e;
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
        // NOTE: allowed duplicated edges if query by duplicated ids
        List<Id> ids = InsertionOrderUtil.newList();
        Map<Id, Edge> edges = InsertionOrderUtil.newMap();

        IdQuery query = new IdQuery(HugeType.EDGE);
        for (Object edgeId : edgeIds) {
            HugeEdge edge;
            Id id = HugeEdge.getIdValue(edgeId);
            if (id == null || this.removedEdges.containsKey(id)) {
                // The record has been deleted
                continue;
            } else if ((edge = this.addedEdges.get(id)) != null ||
                       (edge = this.updatedEdges.get(id)) != null) {
                // Found from local tx
                edges.put(edge.id(), edge);
            } else {
                // Prepare query from backend store
                query.query(id);
            }
            ids.add(id);
        }

        if (!query.empty()) {
            // Query from backend store
            Iterator<HugeEdge> it = this.queryEdgesFromBackend(query);
            while (it.hasNext()) {
                HugeEdge edge = it.next();
                edges.put(edge.id(), edge);
            }
        }

        return new MapperIterator<>(ids.iterator(), id -> {
            return edges.get(id);
        });
    }

    public Iterator<Edge> queryEdges() {
        Query q = new Query(HugeType.EDGE);
        return this.queryEdges(q);
    }

    public Iterator<Edge> queryEdges(Query query) {
        Iterator<HugeEdge> results = this.queryEdgesFromBackend(query);

        Set<Id> returnedEdges = new HashSet<>();
        results = new FilterIterator<>(results, edge -> {
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
                LOG.debug("Result contains duplicated edge: {}", edge);
                return false;
            }
        });

        @SuppressWarnings("unchecked")
        Iterator<Edge> r = (Iterator<Edge>) joinTxEdges(query, results,
                                                        this.removedVertexes);
        return r;
    }

    protected Iterator<HugeEdge> queryEdgesFromBackend(Query query) {
        assert query.resultType().isEdge();

        Iterator<BackendEntry> entries = this.query(query);

        return new FlatMapperIterator<>(entries, entry -> {
            // Edges are in a vertex
            HugeVertex vertex = this.serializer.readVertex(graph(), entry);
            assert vertex != null;
            if (query.ids().size() == 1) {
                assert vertex.getEdges().size() == 1;
            }
            // Copy to avoid ConcurrentModificationException when removing edge
            return ImmutableList.copyOf(vertex.getEdges()).iterator();
        });
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
        this.lockForUpdateProperty(vertex.schemaLabel(), prop, () -> {
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
        this.lockForUpdateProperty(vertex.schemaLabel(), prop, () -> {
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
        this.lockForUpdateProperty(edge.schemaLabel(), prop, () -> {
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
        this.lockForUpdateProperty(edge.schemaLabel(), prop, () -> {
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
                if (this.graph().mode().maintaining()) {
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

    private void checkVertexExistIfCustomizedId(Map<Id, HugeVertex> vertices) {
        Set<Id> ids = new HashSet<>();
        for (HugeVertex vertex : vertices.values()) {
            VertexLabel vl = vertex.schemaLabel();
            if (!vl.hidden() && vl.idStrategy().isCustomized()) {
                ids.add(vertex.id());
            }
        }
        if (ids.isEmpty()) {
            return;
        }
        IdQuery idQuery = new IdQuery(HugeType.VERTEX, ids);
        Iterator<HugeVertex> results = this.queryVerticesFromBackend(idQuery);
        if (results.hasNext()) {
            HugeVertex existedVertex = results.next();
            HugeVertex newVertex = vertices.get(existedVertex.id());
            if (!existedVertex.label().equals(newVertex.label())) {
                throw new HugeException(
                          "The newly added vertex with id:'%s' label:'%s' " +
                          "is not allowed to insert, because already exist " +
                          "a vertex with same id and different label:'%s'",
                          newVertex.id(), newVertex.label(),
                          existedVertex.label());
            }
        }
    }

    private void lockForUpdateProperty(SchemaLabel schemaLabel,
                                       HugeProperty<?> prop,
                                       Runnable callback) {
        this.checkOwnerThread();

        Id pkey = prop.propertyKey().id();
        Set<Id> indexIds = new HashSet<>();
        for (Id il : schemaLabel.indexLabels()) {
            if (graph().indexLabel(il).indexFields().contains(pkey)) {
                indexIds.add(il);
            }
        }
        String group = schemaLabel.type() == HugeType.VERTEX_LABEL ?
                       LockUtil.VERTEX_LABEL_DELETE :
                       LockUtil.EDGE_LABEL_DELETE;
        try {
            this.locksTable.lockReads(group, schemaLabel.id());
            this.locksTable.lockReads(LockUtil.INDEX_LABEL_DELETE, indexIds);
            // Ensure schema label still exists
            if (schemaLabel.type() == HugeType.VERTEX_LABEL) {
                this.graph().vertexLabel(schemaLabel.id());
            } else {
                assert schemaLabel.type() == HugeType.EDGE_LABEL;
                this.graph().edgeLabel(schemaLabel.id());
            }
            /*
             * No need to lock INDEX_LABEL_ADD_UPDATE, because index label
             * update only can add  user data, which is unconcerned with
             * update property
             */
            this.beforeWrite();
            callback.run();
            this.afterWrite();
        } catch (Throwable e) {
            this.locksTable.unlock();
            throw e;
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
        this.checkOwnerThread();
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
        if (this.verticesInTxSize() >= this.vertexesCapacity) {
            throw new LimitExceedException(
                      "Vertices size has reached tx capacity %d",
                      this.vertexesCapacity);
        }
    }

    private void checkTxEdgesCapacity() throws LimitExceedException {
        if (this.edgesInTxSize() >= this.edgesCapacity) {
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

    public void updateIndex(Id ilId, HugeElement element) {
        // TODO: use event to replace direct call
        this.checkOwnerThread();

        this.beforeWrite();
        this.indexTx.updateIndex(ilId, element, false);
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
            this.traverseVerticesByLabel(vertexLabel, vertex -> {
                this.removeVertex((HugeVertex) vertex);
                this.commitIfGtSize(COMMIT_BATCH);
            });
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
                this.traverseEdgesByLabel(edgeLabel, edge -> {
                    this.removeEdge((HugeEdge) edge);
                    this.commitIfGtSize(COMMIT_BATCH);
                });
            }
            this.commit();
        } catch (Exception e) {
            LOG.error("Failed to remove edges", e);
            throw new HugeException("Failed to remove edges", e);
        } finally {
            this.autoCommit(autoCommit);
        }
    }

    public void traverseVerticesByLabel(VertexLabel label,
                                        Consumer<Vertex> consumer) {
        this.traverseByLabel(label, this::queryVertices, consumer);
    }

    public void traverseEdgesByLabel(EdgeLabel label,
                                     Consumer<Edge> consumer) {
        this.traverseByLabel(label, this::queryEdges, consumer);
    }

    private <T> void traverseByLabel(SchemaLabel label,
                                     Function<Query, Iterator<T>> fetcher,
                                     Consumer<T> consumer) {
        HugeType type = label.type() == HugeType.VERTEX_LABEL ?
                        HugeType.VERTEX : HugeType.EDGE;
        ConditionQuery query = new ConditionQuery(type);
        // Whether query system vertices
        if (label.hidden()) {
            query.showHidden(true);
        }

        // Not support label index, query all and filter by label
        if (!label.enableLabelIndex()) {
            query.capacity(Query.NO_CAPACITY);
            Iterator<T> itor = fetcher.apply(query);
            while (itor.hasNext()) {
                T e = itor.next();
                SchemaLabel elemLabel = ((HugeElement) e).schemaLabel();
                if (label.equals(elemLabel)) {
                    consumer.accept(e);
                }
            }
            return;
        }

        /*
         * Support label index, query by label. Set limit&capacity to
         * Query.DEFAULT_CAPACITY to limit elements number per pass
         */
        query.limit(Query.DEFAULT_CAPACITY);
        query.capacity(Query.NO_CAPACITY);
        query.eq(HugeKeys.LABEL, label.id());
        int pass = 0;
        int counter = 0;
        do {
            query.offset(pass++ * Query.DEFAULT_CAPACITY);
            // Process every element in current batch
            Iterator<T> itor = fetcher.apply(query);
            for (counter = 0; itor.hasNext(); ++counter) {
                consumer.accept(itor.next());
            }
            assert counter <= Query.DEFAULT_CAPACITY;
        } while (counter == Query.DEFAULT_CAPACITY); // If not, means finish
    }
}
