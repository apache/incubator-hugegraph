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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.baidu.hugegraph.exception.NotFoundException;
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

    private IndexTransaction indexTx;

    private Set<HugeVertex> addedVertexes;
    private Set<HugeVertex> removedVertexes;

    private Set<HugeEdge> addedEdges;
    private Set<HugeEdge> removedEdges;

    public GraphTransaction(HugeGraph graph, BackendStore store,
                            BackendStore indexStore) {
        super(graph, store);

        this.indexTx = new IndexTransaction(graph, indexStore);
        assert !this.indexTx.autoCommit();
    }

    @Override
    public boolean hasUpdates() {
        boolean empty = (this.addedVertexes.isEmpty() &&
                         this.removedVertexes.isEmpty() &&
                         this.addedEdges.isEmpty() &&
                         this.removedEdges.isEmpty());
        return !empty || super.hasUpdates();
    }

    @Override
    protected void reset() {
        super.reset();

        this.addedVertexes = new LinkedHashSet<>();
        this.removedVertexes = new LinkedHashSet<>();

        this.addedEdges = new LinkedHashSet<>();
        this.removedEdges = new LinkedHashSet<>();
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

    protected void prepareAdditions(Set<HugeVertex> updatedVertexes,
                                    Set<HugeEdge> updatedEdges) {
        // Do vertex update
        for (HugeVertex v : updatedVertexes) {
            assert !v.removed();
            v.committed();
            // Add vertex entry
            this.addEntry(this.serializer.writeVertex(v));
            // Update index of vertex(only include props)
            this.indexTx.updateVertexIndex(v, false);
        }

        // Do edge update
        for (HugeEdge e : updatedEdges) {
            assert !e.removed();
            e.committed();
            // Add edge entry of OUT and IN
            this.addEntry(this.serializer.writeEdge(e));
            this.addEntry(this.serializer.writeEdge(e.switchOwner()));
            // Update index of edge
            this.indexTx.updateEdgeIndex(e, false);
        }

        // Clear updates
        updatedVertexes.clear();
        updatedEdges.clear();
    }

    protected void prepareDeletions(Set<HugeVertex> updatedVertexes,
                                    Set<HugeEdge> updatedEdges) {

        Set<HugeVertex> vertexes = new LinkedHashSet<>();
        vertexes.addAll(updatedVertexes);

        Set<HugeEdge> edges = new LinkedHashSet<>();
        edges.addAll(updatedEdges);

        // Clear updates
        updatedVertexes.clear();
        updatedEdges.clear();

        // In order to remove edges of vertexes, query all edges first
        for (HugeVertex v : vertexes) {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            Collection<HugeEdge> vedges = (Collection) ImmutableList.copyOf(
                                          this.queryEdgesByVertex(v.id()));
            edges.addAll(vedges);
        }

        // Remove vertexes
        for (HugeVertex v : vertexes) {
            /*
             * If the backend stores vertex together with edges, it's edges
             * would be removed after removing vertex. Otherwise, if the
             * backend stores vertex which is separated from edges, it's
             * edges should be removed manually when removing vertex.
             */
            this.removeEntry(this.serializer.writeVertex(v.prepareRemoved()));
            // Calling vertex.prepareAdded() returns a vertex without edges
            this.indexTx.updateVertexIndex(v, true);
        }

        // Remove edges
        for (HugeEdge edge : edges) {
            // Update edge index
            this.indexTx.updateEdgeIndex(edge, true);
            // Remove edge of OUT and IN
            edge = edge.prepareRemoved();
            this.removeEntry(this.serializer.writeEdge(edge));
            this.removeEntry(this.serializer.writeEdge(edge.switchOwner()));
        }
    }

    @Override
    public void commit() throws BackendException {
        try {
            super.commit();
        } catch (Throwable e) {
            this.indexTx.reset();
            throw e;
        }
        this.indexTx.commit();
    }

    @Override
    public void rollback() throws BackendException {
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

        LockUtil.Locks locks = new LockUtil.Locks();
        try {
            locks.lockReads(LockUtil.VERTEX_LABEL, vertex.label());
            locks.lockReads(LockUtil.INDEX_LABEL,
                            vertex.vertexLabel().indexNames());
            this.beforeWrite();
            this.addedVertexes.add(vertex);
            this.afterWrite();
        } finally {
            locks.unlock();
        }
        return vertex;
    }

    @Watched(prefix = "graph")
    public Vertex addVertex(Object... keyValues) {
        HugeElement.ElementKeys elemKeys = HugeElement.classifyKeys(keyValues);

        VertexLabel vertexLabel = this.getVertexLabel(elemKeys.label());
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
        this.addedVertexes.remove(vertex);

        // Get edges in local `addedEdges`, and remove it
        for (Iterator<HugeEdge> itor = this.addedEdges.iterator();
             itor.hasNext();) {
            if (itor.next().belongToVertex(vertex)) {
                itor.remove();
            }
        }

        // Collect the removed vertex
        this.removedVertexes.add(vertex);

        this.afterWrite();
    }

    public Iterable<Vertex> queryVertices(Object... vertexIds) {
        List<Vertex> list = new ArrayList<Vertex>(vertexIds.length);

        for (Object vertexId : vertexIds) {
            Id id = HugeElement.getIdValue(vertexId);
            BackendEntry entry;
            try {
                entry = this.get(HugeType.VERTEX, id);
            } catch (NotFoundException ignored) {
                continue;
            }
            Vertex vertex = this.serializer.readVertex(entry, this.graph());
            assert vertex != null;
            list.add(vertex);
        }

        return list;
    }

    public Iterable<Vertex> queryVertices() {
        Query q = new Query(HugeType.VERTEX);
        return this.queryVertices(q);
    }

    public Iterable<Vertex> queryVertices(Query query) {
        assert Arrays.asList(HugeType.VERTEX, HugeType.EDGE)
                     .contains(query.resultType());
        List<Vertex> list = new ArrayList<Vertex>();

        Iterator<BackendEntry> entries = this.query(query).iterator();
        while (entries.hasNext()) {
            BackendEntry entry = entries.next();
            HugeVertex vertex = this.serializer.readVertex(entry, graph());
            assert vertex != null;
            if (query.showHidden() || !Graph.Hidden.isHidden(vertex.label())) {
                list.add(vertex);
            }
        }
        return list;
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

        LockUtil.Locks locks = new LockUtil.Locks();
        try {
            locks.lockReads(LockUtil.EDGE_LABEL, edge.label());
            locks.lockReads(LockUtil.INDEX_LABEL,
                            edge.edgeLabel().indexNames());
            this.beforeWrite();
            this.addedEdges.add(edge);
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
        this.addedEdges.remove(edge);

        // Collect the removed edge
        this.removedEdges.add(edge);

        this.afterWrite();
    }

    public Iterable<Edge> queryEdges(Object... edgeIds) {
        List<Edge> list = new ArrayList<Edge>(edgeIds.length);

        for (Object edgeId : edgeIds) {
            Id id = HugeElement.getIdValue(edgeId);
            BackendEntry entry;
            try {
                entry = this.get(HugeType.EDGE, id);
            } catch (NotFoundException ignored) {
                continue;
            }
            HugeVertex vertex = this.serializer.readVertex(entry, graph());
            assert vertex != null;
            list.addAll(vertex.getEdges());
        }

        return list;
    }

    public Iterable<Edge> queryEdges() {
        Query q = new Query(HugeType.EDGE);
        return this.queryEdges(q);
    }

    public Iterable<Edge> queryEdges(Query query) {
        assert query.resultType() == HugeType.EDGE;
        Iterator<Vertex> vertices = this.queryVertices(query).iterator();

        Map<Id, Edge> results = new HashMap<>();
        while (vertices.hasNext()) {
            HugeVertex vertex = (HugeVertex) vertices.next();
            for (HugeEdge edge : vertex.getEdges()) {
                if (!query.showHidden() &&
                    Graph.Hidden.isHidden(edge.label())) {
                    continue;
                }
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

        return results.values();
    }

    public Iterable<Edge> queryEdgesByVertex(Id id) {
        return queryEdges(constructEdgesQuery(id, null));
    }

    public <V> void addVertexProperty(HugeVertexProperty<V> prop) {
        // NOTE: this method can also be used to update property

        HugeVertex vertex = prop.element();
        E.checkState(vertex != null,
                     "No owner for updating property '%s'", prop.key());
        E.checkArgument(!vertex.removed(),
                        "Can't update property for removed vertex '%s'",
                        vertex);
        // Check is updating primary key
        List<String> primaryKeys = vertex.vertexLabel().primaryKeys();
        E.checkArgument(!primaryKeys.contains(prop.key()),
                        "Can't update primary key: '%s'", prop.key());
        // Add property in memory for new created vertex
        if (vertex.fresh()) {
            // The owner will do property update
            vertex.setProperty(prop);
            return;
        }

        Set<String> lockNames = relatedIndexNames(prop.name(),
                                                  vertex.vertexLabel());
        this.lockForUpdateProperty(lockNames, (locks) -> {
            // Update old vertex to remove index (without new property)
            this.indexTx.updateVertexIndex(vertex, true);

            // Update index of current vertex (with new property)
            vertex.setProperty(prop);
            this.indexTx.updateVertexIndex(vertex, false);

            // Update vertex (append only with new property)
            HugeVertex v = vertex.prepareRemovedChildren();
            v.setProperty(prop);
            this.appendEntry(this.serializer.writeVertex(v));
        });
    }

    public <V> void removeVertexProperty(HugeVertexProperty<V> prop) {
        HugeVertex vertex = prop.element();
        E.checkState(vertex != null,
                     "No owner for removing property '%s'", prop.key());
        // Maybe have ever been removed
        if (vertex.removed() || !vertex.hasProperty(prop.key())) {
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

        Set<String> lockNames = relatedIndexNames(prop.name(),
                                                  vertex.vertexLabel());
        this.lockForUpdateProperty(lockNames, (locks) -> {
            // Update old vertex to remove index (with the property)
            this.indexTx.updateVertexIndex(vertex, true);

            // Update index of current vertex (without the property)
            vertex.removeProperty(prop.key());
            this.indexTx.updateVertexIndex(vertex, false);

            // Update vertex (eliminate only with the property)
            HugeVertex v = vertex.prepareRemovedChildren();
            v.setProperty(prop);
            this.eliminateEntry(this.serializer.writeVertex(v));
        });
    }

    public <V> void addEdgeProperty(HugeEdgeProperty<V> prop) {
        // NOTE: this method can also be used to update property

        HugeEdge edge = prop.element();
        E.checkState(edge != null,
                     "No owner for updating property '%s'", prop.key());
        E.checkArgument(!edge.removed(),
                        "Can't update property for removed edge '%s'",
                        edge);
        // Check is updating sort key
        E.checkArgument(!edge.edgeLabel().sortKeys().contains(prop.key()),
                        "Can't update sort key '%s'", prop.key());
        // Add property in memory for new created edge
        if (edge.fresh()) {
            // The owner will do property update
            edge.setProperty(prop);
            return;
        }

        Set<String> lockNames = relatedIndexNames(prop.name(),
                                                  edge.edgeLabel());
        this.lockForUpdateProperty(lockNames, (locks) -> {
            // Update old edge to remove index (without new property)
            this.indexTx.updateEdgeIndex(edge, true);

            // Update index of current edge (with new property)
            edge.setProperty(prop);
            this.indexTx.updateEdgeIndex(edge, false);

            // Update edge of OUT and IN (append only with new property)
            HugeEdge e = edge.prepareRemovedChildren();
            e.setProperty(prop);
            this.appendEntry(this.serializer.writeEdge(e));
            this.appendEntry(this.serializer.writeEdge(e.switchOwner()));
        });
    }

    public <V> void removeEdgeProperty(HugeEdgeProperty<V> prop) {
        HugeEdge edge = prop.element();
        E.checkState(edge != null,
                     "No owner for removing property '%s'", prop.key());
        // Maybe have ever been removed
        if (edge.removed() || !edge.hasProperty(prop.key())) {
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

        Set<String> lockNames = relatedIndexNames(prop.name(),
                                                  edge.edgeLabel());
        this.lockForUpdateProperty(lockNames, (locks) -> {
            // Update old edge to remove index (with the property)
            this.indexTx.updateEdgeIndex(edge, true);

            // Update index of current edge (without the property)
            edge.removeProperty(prop.key());
            this.indexTx.updateEdgeIndex(edge, false);

            // Update edge of OUT and IN (eliminate only with the property)
            HugeEdge e = edge.prepareRemovedChildren();
            e.setProperty(prop);
            this.eliminateEntry(this.serializer.writeEdge(e));
            this.eliminateEntry(this.serializer.writeEdge(e.switchOwner()));
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

    private VertexLabel getVertexLabel(Object label) {
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
