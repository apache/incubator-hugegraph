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

package com.baidu.hugegraph.traversal.algorithm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Aggregate;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.iterator.ExtendableIterator;
import com.baidu.hugegraph.iterator.FilterIterator;
import com.baidu.hugegraph.iterator.LimitIterator;
import com.baidu.hugegraph.iterator.MapperIterator;
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.schema.SchemaLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.traversal.algorithm.steps.EdgeStep;
import com.baidu.hugegraph.traversal.algorithm.steps.Steps;
import com.baidu.hugegraph.traversal.optimize.TraversalUtil;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.CollectionType;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.baidu.hugegraph.util.Log;
import com.baidu.hugegraph.util.collection.CollectionFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;

public class HugeTraverser {

    public static final Logger LOG = Log.logger(HugeTraverser.class);

    private HugeGraph graph;

    private static CollectionFactory collectionFactory;

    public static final String DEFAULT_CAPACITY = "10000000";
    public static final String DEFAULT_ELEMENTS_LIMIT = "10000000";
    public static final String DEFAULT_PATHS_LIMIT = "10";
    public static final String DEFAULT_LIMIT = "100";
    public static final String DEFAULT_MAX_DEGREE = "10000";
    public static final String DEFAULT_SKIP_DEGREE = "100000";
    public static final String DEFAULT_SAMPLE = "100";
    public static final String DEFAULT_MAX_DEPTH = "50";
    public static final String DEFAULT_WEIGHT = "0";

    protected static final int MAX_VERTICES = 10;

    // Empirical value of scan limit, with which results can be returned in 3s
    public static final String DEFAULT_PAGE_LIMIT = "100000";

    public static final long NO_LIMIT = -1L;

    // for debugMeasure
    public long edgeIterCounter = 0;
    public long vertexIterCounter = 0;

    public static final String ALGORITHMS_BREADTH_FIRST = "breadth_first";
    public static final String ALGORITHMS_DEEP_FIRST = "deep_first";

    public HugeTraverser(HugeGraph graph) {
        this.graph = graph;
        if (collectionFactory == null) {
            collectionFactory = new CollectionFactory(this.collectionType());
        }
    }

    public HugeGraph graph() {
        return this.graph;
    }

    protected int concurrentDepth() {
        return this.graph.option(CoreOptions.OLTP_CONCURRENT_DEPTH);
    }

    private CollectionType collectionType() {
        return this.graph.option(CoreOptions.OLTP_COLLECTION_TYPE);
    }

    protected Set<Id> adjacentVertices(Id sourceV, Set<Id> vertices,
                                       Directions dir, Id label,
                                       Set<Id> excluded, long degree,
                                       long limit) {
        if (limit == 0) {
            return ImmutableSet.of();
        }

        Set<Id> neighbors = newIdSet();
        for (Id source : vertices) {
            Iterator<Edge> edges = this.edgesOfVertex(source, dir,
                                                      label, degree, false);
            while (edges.hasNext()) {
                this.edgeIterCounter++;
                HugeEdge e = (HugeEdge) edges.next();
                Id target = e.id().otherVertexId();
                boolean matchExcluded = (excluded != null &&
                                         excluded.contains(target));
                if (matchExcluded || neighbors.contains(target) ||
                    sourceV.equals(target)) {
                    continue;
                }
                neighbors.add(target);
                if (limit != NO_LIMIT && neighbors.size() >= limit) {
                    return neighbors;
                }
            }
        }
        return neighbors;
    }

    protected Iterator<Id> adjacentVertices(Id source, Directions dir,
                                            Id label, long limit) {
        Iterator<Edge> edges = this.edgesOfVertex(source, dir, label, limit, false);
        return new MapperIterator<>(edges, e -> {
            HugeEdge edge = (HugeEdge) e;
            return edge.id().otherVertexId();
        });
    }

    protected Set<Id> adjacentVertices(Id source, EdgeStep step) {
        Set<Id> neighbors = newSet();
        Iterator<Edge> edges = this.edgesOfVertex(source, step, false);
        while (edges.hasNext()) {
            this.edgeIterCounter++;
            neighbors.add(((HugeEdge) edges.next()).id().otherVertexId());
        }
        return neighbors;
    }

    protected int vertexDegree(Id source, EdgeStep step) {
        return Iterators.size(this.edgesOfVertex(source, step, false));
    }

    @Watched
    protected Iterator<Edge> edgesOfVertex(Id source, Directions dir,
                                           Id label, long limit,
                                           boolean withEdgeProperties) {
        Id[] labels = {};
        if (label != null) {
            labels = new Id[]{label};
        }

        Query query = GraphTransaction.constructEdgesQuery(source, dir, labels);
        if (limit != NO_LIMIT) {
            query.limit(limit);
        }
        query.withProperties(withEdgeProperties);
        return this.graph.edges(query);
    }

    @Watched
    protected Iterator<Edge> edgesOfVertexAF(Id source, Directions dir,
                                             Id label, long limit,
                                             Steps steps,
                                             boolean withEdgeProperties) {
        Id[] labels = {};
        if (label != null) {
            labels = new Id[]{label};
        }

        Query query = GraphTransaction.constructEdgesQuery(source, dir, labels);
        if (limit != NO_LIMIT) {
            query.limit(limit);
        }
        // check if we need edgeProperties.
        query.withProperties(withEdgeProperties ||
                             !steps.isEdgeStepPropertiesEmpty());

        Iterator<Edge> result = this.graph.edges(query);
        return edgesOfVertexStep(result, steps);
    }

    protected Iterator<Edge> edgesOfVertexStep(Iterator<Edge> edges,
                                               Steps steps) {
        if (checkParamsNull(edges, steps)) {
            return edges;
        }

        Iterator<Edge> result = edges;
        if (steps.isEdgeStepPropertiesEmpty() && !steps.isVertexEmpty()) {
            // vertex's label and vertex's properties
            Map<Id, ConditionQuery> vConditions =
                    getElementFilterQuery(steps.vertexSteps(), HugeType.VERTEX);

            result = new FilterIterator<>(result, edge -> {
                return validateVertex(vConditions, (HugeEdge) edge);
            });

        } else if (!steps.isEdgeStepPropertiesEmpty() &&
                   steps.isVertexEmpty()) {
            // edge's properties
            Map<Id, ConditionQuery> eConditions =
                    getElementFilterQuery(steps.edgeSteps(), HugeType.EDGE);

            result = new FilterIterator<>(result, edge -> {
                return validateEdge(eConditions, (HugeEdge) edge);
            });
        } else {
            // Edge & Vertex Step are not empty
            Map<Id, ConditionQuery> vConditions =
                    getElementFilterQuery(steps.vertexSteps(), HugeType.VERTEX);
            Map<Id, ConditionQuery> eCondtions =
                    getElementFilterQuery(steps.edgeSteps(), HugeType.EDGE);

            result = new FilterIterator<>(result, edge -> {
                return validateVertex(vConditions, (HugeEdge) edge) ?
                       validateEdge(eCondtions, (HugeEdge) edge) : false;
            });
        }
        return result;
    }

    private Boolean validateVertex(Map<Id, ConditionQuery> vConditions,
                                   HugeEdge edge) {
        HugeVertex sVertex = edge.sourceVertex();
        HugeVertex tVertex = edge.targetVertex();
        if (!vConditions.containsKey(sVertex.schemaLabel().id()) ||
            !vConditions.containsKey(tVertex.schemaLabel().id())) {
            return false;
        }

        ConditionQuery cq = vConditions.get(sVertex.schemaLabel().id());
        if (cq != null) {
            sVertex = (HugeVertex) this.graph.vertex(sVertex.id());
            if (!cq.test(sVertex)) {
                return false;
            }
        }

        cq = vConditions.get(tVertex.schemaLabel().id());
        if (cq != null) {
            tVertex = (HugeVertex) this.graph.vertex(tVertex.id());
            return cq.test(tVertex);
        }
        return true;
    }

    private Boolean validateEdge(Map<Id, ConditionQuery> eConditions,
                                 HugeEdge edge) {
        if (!eConditions.containsKey(edge.schemaLabel().id())) {
            return false;
        }

        ConditionQuery cq = eConditions.get(edge.schemaLabel().id());
        if (cq != null) {
            return cq.test(edge);
        }
        return true;
    }

    private Map<Id, ConditionQuery> getElementFilterQuery(
            Map<Id, Steps.StepEntity> idStepEntityMap, HugeType vertex) {
        Map<Id, ConditionQuery> vertexConditions = new HashMap<>();
        for (Map.Entry<Id, Steps.StepEntity> entry :
                idStepEntityMap.entrySet()) {
            Steps.StepEntity stepEntity = entry.getValue();
            if (stepEntity.getProperties() != null &&
                !stepEntity.getProperties().isEmpty()) {
                ConditionQuery cq = new ConditionQuery(vertex);
                Map<Id, Object> props = stepEntity.getProperties();
                TraversalUtil.fillConditionQuery(cq, props, this.graph);
                vertexConditions.put(entry.getKey(), cq);
            } else {
                vertexConditions.put(entry.getKey(), null);
            }
        }
        return vertexConditions;
    }

    private boolean checkParamsNull(Iterator<Edge> edges, Steps s) {
        if (edges == null || s == null || (!edges.hasNext())) {
            return true;
        }
        // Both are empty
        return (s.edgeSteps() == null || s.isEdgeStepPropertiesEmpty()) &&
               (s.vertexSteps() == null || s.vertexSteps().isEmpty());
    }

    @Watched
    protected Iterator<Edge> edgesOfVertex(Id source, Directions dir,
                                           Map<Id, String> labels, long limit,
                                           boolean withProperties) {
        if (labels == null || labels.isEmpty()) {
            return this.edgesOfVertex(source, dir, (Id) null, limit, withProperties);
        }
        ExtendableIterator<Edge> results = new ExtendableIterator<>();
        for (Id label : labels.keySet()) {
            E.checkNotNull(label, "edge label");
            results.extend(this.edgesOfVertex(source, dir, label, limit, withProperties));
        }

        if (limit == NO_LIMIT) {
            return results;
        }

        long[] count = new long[1];
        return new LimitIterator<>(results, e -> {
            return count[0]++ >= limit;
        });
    }

    @Watched
    protected Iterator<Edge> edgesOfVertexAF(Id source, Directions dir,
                                             Id[] labels, long limit,
                                             Steps steps,
                                             boolean withEdgeProperties) {
        if (labels == null || labels.length == 0) {
            return this.edgesOfVertexAF(source, dir, (Id) null,
                                        limit, steps, withEdgeProperties);
        }
        ExtendableIterator<Edge> results = new ExtendableIterator<>();
        for (Id label : labels) {
            E.checkNotNull(label, "Edge label can't be null");
            results.extend(this.edgesOfVertexAF(source, dir, label,
                                                limit, steps, withEdgeProperties));
        }

        if (limit == NO_LIMIT) {
            return results;
        }

        long[] count = new long[1];
        return new LimitIterator<>(results, e -> {
            return count[0]++ >= limit;
        });
    }

    protected Iterator<Edge> edgesOfVertexAF(Id source, Steps steps,
                                             boolean withEdgeProperties) {

        if (steps.isEdgeStepPropertiesEmpty()) {
            Iterator<Edge> edges = this.edgesOfVertexAF(source,
                                                        steps.direction(),
                                                        steps.edgeLabels(),
                                                        steps.limit(),
                                                        steps,
                                                        withEdgeProperties);
            return steps.skipSuperNodeIfNeeded(edges);
        }

        Id[] edgeLabels = steps.edgeLabels();
        Query query = GraphTransaction.constructEdgesQuery(
                      source, steps.direction(), edgeLabels);
        query.capacity(Query.NO_CAPACITY);
        if (steps.limit() != NO_LIMIT) {
            query.limit(steps.limit());
        }
        Iterator<Edge> edges = this.graph().edges(query);
        edges = edgesOfVertexStep(edges, steps);
        return steps.skipSuperNodeIfNeeded(edges);
    }

    protected Iterator<Edge> edgesOfVertex(Id source, EdgeStep edgeStep,
                                           boolean withProperties) {
        if (edgeStep.properties() == null || edgeStep.properties().isEmpty()) {
            Iterator<Edge> edges = this.edgesOfVertex(source,
                                                      edgeStep.direction(),
                                                      edgeStep.labels(),
                                                      edgeStep.limit(),
                                                      withProperties);
            return edgeStep.skipSuperNodeIfNeeded(edges);
        }

        Id[] edgeLabels = edgeStep.edgeLabels();
        Query query = GraphTransaction.constructEdgesQuery(source,
                                                           edgeStep.direction(),
                                                           edgeLabels);
        ConditionQuery filter = (ConditionQuery) query.copy();
        this.fillFilterByProperties(filter, edgeStep.properties());
        query.capacity(Query.NO_CAPACITY);
        if (edgeStep.limit() != NO_LIMIT) {
            query.limit(edgeStep.limit());
        }
        Iterator<Edge> edges = this.graph().edges(query);
        edges = new FilterIterator<>(edges, (e) -> {
            return filter.test((HugeEdge) e);
        });
        return edgeStep.skipSuperNodeIfNeeded(edges);
    }

    private void fillFilterBySortKeys(Query query, Id[] edgeLabels,
                                      Map<Id, Object> properties) {
        if (properties == null || properties.isEmpty()) {
            return;
        }

        E.checkArgument(edgeLabels.length == 1,
                        "The properties filter condition can be set " +
                        "only if just set one edge label");

        this.fillFilterByProperties(query, properties);

        ConditionQuery condQuery = (ConditionQuery) query;
        if (!GraphTransaction.matchFullEdgeSortKeys(condQuery, this.graph())) {
            Id label = condQuery.condition(HugeKeys.LABEL);
            E.checkArgument(false, "The properties %s does not match " +
                            "sort keys of edge label '%s'",
                            this.graph().mapPkId2Name(properties.keySet()),
                            this.graph().edgeLabel(label).name());
        }
    }

    private void fillFilterByProperties(Query query,
                                        Map<Id, Object> properties) {
        if (properties == null || properties.isEmpty()) {
            return;
        }

        ConditionQuery condQuery = (ConditionQuery) query;
        TraversalUtil.fillConditionQuery(condQuery, properties, this.graph);
    }

    protected long edgesCount(Id source, EdgeStep edgeStep) {
        Id[] edgeLabels = edgeStep.edgeLabels();
        Query query = GraphTransaction.constructEdgesQuery(source,
                                                           edgeStep.direction(),
                                                           edgeLabels);
        this.fillFilterBySortKeys(query, edgeLabels, edgeStep.properties());
        query.aggregate(Aggregate.AggregateFunc.COUNT, null);
        query.capacity(Query.NO_CAPACITY);
        query.limit(Query.NO_LIMIT);
        long count = graph().queryNumber(query).longValue();
        if (edgeStep.degree() == NO_LIMIT || count < edgeStep.degree()) {
            return count;
        } else if (edgeStep.skipDegree() != 0L &&
                   count >= edgeStep.skipDegree()) {
            return 0L;
        } else {
            return edgeStep.degree();
        }
    }

    protected Object getVertexLabelId(Object label) {
        if (label == null) {
            return null;
        }
        return SchemaLabel.getLabelId(this.graph, HugeType.VERTEX, label);
    }

    protected Id getEdgeLabelId(Object label) {
        if (label == null) {
            return null;
        }
        return SchemaLabel.getLabelId(this.graph, HugeType.EDGE, label);
    }

    protected void checkVertexExist(Id vertexId, String name) {
        try {
            this.graph.vertex(vertexId);
        } catch (NotFoundException e) {
            throw new IllegalArgumentException(String.format(
                      "The %s with id '%s' does not exist", name, vertexId),
                      e);
        }
    }

    public static void checkDegree(long degree) {
        checkPositiveOrNoLimit(degree, "max degree");
    }

    public static void checkCapacity(long capacity) {
        checkPositiveOrNoLimit(capacity, "capacity");
    }

    public static void checkLimit(long limit) {
        checkPositiveOrNoLimit(limit, "limit");
    }

    public static void checkPositive(long value, String name) {
        E.checkArgument(value > 0,
                "The %s parameter must be > 0, but got %s",
                name, value);
    }

    public static void checkPositiveOrNoLimit(long value, String name) {
        E.checkArgument(value > 0L || value == NO_LIMIT,
                        "The %s parameter must be > 0 or == %s, but got: %s",
                        name, NO_LIMIT, value);
    }

    public static void checkNonNegative(long value, String name) {
        E.checkArgument(value >= 0L,
                        "The %s parameter must be >= 0, but got: %s",
                        name, value);
    }

    public static void checkNonNegativeOrNoLimit(long value, String name) {
        E.checkArgument(value >= 0L || value == NO_LIMIT,
                        "The %s parameter must be >= 0 or == %s, but got: %s",
                        name, NO_LIMIT, value);
    }

    public static void checkCapacity(long capacity, long access,
                                     String traverse) {
        if (capacity != NO_LIMIT && access > capacity) {
            throw new HugeException("Exceed capacity '%s' while finding %s",
                      capacity, traverse);
        }
    }

    public static void checkSkipDegree(long skipDegree, long degree,
                                       long capacity) {
        E.checkArgument(skipDegree >= 0L &&
                        skipDegree <= Query.DEFAULT_CAPACITY,
                        "The skipped degree must be in [0, %s], but got '%s'",
                        Query.DEFAULT_CAPACITY, skipDegree);
        if (capacity != NO_LIMIT) {
            E.checkArgument(degree != NO_LIMIT && degree < capacity,
                            "The max degree must be < capacity");
            E.checkArgument(skipDegree < capacity,
                            "The skipped degree must be < capacity");
        }
        if (skipDegree > 0L) {
            E.checkArgument(degree != NO_LIMIT && skipDegree >= degree,
                            "The skipped degree must be >= max degree, " +
                            "but got skipped degree '%s' and max degree '%s'",
                            skipDegree, degree);
        }
    }

    public static void checkAlgorithm(String algorithm) {
        E.checkArgument(algorithm.compareToIgnoreCase(ALGORITHMS_BREADTH_FIRST) == 0 ||
                        algorithm.compareToIgnoreCase(ALGORITHMS_DEEP_FIRST) == 0,
                        "The algorithm must be one of '%s' or '%s', but got '%s'",
                        ALGORITHMS_BREADTH_FIRST, ALGORITHMS_DEEP_FIRST, algorithm);
    }

    public static boolean isDeepFirstAlgorithm(String algorithm) {
        return algorithm.compareToIgnoreCase(ALGORITHMS_DEEP_FIRST) == 0;
    }

    public static <K, V extends Comparable<? super V>> Map<K, V> topN(
                                                                 Map<K, V> map,
                                                                 boolean sorted,
                                                                 long limit) {
        if (sorted) {
            map = CollectionUtil.sortByValue(map, false);
        }
        if (limit == NO_LIMIT || map.size() <= limit) {
            return map;
        }
        Map<K, V> results = InsertionOrderUtil.newMap();
        long count = 0L;
        for (Map.Entry<K, V> entry : map.entrySet()) {
            results.put(entry.getKey(), entry.getValue());
            if (++count >= limit) {
                break;
            }
        }
        return results;
    }

    public static Iterator<Edge> skipSuperNodeIfNeeded(Iterator<Edge> edges,
                                                       long degree,
                                                       long skipDegree) {
        if (skipDegree <= 0L) {
            return edges;
        }
        List<Edge> edgeList = newList();
        for (int i = 1; edges.hasNext(); i++) {
            Edge edge = edges.next();
            if (i <= degree) {
                edgeList.add(edge);
            }
            if (i >= skipDegree) {
                return QueryResults.emptyIterator();
            }
        }
        return edgeList.iterator();
    }

    protected static Set<Id> newIdSet() {
        return collectionFactory.newIdSet();
    }

    protected static <V> Set<V> newSet() {
        return newSet(false);
    }

    protected static <V> Set<V> newSet(boolean concurrent) {
        if (concurrent) {
            return ConcurrentHashMap.newKeySet();
        } else {
            return collectionFactory.newSet();
        }
    }

    protected static <V> Set<V> newSet(int initialCapacity) {
        return collectionFactory.newSet(initialCapacity);
    }

    protected static <V> Set<V> newSet(Collection<V> collection) {
        return collectionFactory.newSet(collection);
    }

    protected static <V> List<V> newList() {
        return collectionFactory.newList();
    }

    protected static <V> List<V> newList(int initialCapacity) {
        return collectionFactory.newList(initialCapacity);
    }

    protected static <V> List<V> newList(Collection<V> collection) {
        return collectionFactory.newList(collection);
    }

    protected static <K, V> Map<K, V> newMap() {
        return collectionFactory.newMap();
    }

    protected static <K, V> Map<K, V> newMap(int initialCapacity) {
        return collectionFactory.newMap(initialCapacity);
    }

    protected static <K, V> MultivaluedMap<K, V> newMultivalueMap() {
        return new MultivaluedHashMap<>();
    }

    protected static List<Id> joinPath(Node prev, Node back, boolean ring) {
        // Get self path
        List<Id> path = prev.path();

        // Get reversed other path
        List<Id> backPath = back.path();
        Collections.reverse(backPath);

        if (!ring) {
            // Avoid loop in path
            if (CollectionUtils.containsAny(path, backPath)) {
                return ImmutableList.of();
            }
        }

        // Append other path behind self path
        path.addAll(backPath);
        return path;
    }

    public static class Node {

        private Id id;
        private Node parent;

        public Node(Id id) {
            this(id, null);
        }

        public Node(Id id, Node parent) {
            E.checkArgumentNotNull(id, "Id of Node can't be null");
            this.id = id;
            this.parent = parent;
        }

        public Id id() {
            return this.id;
        }

        public Node parent() {
            return this.parent;
        }

        public List<Id> path() {
            List<Id> ids = newList();
            Node current = this;
            do {
                ids.add(current.id);
                current = current.parent;
            } while (current != null);
            Collections.reverse(ids);
            return ids;
        }

        public List<Id> joinPath(Node back) {
            return HugeTraverser.joinPath(this, back, false);
        }

        public boolean contains(Id id) {
            Node node = this;
            do {
                if (node.id.equals(id)) {
                    return true;
                }
                node = node.parent;
            } while (node != null);
            return false;
        }

        @Override
        public int hashCode() {
            return this.id.hashCode();
        }

        @Override
        public boolean equals(Object object) {
            if (!(object instanceof Node)) {
                return false;
            }
            Node other = (Node) object;
            return Objects.equals(this.id, other.id) &&
                   Objects.equals(this.parent, other.parent);
        }

        @Override
        public String toString() {
            return this.id.toString();
        }
    }

    public static class Path {

        public static final Path EMPTY_PATH = new Path(ImmutableList.of());

        private Id crosspoint;
        private List<Id> vertices;

        public Path(List<Id> vertices) {
            this(null, vertices);
        }

        public Path(Id crosspoint, List<Id> vertices) {
            this.crosspoint = crosspoint;
            this.vertices = vertices;
        }

        public Id crosspoint() {
            return this.crosspoint;
        }

        public void addToLast(Id id) {
            this.vertices.add(id);
        }

        public List<Id> vertices() {
            return this.vertices;
        }

        public void reverse() {
            Collections.reverse(this.vertices);
        }

        public Map<String, Object> toMap(boolean withCrossPoint) {
            if (withCrossPoint) {
                return ImmutableMap.of("crosspoint", this.crosspoint,
                                       "objects", this.vertices);
            } else {
                return ImmutableMap.of("objects", this.vertices);
            }
        }

        public boolean ownedBy(Id source) {
            E.checkNotNull(source, "source");
            Id min = null;
            for (Id id : this.vertices) {
                if (min == null || id.compareTo(min) < 0) {
                    min = id;
                }
            }
            return source.equals(min);
        }

        @Override
        public int hashCode() {
            return this.vertices.hashCode();
        }

        /**
         * Compares the specified object with this path for equality.
         * Returns <tt>true</tt> if and only if both have same vertices list
         * without regard of crosspoint.
         *
         * @param other the object to be compared for equality with this path
         * @return <tt>true</tt> if the specified object is equal to this path
         */
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Path)) {
                return false;
            }
            return this.vertices.equals(((Path) other).vertices);
        }
    }

    public static class PathSet implements Set<Path> {

        private static final long serialVersionUID = -8237531948776524872L;

        private Set<Path> paths = newSet();

        @Override
        public boolean add(Path path) {
            return this.paths.add(path);
        }

        @Override
        public boolean remove(Object o) {
            return this.paths.remove(o);
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return this.paths.containsAll(c);
        }

        @Override
        public boolean addAll(Collection<? extends Path> c) {
            return this.paths.addAll(c);
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            return this.paths.retainAll(c);
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            return this.paths.removeAll(c);
        }

        @Override
        public void clear() {
            this.paths.clear();
        }

        @Override
        public boolean isEmpty() {
            return this.paths.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            return this.paths.contains(o);
        }

        @Override
        public int size() {
            return this.paths.size();
        }

        @Override
        public Iterator<Path> iterator() {
            return this.paths.iterator();
        }

        @Override
        public Object[] toArray() {
            return this.paths.toArray();
        }

        @Override
        public <T> T[] toArray(T[] a) {
            return this.paths.toArray(a);
        }

        public boolean addAll(PathSet paths) {
            return this.paths.addAll(paths.paths);
        }

        public Set<Id> vertices() {
            Set<Id> vertices = newIdSet();
            for (Path path : this.paths) {
                vertices.addAll(path.vertices());
            }
            return vertices;
        }

        public void append(Id current) {
            for (Iterator<Path> iter = this.paths.iterator(); iter.hasNext();) {
                Path path = iter.next();
                if (path.vertices().contains(current)) {
                    iter.remove();
                    continue;
                }
                path.addToLast(current);
            }
        }
    }

    public static class NestedIterator implements Iterator<Edge> {

        private static final int MAX_CACHED_COUNT = 1000;

        // skip de-dup for vertices those exceed this limit
        private static final int MAX_VISITED_COUNT = 1000000;

        private final Iterator<Edge> parentIterator;
        private final HugeTraverser traverser;
        private final Steps steps;

        // visited vertex-ids of all parent-tree,
        // used to exclude visited vertex or other purpose
        private final Set<Id> visited;

        // cache for edges, initial capacity to avoid mem-fragment
        private final ArrayList<HugeEdge> cache = new ArrayList<>(MAX_CACHED_COUNT);
        private int cachePointer = 0;
        private boolean withEdgeProperties;

        // of parent
        private HugeEdge currentEdge = null;
        private Iterator<Edge> currentIterator = null;

        // todo: may add edge-filter and/or vertex-filter ...

        public NestedIterator(HugeTraverser traverser, Iterator<Edge> parent,
                              Steps steps, Set<Id> visited,
                              boolean withEdgeProperties) {
            this.traverser = traverser;
            this.parentIterator = parent;
            this.steps = steps;
            this.visited = visited;
            this.withEdgeProperties = withEdgeProperties;
        }

        @Override
        public boolean hasNext() {
            if (currentIterator == null || !currentIterator.hasNext()) {
                return fetch();
            }
            return true;
        }

        @Override
        public Edge next() {
            return currentIterator.next();
        }

        protected boolean fetch() {
            while (this.currentIterator == null ||
                    !this.currentIterator.hasNext()) {
                if (this.cache.size() == this.cachePointer &&
                        !this.fillCache() ) {
                    return false;
                }
                this.currentEdge = this.cache.get(this.cachePointer);
                this.cachePointer++;
                this.currentIterator = traverser.edgesOfVertexAF(
                        this.currentEdge.id().otherVertexId(),
                        steps, withEdgeProperties);
            }
            return true;
        }

        protected boolean fillCache() {
            this.cache.clear();
            this.cachePointer = 0;
            // fill cache from parent
            while (this.parentIterator.hasNext() &&
                    this.cache.size() < MAX_CACHED_COUNT) {
                HugeEdge e = (HugeEdge) parentIterator.next();
                traverser.edgeIterCounter++;
                Id vid = e.id().otherVertexId();
                if (!visited.contains(vid)) {
                    this.cache.add(e);
                    if (visited.size() < MAX_VISITED_COUNT) {
                        // skip over limit visited vertices.
                        this.visited.add(vid);
                    }
                }
            }
            return this.cache.size() > 0;
        }

        public List<HugeEdge> getPathEdges(List<HugeEdge> edges) {
            if (parentIterator instanceof NestedIterator) {
                NestedIterator parent = (NestedIterator) this.parentIterator;
                parent.getPathEdges(edges);
            }
            edges.add(currentEdge);
            return edges;
        }
    }

    public Iterator<Edge> createNestedIterator(Id sourceV, Steps steps, int depth,
                                               Set<Id> visited,
                                               boolean withEdgeProperties) {
        E.checkArgument(depth > 0,
                        "The depth should large than 0 for nested iterator");

        visited.add(sourceV);

        // build a chained iterator path with length of depth
        Iterator<Edge> it = this.edgesOfVertexAF(sourceV, steps, withEdgeProperties);
        for (int i = 1; i < depth; i++) {
            it = new NestedIterator(this, it, steps, visited, withEdgeProperties);
        }
        return it;
    }

    public static List<HugeEdge> getPathEdges(Iterator<Edge> it,
                                              HugeEdge edge) {
        ArrayList<HugeEdge> edges = new ArrayList<>();
        if (it instanceof NestedIterator) {
            ((NestedIterator) it).getPathEdges(edges);
        }
        edges.add(edge);
        return edges;
    }
}
