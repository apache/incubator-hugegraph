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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Edge;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class SingleSourceShortestPathTraverser extends HugeTraverser {

    public SingleSourceShortestPathTraverser(HugeGraph graph) {
        super(graph);
    }

    public WeightedPaths singleSourceShortestPaths(Id sourceV, Directions dir,
                                                   String label, String weight,
                                                   long degree, long skipDegree,
                                                   long capacity, long limit) {
        E.checkNotNull(sourceV, "source vertex id");
        this.checkVertexExist(sourceV);
        E.checkNotNull(dir, "direction");
        checkDegree(degree);
        checkCapacity(capacity);
        checkSkipDegree(skipDegree, degree, capacity);
        checkLimit(limit);

        Id labelId = this.getEdgeLabelId(label);
        Traverser traverser = new Traverser(sourceV, dir, labelId, weight,
                                            degree, skipDegree, capacity,
                                            limit);
        while (true) {
            // Found, reach max depth or reach capacity, stop searching
            traverser.forward();
            if (traverser.done()) {
                return traverser.shortestPaths();
            }
            checkCapacity(traverser.capacity, traverser.size, "shortest path");
        }
    }

    public NodeWithWeight weightedShortestPath(Id sourceV, Id targetV,
                                               Directions dir, String label,
                                               String weight, long degree,
                                               long skipDegree, long capacity) {
        E.checkNotNull(sourceV, "source vertex id");
        this.checkVertexExist(sourceV);
        E.checkNotNull(dir, "direction");
        E.checkNotNull(weight, "weight property");
        checkDegree(degree);
        checkCapacity(capacity);
        checkSkipDegree(skipDegree, degree, capacity);

        Id labelId = this.getEdgeLabelId(label);
        Traverser traverser = new Traverser(sourceV, dir, labelId, weight,
                                            degree, skipDegree, capacity,
                                            NO_LIMIT);
        while (true) {
            traverser.forward();
            Map<Id, NodeWithWeight> results = traverser.shortestPaths();
            if (results.containsKey(targetV) || traverser.done()) {
                return results.get(targetV);
            }
            checkCapacity(traverser.capacity, traverser.size, "shortest path");
        }
    }

    private class Traverser {

        private WeightedPaths findingNodes = new WeightedPaths();
        private WeightedPaths foundNodes = new WeightedPaths();
        private Set<NodeWithWeight> sources;
        private Id source;
        private final Directions direction;
        private final Id label;
        private final String weight;
        private final long degree;
        private final long skipDegree;
        private final long capacity;
        private final long limit;
        private long size;
        private boolean done = false;

        public Traverser(Id sourceV, Directions dir, Id label, String weight,
                         long degree, long skipDegree, long capacity,
                         long limit) {
            this.source = sourceV;
            this.sources = ImmutableSet.of(new NodeWithWeight(
                           0D, new Node(sourceV, null)));
            this.direction = dir;
            this.label = label;
            this.weight = weight;
            this.degree = degree;
            this.skipDegree = skipDegree;
            this.capacity = capacity;
            this.limit = limit;
            this.size = 0L;
        }

        /**
         * Search forward from source
         */
        public void forward() {
            long degree = this.skipDegree > 0L ? this.skipDegree : this.degree;
            for (NodeWithWeight node : this.sources) {
                Iterator<Edge> edges = edgesOfVertex(node.node().id(),
                                                     this.direction,
                                                     this.label, degree);
                edges = this.skipSuperNodeIfNeeded(edges);
                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    if (this.foundNodes.containsKey(target) ||
                        this.source.equals(target)) {
                        // Already find shortest path for target, skip
                        continue;
                    }

                    double currentWeight = this.edgeWeight(edge);
                    double weight = currentWeight + node.weight();
                    NodeWithWeight nw = new NodeWithWeight(weight, target, node);
                    NodeWithWeight exist = this.findingNodes.get(target);
                    if (exist == null || weight < exist.weight()) {
                        /*
                         * There are 2 scenarios to update finding nodes:
                         * 1. The 'target' found first time, add current path
                         * 2. Already exist path for 'target' and current
                         *    path is shorter, update path for 'target'
                         */
                        this.findingNodes.put(target, nw);
                    }
                }
            }

            Map<Id, NodeWithWeight> sorted = CollectionUtil.sortByValue(
                                             this.findingNodes, true);
            double minWeight = 0;
            Set<NodeWithWeight> newSources = new HashSet<>();
            for (Map.Entry<Id, NodeWithWeight> entry : sorted.entrySet()) {
                Id id = entry.getKey();
                NodeWithWeight wn = entry.getValue();
                if (minWeight == 0) {
                    minWeight = wn.weight();
                } else if (wn.weight() > minWeight) {
                    break;
                }
                // Move shortest paths from 'findingNodes' to 'foundNodes'
                this.foundNodes.put(id, wn);
                if (this.limit != NO_LIMIT &&
                    this.foundNodes.size() >= this.limit) {
                    this.done = true;
                    return;
                }
                this.findingNodes.remove(id);
                // Re-init 'sources'
                newSources.add(wn);
            }
            this.sources = newSources;
            if (this.sources.isEmpty()) {
                this.done = true;
            }
        }

        public boolean done() {
            return this.done;
        }

        public WeightedPaths shortestPaths() {
            return this.foundNodes;
        }

        private double edgeWeight(HugeEdge edge) {
            double edgeWeight;
            if (this.weight == null ||
                !edge.property(this.weight).isPresent()) {
                edgeWeight = 1.0;
            } else {
                edgeWeight = edge.value(this.weight);
            }
            return edgeWeight;
        }

        private Iterator<Edge> skipSuperNodeIfNeeded(Iterator<Edge> edges) {
            if (this.skipDegree <= 0L) {
                return edges;
            }
            List<Edge> edgeList = new ArrayList<>();
            int count = 0;
            while (edges.hasNext()) {
                if (count < this.degree) {
                    edgeList.add(edges.next());
                }
                if (count >= this.skipDegree) {
                    return QueryResults.emptyIterator();
                }
                count++;
            }
            return edgeList.iterator();
        }
    }

    public static class NodeWithWeight implements Comparable<NodeWithWeight> {

        private final double weight;
        private final Node node;

        public NodeWithWeight(double weight, Node node) {
            this.weight = weight;
            this.node = node;
        }

        public NodeWithWeight(double weight, Id id, NodeWithWeight prio) {
            this(weight, new Node(id, prio.node()));
        }

        public double weight() {
            return weight;
        }

        public Node node() {
            return this.node;
        }

        public Map<String, Object> toMap() {
            return ImmutableMap.of("weight", this.weight,
                                   "vertices", this.node().path());
        }

        @Override
        public int compareTo(NodeWithWeight other) {
            return Double.compare(this.weight, other.weight);
        }
    }

    public static class WeightedPaths extends HashMap<Id, NodeWithWeight> {

        private static final long serialVersionUID = -313873642177730993L;

        public Set<Id> vertices() {
            Set<Id> vertices = new HashSet<>();
            vertices.addAll(this.keySet());
            for (NodeWithWeight nw : this.values()) {
                vertices.addAll(nw.node().path());
            }
            return vertices;
        }

        public Map<Id, Map<String, Object>> toMap() {
            Map<Id, Map<String, Object>> results = new HashMap<>();
            for (Map.Entry<Id, NodeWithWeight> entry : this.entrySet()) {
                Id source = entry.getKey();
                NodeWithWeight nw = entry.getValue();
                Map<String, Object> result = nw.toMap();
                results.put(source, result);
            }
            return results;
        }
    }
}
