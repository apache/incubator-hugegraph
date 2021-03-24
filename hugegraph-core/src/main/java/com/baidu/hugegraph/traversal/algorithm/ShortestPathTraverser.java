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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.traversal.algorithm.steps.EdgeStep;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableList;

public class ShortestPathTraverser extends HugeTraverser {

    public ShortestPathTraverser(HugeGraph graph) {
        super(graph);
    }

    public Path shortestPath(Id sourceV, Id targetV, Directions dir,
                             List<String> labels, int depth, long degree,
                             long skipDegree, long capacity) {
        E.checkNotNull(sourceV, "source vertex id");
        E.checkNotNull(targetV, "target vertex id");
        this.checkVertexExist(sourceV, "source vertex");
        this.checkVertexExist(targetV, "target vertex");
        E.checkNotNull(dir, "direction");
        checkPositive(depth, "max depth");
        checkDegree(degree);
        checkCapacity(capacity);
        checkSkipDegree(skipDegree, degree, capacity);

        if (sourceV.equals(targetV)) {
            return new Path(ImmutableList.of(sourceV));
        }

        Map<Id, String> labelMap = newMap(labels.size());
        for (String label : labels) {
            labelMap.put(this.getEdgeLabelId(label), label);
        }
        Traverser traverser = new Traverser(sourceV, targetV, dir, labelMap,
                                            degree, skipDegree, capacity);
        PathSet paths;
        while (true) {
            // Found, reach max depth or reach capacity, stop searching
            if (!(paths = traverser.forward(false)).isEmpty() ||
                --depth <= 0) {
                break;
            }
            checkCapacity(traverser.capacity, traverser.size, "shortest path");

            if (!(paths = traverser.backward(false)).isEmpty() ||
                --depth <= 0) {
                if (!paths.isEmpty()) {
                    Path path = paths.iterator().next();
                    Collections.reverse(path.vertices());
                }
                break;
            }
            checkCapacity(traverser.capacity, traverser.size, "shortest path");
        }
        return paths.isEmpty() ? Path.EMPTY_PATH : paths.iterator().next();
    }

    public Path shortestPath(Id sourceV, Id targetV, EdgeStep step,
                             int depth, long capacity) {
        return this.shortestPath(sourceV, targetV, step.direction(),
                                 newList(step.labels().values()),
                                 depth, step.degree(), step.skipDegree(),
                                 capacity);
    }

    public PathSet allShortestPaths(Id sourceV, Id targetV, Directions dir,
                                    List<String> labels, int depth, long degree,
                                    long skipDegree, long capacity) {
        E.checkNotNull(sourceV, "source vertex id");
        E.checkNotNull(targetV, "target vertex id");
        this.checkVertexExist(sourceV, "source vertex");
        this.checkVertexExist(targetV, "target vertex");
        E.checkNotNull(dir, "direction");
        checkPositive(depth, "max depth");
        checkDegree(degree);
        checkCapacity(capacity);
        checkSkipDegree(skipDegree, degree, capacity);

        PathSet paths = new PathSet();
        if (sourceV.equals(targetV)) {
            paths.add(new Path(ImmutableList.of(sourceV)));
            return paths;
        }

        Map<Id, String> labelMap = newMap(labels.size());
        for (String label : labels) {
            labelMap.put(this.getEdgeLabelId(label), label);
        }
        Traverser traverser = new Traverser(sourceV, targetV, dir, labelMap,
                                            degree, skipDegree, capacity);
        while (true) {
            // Found, reach max depth or reach capacity, stop searching
            if (!(paths = traverser.forward(true)).isEmpty() ||
                --depth <= 0) {
                break;
            }
            checkCapacity(traverser.capacity, traverser.size, "shortest path");

            if (!(paths = traverser.backward(true)).isEmpty() ||
                --depth <= 0) {
                for (Path path : paths.paths()) {
                    Collections.reverse(path.vertices());
                }
                break;
            }
            checkCapacity(traverser.capacity, traverser.size, "shortest path");
        }
        return paths;
    }

    private class Traverser {

        // TODO: change Map to Set to reduce memory cost
        private Map<Id, Node> sources = newMap();
        private Map<Id, Node> targets = newMap();

        private final Directions direction;
        private final Map<Id, String> labels;
        private final long degree;
        private final long skipDegree;
        private final long capacity;
        private long size;

        public Traverser(Id sourceV, Id targetV, Directions dir,
                         Map<Id, String> labels, long degree,
                         long skipDegree, long capacity) {
            this.sources.put(sourceV, new Node(sourceV));
            this.targets.put(targetV, new Node(targetV));
            this.direction = dir;
            this.labels = labels;
            this.degree = degree;
            this.skipDegree = skipDegree;
            this.capacity = capacity;
            this.size = 0L;
        }

        /**
         * Search forward from source
         */
        public PathSet forward(boolean all) {
            PathSet paths = new PathSet();
            Map<Id, Node> newVertices = newMap();
            long degree = this.skipDegree > 0L ? this.skipDegree : this.degree;
            // Traversal vertices of previous level
            for (Node v : this.sources.values()) {
                Iterator<Edge> edges = edgesOfVertex(v.id(), this.direction,
                                                     this.labels, degree);
                edges = skipSuperNodeIfNeeded(edges, this.degree,
                                              this.skipDegree);
                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    // If cross point exists, shortest path found, concat them
                    if (this.targets.containsKey(target)) {
                        if (this.superNode(target, this.direction)) {
                            continue;
                        }
                        paths.add(new Path(
                                  v.joinPath(this.targets.get(target))));
                        if (!all) {
                            return paths;
                        }
                    }

                    /*
                     * Not found shortest path yet, node is added to
                     * newVertices if:
                     * 1. not in sources and newVertices yet
                     * 2. path of node doesn't have loop
                     */
                    if (!newVertices.containsKey(target) &&
                        !this.sources.containsKey(target) &&
                        !v.contains(target)) {
                        newVertices.put(target, new Node(target, v));
                    }
                }
            }

            // Re-init sources
            this.sources = newVertices;
            this.size += newVertices.size();

            return paths;
        }

        /**
         * Search backward from target
         */
        public PathSet backward(boolean all) {
            PathSet paths = new PathSet();
            Map<Id, Node> newVertices = newMap();
            long degree = this.skipDegree > 0L ? this.skipDegree : this.degree;
            Directions opposite = this.direction.opposite();
            // Traversal vertices of previous level
            for (Node v : this.targets.values()) {
                Iterator<Edge> edges = edgesOfVertex(v.id(), opposite,
                                                     this.labels, degree);
                edges = skipSuperNodeIfNeeded(edges, this.degree,
                                              this.skipDegree);
                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    // If cross point exists, shortest path found, concat them
                    if (this.sources.containsKey(target)) {
                        if (this.superNode(target, opposite)) {
                            continue;
                        }
                        paths.add(new Path(
                                  v.joinPath(this.sources.get(target))));
                        if (!all) {
                            return paths;
                        }
                    }

                    /*
                     * Not found shortest path yet, node is added to
                     * newVertices if:
                     * 1. not in targets and newVertices yet
                     * 2. path of node doesn't have loop
                     */
                    if (!newVertices.containsKey(target) &&
                        !this.targets.containsKey(target) &&
                        !v.contains(target)) {
                        newVertices.put(target, new Node(target, v));
                    }
                }
            }

            // Re-init targets
            this.targets = newVertices;
            this.size += newVertices.size();

            return paths;
        }

        private boolean superNode(Id vertex, Directions direction) {
            if (this.skipDegree <= 0L) {
                return false;
            }
            Iterator<Edge> edges = edgesOfVertex(vertex, direction,
                                                 this.labels, this.skipDegree);
            return IteratorUtils.count(edges) >= this.skipDegree;
        }
    }
}
