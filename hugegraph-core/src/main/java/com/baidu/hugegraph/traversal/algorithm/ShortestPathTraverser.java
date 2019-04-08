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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableList;

public class ShortestPathTraverser extends HugeTraverser {

    public ShortestPathTraverser(HugeGraph graph) {
        super(graph);
    }

    public List<Id> shortestPath(Id sourceV, Id targetV, Directions dir,
                                 String label, int depth, long degree,
                                 long skipDegree, long capacity) {
        E.checkNotNull(sourceV, "source vertex id");
        E.checkNotNull(targetV, "target vertex id");
        E.checkNotNull(dir, "direction");
        checkPositive(depth, "max depth");
        checkDegree(degree);
        checkCapacity(capacity);
        checkSkipDegree(skipDegree, degree, capacity);

        if (sourceV.equals(targetV)) {
            return ImmutableList.of(sourceV);
        }

        Id labelId = this.getEdgeLabelId(label);
        Traverser traverser = new Traverser(sourceV, targetV, dir, labelId,
                                            degree, skipDegree, capacity);
        List<Id> path;
        while (true) {
            // Found, reach max depth or reach capacity, stop searching
            if ((path = traverser.forward()) != PATH_NONE || --depth <= 0) {
                break;
            }
            checkCapacity(traverser.capacity, traverser.size, "shortest path");

            if ((path = traverser.backward()) != PATH_NONE || --depth <= 0) {
                Collections.reverse(path);
                break;
            }
            checkCapacity(traverser.capacity, traverser.size, "shortest path");
        }
        return path;
    }

    private static void checkSkipDegree(long skipDegree, long degree,
                                        long capacity) {
        E.checkArgument(skipDegree >= 0L,
                        "The skipped degree must be >= 0, but got '%s'",
                        skipDegree);
        if (capacity != NO_LIMIT) {
            E.checkArgument(degree != NO_LIMIT && degree < capacity,
                            "The degree must be < capacity");
            E.checkArgument(skipDegree < capacity,
                            "The skipped degree must be < capacity");
        }
        if (skipDegree > 0L) {
            E.checkArgument(degree != NO_LIMIT && skipDegree >= degree,
                            "The skipped degree must be >= degree, " +
                            "but got skipped degree '%s' and degree '%s'",
                            skipDegree, degree);
        }
    }

    private class Traverser {

        // TODO: change Map to Set to reduce memory cost
        private Map<Id, Node> sources = newMap();
        private Map<Id, Node> targets = newMap();

        private final Directions direction;
        private final Id label;
        private final long degree;
        private final long skipDegree;
        private final long capacity;
        private long size;

        public Traverser(Id sourceV, Id targetV, Directions dir, Id label,
                         long degree, long skipDegree, long capacity) {
            this.sources.put(sourceV, new Node(sourceV));
            this.targets.put(targetV, new Node(targetV));
            this.direction = dir;
            this.label = label;
            this.degree = degree;
            this.skipDegree = skipDegree;
            this.capacity = capacity;
            this.size = 0L;
        }

        /**
         * Search forward from source
         */
        public List<Id> forward() {
            Map<Id, Node> newVertices = newMap();
            long degree = this.skipDegree > 0L ? this.skipDegree : this.degree;
            // Traversal vertices of previous level
            for (Node v : this.sources.values()) {
                Iterator<Edge> edges = edgesOfVertex(v.id(), this.direction,
                                                     this.label, degree);
                edges = this.skipSuperNodeIfNeeded(edges);
                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    // If cross point exists, shortest path found, concat them
                    if (this.targets.containsKey(target)) {
                        if (this.superNode(target, this.direction)) {
                            continue;
                        }
                        return v.joinPath(this.targets.get(target));
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

            return PATH_NONE;
        }

        /**
         * Search backward from target
         */
        public List<Id> backward() {
            Map<Id, Node> newVertices = newMap();
            long degree = this.skipDegree > 0L ? this.skipDegree : this.degree;
            Directions opposite = this.direction.opposite();
            // Traversal vertices of previous level
            for (Node v : this.targets.values()) {
                Iterator<Edge> edges = edgesOfVertex(v.id(), opposite,
                                                     this.label, degree);
                edges = this.skipSuperNodeIfNeeded(edges);
                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    // If cross point exists, shortest path found, concat them
                    if (this.sources.containsKey(target)) {
                        if (this.superNode(target, opposite)) {
                            continue;
                        }
                        return v.joinPath(this.sources.get(target));
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

            return PATH_NONE;
        }

        private Iterator<Edge> skipSuperNodeIfNeeded(Iterator<Edge> edges) {
            if (this.skipDegree <= 0L) {
                return edges;
            }
            List<Edge> edgeList = new ArrayList<>();
            for (int i = 1; edges.hasNext(); i++) {
                if (i <= this.degree) {
                    edgeList.add(edges.next());
                }
                if (i >= this.skipDegree) {
                    return Collections.emptyIterator();
                }
            }
            return edgeList.iterator();
        }

        private boolean superNode(Id vertex, Directions direction) {
            if (this.skipDegree <= 0L) {
                return false;
            }
            Iterator<Edge> edges = edgesOfVertex(vertex, direction,
                                                 this.label, this.skipDegree);
            return IteratorUtils.count(edges) >= this.skipDegree;
        }
    }
}
