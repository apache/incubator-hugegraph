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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.MultivaluedMap;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;

public class SubGraphTraverser extends HugeTraverser {

    public SubGraphTraverser(HugeGraph graph) {
        super(graph);
    }

    public List<Path> rays(Id sourceV, Directions dir, String label,
                           int depth, long degree, long capacity, long limit) {
        return this.subGraphPaths(sourceV, dir, label, depth, degree,
                                  capacity, limit, false, false);
    }

    public List<Path> rings(Id sourceV, Directions dir, String label, int depth,
                            boolean sourceInRing, long degree, long capacity,
                            long limit) {
        return this.subGraphPaths(sourceV, dir, label, depth, degree,
                                  capacity, limit, true, sourceInRing);
    }

    private List<Path> subGraphPaths(Id sourceV, Directions dir, String label,
                                     int depth, long degree, long capacity,
                                     long limit, boolean rings,
                                     boolean sourceInRing) {
        E.checkNotNull(sourceV, "source vertex id");
        E.checkNotNull(dir, "direction");
        checkPositive(depth, "max depth");
        checkDegree(degree);
        checkCapacity(capacity);
        checkLimit(limit);

        Id labelId = this.getEdgeLabelId(label);
        Traverser traverser = new Traverser(sourceV, labelId, depth, degree,
                                            capacity, limit, rings,
                                            sourceInRing);
        List<Path> paths = new ArrayList<>();
        while (true) {
            paths.addAll(traverser.forward(dir));
            if (--depth <= 0 || traverser.reachLimit() ||
                traverser.finished()) {
                break;
            }
        }
        return paths;
    }

    private static boolean multiEdges(List<Edge> edges, Id target) {
        int count = 0;
        for (Edge edge : edges) {
            if (((HugeEdge) edge).id().otherVertexId().equals(target)) {
                if (++count >= 2) {
                    return true;
                }
            }
        }
        assert count == 1;
        return false;
    }

    private class Traverser {

        private final Id source;
        private MultivaluedMap<Id, Node> sources = newMultivalueMap();
        private Set<Id> accessedVertices = newSet();

        private final Id label;
        private int depth;
        private final long degree;
        private final long capacity;
        private final long limit;
        private final boolean rings;
        private final boolean sourceInRing;
        private long pathCount;

        public Traverser(Id sourceV, Id label, int depth, long degree,
                         long capacity, long limit, boolean rings,
                         boolean sourceInRing) {
            this.source = sourceV;
            this.sources.add(sourceV, new Node(sourceV));
            this.accessedVertices.add(sourceV);
            this.label = label;
            this.depth = depth;
            this.degree = degree;
            this.capacity = capacity;
            this.limit = limit;
            this.rings = rings;
            this.sourceInRing = sourceInRing;
            this.pathCount = 0L;
        }

        /**
         * Search forward from source
         */
        public List<Path> forward(Directions direction) {
            List<Path> paths = new ArrayList<>();
            MultivaluedMap<Id, Node> newVertices = newMultivalueMap();
            Iterator<Edge> edges;
            // Traversal vertices of previous level
            for (Map.Entry<Id, List<Node>> entry : this.sources.entrySet()) {
                Id vid = entry.getKey();
                List<Edge> edgeList = IteratorUtils.list(edgesOfVertex(
                                      vid, direction, this.label, this.degree));
                edges = edgeList.iterator();

                if (!edges.hasNext()) {
                    // Reach the end, rays found
                    if (this.rings) {
                        continue;
                    }
                    for (Node n : entry.getValue()) {
                        // Store rays
                        paths.add(new Path(null, n.path()));
                        this.pathCount++;
                        if (reachLimit()) {
                            return paths;
                        }
                    }
                }

                int neighborCount = 0;
                Set<Id> currentNeighbors = new HashSet<>();
                while (edges.hasNext()) {
                    neighborCount++;
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();
                    // Avoid dedup path
                    if (currentNeighbors.contains(target)) {
                        continue;
                    }
                    currentNeighbors.add(target);
                    this.accessedVertices.add(target);
                    for (Node node : entry.getValue()) {
                        // No ring, continue
                        if (!node.contains(target)) {
                            // Add node to next start-nodes
                            newVertices.add(target, new Node(target, node));
                            continue;
                        }

                        // Rays found if fake ring like:
                        // path is pattern: A->B<-A && A is only neighbor of B
                        if (!this.rings && target.equals(node.parent().id()) &&
                            neighborCount == 1 && !edges.hasNext() &&
                            direction == Directions.BOTH) {
                            paths.add(new Path(null, node.path()));
                            this.pathCount++;
                            if (reachLimit()) {
                                return paths;
                            }
                        }

                        // Actual rings found
                        if (this.rings) {
                            boolean ringsFound = false;
                            // 1. sourceInRing is false, or
                            // 2. sourceInRing is true and target == source
                            if (!sourceInRing || target.equals(this.source)) {
                                if (!target.equals(node.parent().id())) {
                                    ringsFound = true;
                                } else if (direction != Directions.BOTH) {
                                    ringsFound = true;
                                } else if (multiEdges(edgeList, target)) {
                                    ringsFound = true;
                                }
                            }

                            if (ringsFound) {
                                List<Id> path = node.path();
                                path.add(target);
                                paths.add(new Path(null, path));
                                this.pathCount++;
                                if (reachLimit()) {
                                    return paths;
                                }
                            }
                        }
                    }
                }
            }
            // Re-init sources
            this.sources = newVertices;

            if (!rings && --this.depth <= 0) {
                for (List<Node> list : newVertices.values()) {
                    for (Node n : list) {
                        paths.add(new Path(null, n.path()));
                    }
                }
            }

            return paths;
        }

        private boolean reachLimit() {
            checkCapacity(this.capacity, this.accessedVertices.size(),
                          this.rings ? "rings" : "rays");
            if (this.limit == NO_LIMIT || this.pathCount < this.limit) {
                return false;
            }
            return true;
        }

        private boolean finished() {
            return this.sources.isEmpty();
        }
    }
}
