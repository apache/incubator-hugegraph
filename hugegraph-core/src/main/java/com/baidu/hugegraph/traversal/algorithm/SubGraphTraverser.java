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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.MultivaluedMap;

import org.apache.tinkerpop.gremlin.structure.Edge;

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
                                  capacity, limit, false);
    }

    public List<Path> rings(Id sourceV, Directions dir, String label,
                            int depth, long degree, long capacity, long limit) {
        return this.subGraphPaths(sourceV, dir, label, depth, degree,
                                  capacity, limit, true);
    }

    private List<Path> subGraphPaths(Id sourceV, Directions dir, String label,
                                     int depth, long degree, long capacity,
                                     long limit, boolean rings) {
        E.checkNotNull(sourceV, "source vertex id");
        E.checkNotNull(dir, "direction");
        checkPositive(depth, "max depth");
        checkDegree(degree);
        checkCapacity(capacity);
        checkLimit(limit);

        Id labelId = this.getEdgeLabelId(label);
        Traverser traverser = new Traverser(sourceV, labelId, degree,
                                            capacity, limit, rings);
        List<Path> paths = new ArrayList<>();
        while (true) {
            paths.addAll(traverser.forward(dir));
            boolean reachDepth = rings ? --depth <= 0 : depth-- <= 0;
            if (reachDepth || traverser.reachLimit() ||
                traverser.finished()) {
                break;
            }
        }
        return paths;
    }

    private class Traverser {

        private MultivaluedMap<Id, Node> sources = newMultivalueMap();
        private Set<Id> accessedVertices = newSet();

        private final Id label;
        private final long degree;
        private final long capacity;
        private final long limit;
        private final boolean rings;
        private long pathCount;

        public Traverser(Id sourceV, Id label, long degree,
                         long capacity, long limit, boolean rings) {
            this.sources.add(sourceV, new Node(sourceV));
            this.accessedVertices.add(sourceV);
            this.label = label;
            this.degree = degree;
            this.capacity = capacity;
            this.limit = limit;
            this.rings = rings;
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
                edges = edgesOfVertex(vid, direction, this.label, this.degree);

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
                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();
                    this.accessedVertices.add(target);
                    for (Node n : entry.getValue()) {
                        if (!n.contains(target)) {
                            // Add node to next start-nodes
                            newVertices.add(target, new Node(target, n));
                            continue;
                        }
                        // Rings found and expect rings
                        if (this.rings) {
                            assert n.contains(target);
                            List<Id> prePath = n.path();
                            prePath.add(target);
                            paths.add(new Path(null, prePath));
                            this.pathCount++;
                            if (reachLimit()) {
                                return paths;
                            }
                        }
                    }
                }
            }
            // Re-init sources
            this.sources = newVertices;

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
