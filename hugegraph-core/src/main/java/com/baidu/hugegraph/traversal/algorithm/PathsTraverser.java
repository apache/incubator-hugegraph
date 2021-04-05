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

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import org.apache.tinkerpop.gremlin.structure.Edge;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableList;

public class PathsTraverser extends HugeTraverser {

    public PathsTraverser(HugeGraph graph) {
        super(graph);
    }

    public PathSet paths(Id sourceV, Directions sourceDir,
                         Id targetV, Directions targetDir, String label,
                         int depth, long degree, long capacity, long limit) {
        E.checkNotNull(sourceV, "source vertex id");
        E.checkNotNull(targetV, "target vertex id");
        this.checkVertexExist(sourceV, "source vertex");
        this.checkVertexExist(targetV, "target vertex");
        E.checkNotNull(sourceDir, "source direction");
        E.checkNotNull(targetDir, "target direction");
        E.checkArgument(sourceDir == targetDir ||
                        sourceDir == targetDir.opposite(),
                        "Source direction must equal to target direction" +
                        " or opposite to target direction");
        checkPositive(depth, "max depth");
        checkDegree(degree);
        checkCapacity(capacity);
        checkLimit(limit);

        PathSet paths = new PathSet();
        if (sourceV.equals(targetV)) {
            return paths;
        }

        Id labelId = this.getEdgeLabelId(label);
        Traverser traverser = new Traverser(sourceV, targetV, labelId,
                                            degree, capacity, limit);
        while (true) {
            if (--depth < 0 || traverser.reachLimit()) {
                break;
            }
            traverser.forward(sourceDir);

            if (--depth < 0 || traverser.reachLimit()) {
                break;
            }
            traverser.backward(targetDir);
        }
        paths.addAll(traverser.paths().paths());
        return paths;
    }

    private class Traverser {

        private MultivaluedMap<Id, Node> sources = newMultivalueMap();
        private MultivaluedMap<Id, Node> targets = newMultivalueMap();

        private final Id label;
        private final long degree;
        private final long capacity;
        private final long limit;
        private PathSet paths;
        private int accessed;

        public Traverser(Id sourceV, Id targetV, Id label,
                         long degree, long capacity, long limit) {
            this.sources.add(sourceV, new Node(sourceV));
            this.targets.add(targetV, new Node(targetV));
            this.accessed = 2;
            this.label = label;
            this.degree = degree;
            this.capacity = capacity;
            this.limit = limit;
            this.paths = new PathSet();
        }

        /**
         * Search forward from source
         */
        public void forward(Directions direction) {
            MultivaluedMap<Id, Node> newVertices = newMultivalueMap();
            Iterator<Edge> edges;
            // Traversal vertices of previous level
            for (Map.Entry<Id, List<Node>> entry : this.sources.entrySet()) {
                Id vid = entry.getKey();
                edges = edgesOfVertex(vid, direction, this.label, this.degree);

                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    for (Node n : entry.getValue()) {
                        // If have loop, skip target
                        if (n.contains(target)) {
                            continue;
                        }

                        // If cross point exists, path found, concat them
                        if (this.targets.containsKey(target)) {
                            for (Node node : this.targets.get(target)) {
                                List<Id> path = n.joinPath(node);
                                if (!path.isEmpty()) {
                                    this.paths.add(new Path(target, path));
                                    if (this.reachLimit()) {
                                        return;
                                    }
                                }
                            }
                        }

                        // Add node to next start-nodes
                        newVertices.add(target, new Node(target, n));
                    }
                }
            }
            // Re-init sources
            this.sources = newVertices;
            this.accessed += this.sources.size();
        }

        /**
         * Search backward from target
         */
        public void backward(Directions direction) {
            MultivaluedMap<Id, Node> newVertices = newMultivalueMap();
            Iterator<Edge> edges;
            // Traversal vertices of previous level
            for (Map.Entry<Id, List<Node>> entry : this.targets.entrySet()) {
                Id vid = entry.getKey();
                edges = edgesOfVertex(vid, direction, this.label, this.degree);

                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    for (Node n : entry.getValue()) {
                        // If have loop, skip target
                        if (n.contains(target)) {
                            continue;
                        }

                        // If cross point exists, path found, concat them
                        if (this.sources.containsKey(target)) {
                            for (Node node : this.sources.get(target)) {
                                List<Id> path = n.joinPath(node);
                                if (!path.isEmpty()) {
                                    Path newPath = new Path(target, path);
                                    newPath.reverse();
                                    this.paths.add(newPath);
                                    if (this.reachLimit()) {
                                        return;
                                    }
                                }
                            }
                        }

                        // Add node to next start-nodes
                        newVertices.add(target, new Node(target, n));
                    }
                }
            }

            // Re-init targets
            this.targets = newVertices;
            this.accessed = this.targets.size();
        }

        public PathSet paths() {
            return this.paths;
        }

        private boolean reachLimit() {
            checkCapacity(this.capacity, this.accessed, "paths");
            if (this.limit == NO_LIMIT || this.paths.size() < this.limit) {
                return false;
            }
            return true;
        }
    }
}
