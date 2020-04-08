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
            paths.add(new Path(sourceV, ImmutableList.of(sourceV)));
        }

        Id labelId = this.getEdgeLabelId(label);
        Traverser traverser = new Traverser(sourceV, targetV, labelId,
                                            degree, capacity, limit);
        while (true) {
            if (--depth < 0 || traverser.reachLimit()) {
                break;
            }
            List<Path> foundPaths = traverser.forward(sourceDir);
            paths.addAll(foundPaths);

            if (--depth < 0 || traverser.reachLimit()) {
                break;
            }
            foundPaths = traverser.backward(targetDir);
            for (Path path : foundPaths) {
                path.reverse();
                paths.add(path);
            }
        }
        return paths;
    }

    private class Traverser {

        private MultivaluedMap<Id, Node> sources = newMultivalueMap();
        private MultivaluedMap<Id, Node> targets = newMultivalueMap();
        private MultivaluedMap<Id, Node> sourcesAll = newMultivalueMap();
        private MultivaluedMap<Id, Node> targetsAll = newMultivalueMap();

        private final Id label;
        private final long degree;
        private final long capacity;
        private final long limit;
        private long count;

        public Traverser(Id sourceV, Id targetV, Id label,
                         long degree, long capacity, long limit) {
            this.sources.add(sourceV, new Node(sourceV));
            this.targets.add(targetV, new Node(targetV));
            this.sourcesAll.putAll(this.sources);
            this.targetsAll.putAll(this.targets);
            this.label = label;
            this.degree = degree;
            this.capacity = capacity;
            this.limit = limit;
            this.count = 0L;
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

                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    for (Node n : entry.getValue()) {
                        // If have loop, skip target
                        if (n.contains(target)) {
                            continue;
                        }

                        // If cross point exists, path found, concat them
                        if (this.targetsAll.containsKey(target)) {
                            for (Node node : this.targetsAll.get(target)) {
                                List<Id> path = n.joinPath(node);
                                if (!path.isEmpty()) {
                                    paths.add(new Path(target, path));
                                    ++this.count;
                                    if (this.reachLimit()) {
                                        return paths;
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
            // Record all passed vertices
            this.sourcesAll.putAll(newVertices);

            return paths;
        }

        /**
         * Search backward from target
         */
        public List<Path> backward(Directions direction) {
            List<Path> paths = new ArrayList<>();
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
                        if (this.sourcesAll.containsKey(target)) {
                            for (Node node : this.sourcesAll.get(target)) {
                                List<Id> path = n.joinPath(node);
                                if (!path.isEmpty()) {
                                    paths.add(new Path(target, path));
                                    ++this.count;
                                    if (this.reachLimit()) {
                                        return paths;
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
            // Record all passed vertices
            this.targetsAll.putAll(newVertices);

            return paths;
        }

        private int accessedNodes() {
            return this.sourcesAll.size() + this.targetsAll.size();
        }

        private boolean reachLimit() {
            checkCapacity(this.capacity, this.accessedNodes(), "paths");
            if (this.limit == NO_LIMIT || this.count < this.limit) {
                return false;
            }
            return true;
        }
    }
}
