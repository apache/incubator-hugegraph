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

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableList;

public class ShortestPathTraverserNew extends HugeTraverser {

    public ShortestPathTraverserNew(HugeGraph graph) {
        super(graph);
    }

    public List<Id> shortestPath(Id sourceV, Id targetV, Directions dir,
                                 String label, int depth,int max_edges, long degree,
                                 long capacity) {
        E.checkNotNull(sourceV, "source vertex id");
        E.checkNotNull(targetV, "target vertex id");
        E.checkNotNull(dir, "direction");
        checkPositive(depth, "max depth");
        checkDegree(degree);
        checkCapacity(capacity);

        if (sourceV.equals(targetV)) {
            return ImmutableList.of(sourceV);
        }

        Id labelId = this.getEdgeLabelId(label);
        Traverser traverser = new Traverser(sourceV, targetV, dir, labelId,max_edges,
                                            degree, capacity);
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

    private class Traverser {

        // TODO: change Map to Set to reduce memory cost
        private Map<Id, Node> sources = newMap();
        private Map<Id, Node> targets = newMap();

        private final Directions direction;
        private final Id label;
        private final long degree;
        private final long capacity;
        private long size;
        private final int max_edges;

        public Traverser(Id sourceV, Id targetV, Directions dir,
                         Id label,int max_edges, long degree, long capacity) {
            this.sources.put(sourceV, new Node(sourceV));
            this.targets.put(targetV, new Node(targetV));
            this.direction = dir;
            this.label = label;
            this.degree = degree;
            this.capacity = capacity;
            this.size = 0L;
            this.max_edges=max_edges;
        }

        /**
         * Search forward from source
         */
        public List<Id> forward() {
            Map<Id, Node> newVertices = newMap();
            // Traversal vertices of previous level
            for (Node v : this.sources.values()) {
                //TODO 判断是否为超级节点，如当前节点边的数量大于1000，就不经过滤当前点
                if(v.path().size()< max_edges) {
                    Iterator<Edge> edges = edgesOfVertex(v.id(), this.direction,
                            this.label, this.degree);
                    while (edges.hasNext()) {
                        HugeEdge edge = (HugeEdge) edges.next();
                        Id target = edge.id().otherVertexId();

                        // If cross point exists, shortest path found, concat them
                        if (this.targets.containsKey(target)) {
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
            Directions opposite = this.direction.opposite();
            // Traversal vertices of previous level
            for (Node v : this.targets.values()) {
                //TODO 判断是否为超级节点，如当前节点边的数量大于1000，就不经过滤当前点
                if(v.path().size()< max_edges) {

                    Iterator<Edge> edges = edgesOfVertex(v.id(), opposite,
                            this.label, this.degree);
                    while (edges.hasNext()) {
                        HugeEdge edge = (HugeEdge) edges.next();
                        Id target = edge.id().otherVertexId();

                        // If cross point exists, shortest path found, concat them
                        if (this.sources.containsKey(target)) {
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
            }

            // Re-init targets
            this.targets = newVertices;
            this.size += newVertices.size();

            return PATH_NONE;
        }
    }
}
