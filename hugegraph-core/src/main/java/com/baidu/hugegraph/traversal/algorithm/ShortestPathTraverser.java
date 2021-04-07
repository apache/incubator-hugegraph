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
import java.util.Stack;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.eclipse.collections.api.iterator.IntIterator;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.traversal.algorithm.steps.EdgeStep;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableList;

public class ShortestPathTraverser extends HugeTraverser {

    public ShortestPathTraverser(HugeGraph graph) {
        super(graph);
        this.idMapping = new IdMapping();
    }

    @Watched
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
                break;
            }
            checkCapacity(traverser.capacity, traverser.size, "shortest path");
        }
        return paths;
    }

    private class Traverser {

        private Stack<IntIntHashMap> sourceLayers;
        private Stack<IntIntHashMap> targetLayers;

        private LongHashSet accessed = new LongHashSet();

        private final Directions direction;
        private final Map<Id, String> labels;
        private final long degree;
        private final long skipDegree;
        private final long capacity;
        private long size;

        private boolean foundPath = false;

        public Traverser(Id sourceV, Id targetV, Directions dir,
                         Map<Id, String> labels, long degree,
                         long skipDegree, long capacity) {
            int sourceCode = this.code(sourceV);
            int targetCode = this.code(targetV);
            IntIntHashMap firstSourceLayer = new IntIntHashMap();
            IntIntHashMap firstTargetLayer = new IntIntHashMap();
            firstSourceLayer.put(sourceCode, 0);
            firstTargetLayer.put(targetCode, 0);
            this.sourceLayers = new Stack<>();
            this.targetLayers = new Stack<>();
            this.sourceLayers.push(firstSourceLayer);
            this.targetLayers.push(firstTargetLayer);

            this.accessed.add(sourceCode);
            this.accessed.add(targetCode);

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
        @Watched
        public PathSet forward(boolean all) {
            PathSet paths = new PathSet();
            IntIntHashMap newLayer = new IntIntHashMap();
            long degree = this.skipDegree > 0L ? this.skipDegree : this.degree;

            assert !this.sourceLayers.isEmpty();
            assert !this.targetLayers.isEmpty();
            IntIntHashMap sourceTopLayer = this.sourceLayers.peek();
            IntIntHashMap targetTopLayer = this.targetLayers.peek();

            IntIterator iterator = sourceTopLayer.keySet().intIterator();
            while (iterator.hasNext()) {
                int sourceCode = iterator.next();
                Iterator<Edge> edges = edgesOfVertex(this.id(sourceCode),
                                                     this.direction,
                                                     this.labels, degree);
                edges = skipSuperNodeIfNeeded(edges, this.degree,
                                              this.skipDegree);
                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();
                    int targetCode = this.code(target);

                    // If cross point exists, shortest path found, concat them
                    if (targetTopLayer.containsKey(targetCode)) {
                        if (this.superNode(target, this.direction)) {
                            continue;
                        }

                        paths.add(this.getPath(sourceCode, targetCode));
                        this.foundPath = true;
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
                    if (!this.foundPath && !newLayer.containsKey(targetCode) &&
                        !this.accessed.contains(this.code(target))) {
                        newLayer.put(targetCode, sourceCode);
                    }
                }
            }

            // Re-init sources
            this.sourceLayers.push(newLayer);
            this.size += newLayer.size();

            return paths;
        }

        /**
         * Search backward from target
         */
        @Watched
        public PathSet backward(boolean all) {
            PathSet paths = new PathSet();
            IntIntHashMap newLayer = new IntIntHashMap();
            long degree = this.skipDegree > 0L ? this.skipDegree : this.degree;

            assert !this.sourceLayers.isEmpty();
            assert !this.targetLayers.isEmpty();
            IntIntHashMap sourceTopLayer = this.targetLayers.peek();
            IntIntHashMap targetTopLayer = this.sourceLayers.peek();

            Directions opposite = this.direction.opposite();

            IntIterator iterator = sourceTopLayer.keySet().intIterator();
            while (iterator.hasNext()) {
                int sourceCode = iterator.next();
                Iterator<Edge> edges = edgesOfVertex(this.id(sourceCode),
                                                     opposite, this.labels,
                                                     degree);
                edges = skipSuperNodeIfNeeded(edges, this.degree,
                                              this.skipDegree);
                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();
                    int targetCode = this.code(target);

                    // If cross point exists, shortest path found, concat them
                    if (targetTopLayer.containsKey(targetCode)) {
                        if (this.superNode(target, opposite)) {
                            continue;
                        }
                        paths.add(this.getPath(targetCode, sourceCode));
                        this.foundPath = true;
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
                    if (!this.foundPath && !newLayer.containsKey(targetCode) &&
                        !this.accessed.contains(targetCode)) {
                        newLayer.put(targetCode, sourceCode);
                    }
                }
            }

            // Re-init targets
            this.targetLayers.push(newLayer);
            this.size += newLayer.size();

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

        private Path getPath(int source, int target) {
            int sourceLayerSize = this.sourceLayers.size();
            int targetLayerSize = this.targetLayers.size();

            List<Id> ids = new ArrayList<>(sourceLayerSize + targetLayerSize);

            ids.add(this.id(source));
            int value = source;
            for (int i = sourceLayerSize - 1; i > 0 ; i--) {
                IntIntHashMap layer = this.sourceLayers.elementAt(i);
                value = layer.get(value);
                ids.add(this.id(value));
            }
            Collections.reverse(ids);
            ids.add(this.id(target));
            value = target;
            for (int i = this.targetLayers.size() - 1; i > 0 ; i--) {
                IntIntHashMap layer = this.targetLayers.elementAt(i);
                value = layer.get(value);
                ids.add(this.id(value));
            }
            return new Path(ids);
        }

        private int code(Id id) {
            return ShortestPathTraverser.this.code(id);
        }

        private Id id(int code) {
            return ShortestPathTraverser.this.id(code);
        }
    }
}
