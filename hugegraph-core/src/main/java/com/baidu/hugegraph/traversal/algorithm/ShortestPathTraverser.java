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

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.traversal.algorithm.records.ShortestPathRecords;
import com.baidu.hugegraph.traversal.algorithm.steps.EdgeStep;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableList;

public class ShortestPathTraverser extends HugeTraverser {

    public ShortestPathTraverser(HugeGraph graph) {
        super(graph);
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
            checkCapacity(traverser.capacity, traverser.accessed(),
                          "shortest path");

            if (!(paths = traverser.backward(false)).isEmpty() ||
                --depth <= 0) {
                break;
            }
            checkCapacity(traverser.capacity, traverser.accessed(),
                          "shortest path");
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
            paths = traverser.forward() ?
                    traverser.forward(true) : traverser.backward(true);
            // Found, reach max depth or reach capacity, stop searching
            if (!paths.isEmpty() || --depth <= 0) {
                break;
            }
            checkCapacity(traverser.capacity, traverser.accessed(),
                          "shortest path");
        }
        return paths;
    }

    private class Traverser {

        private ShortestPathRecords record;
        private final Directions direction;
        private final Map<Id, String> labels;
        private final long degree;
        private final long skipDegree;
        private final long capacity;

        public Traverser(Id sourceV, Id targetV, Directions dir,
                         Map<Id, String> labels, long degree,
                         long skipDegree, long capacity) {
            this.record = new ShortestPathRecords(sourceV, targetV);
            this.direction = dir;
            this.labels = labels;
            this.degree = degree;
            this.skipDegree = skipDegree;
            this.capacity = capacity;
        }

        /**
         * Search forward from source
         */
        @Watched
        public PathSet forward(boolean all) {
            PathSet results = new PathSet();
            long degree = this.skipDegree > 0L ? this.skipDegree : this.degree;

            this.record.startOneLayer(true);
            while (this.record.hasNextKey()) {
                Id source = this.record.nextKey();

                Iterator<Edge> edges = edgesOfVertex(source, this.direction,
                                                     this.labels, degree);
                edges = skipSuperNodeIfNeeded(edges, this.degree,
                                              this.skipDegree);
                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    PathSet paths = this.record.findPath(target,
                                    t -> !this.superNode(t, this.direction),
                                    all, false);

                    if (paths.isEmpty()) {
                        continue;
                    }
                    results.addAll(paths);
                    if (!all) {
                        return paths;
                    }
                }
            }

            this.record.finishOneLayer();

            return results;
        }

        /**
         * Search backward from target
         */
        @Watched
        public PathSet backward(boolean all) {
            PathSet results = new PathSet();
            long degree = this.skipDegree > 0L ? this.skipDegree : this.degree;
            Directions opposite = this.direction.opposite();

            this.record.startOneLayer(false);
            while (this.record.hasNextKey()) {
                Id source = this.record.nextKey();

                Iterator<Edge> edges = edgesOfVertex(source, opposite,
                                                     this.labels, degree);
                edges = skipSuperNodeIfNeeded(edges, this.degree,
                                              this.skipDegree);
                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    PathSet paths = this.record.findPath(target,
                                    t -> !this.superNode(t, opposite),
                                    all, false);

                    if (paths.isEmpty()) {
                        continue;
                    }
                    results.addAll(paths);
                    if (!all) {
                        return results;
                    }
                }
            }

            // Re-init targets
            this.record.finishOneLayer();

            return results;
        }

        public boolean forward() {
            return this.record.lessSources();
        }

        private boolean superNode(Id vertex, Directions direction) {
            if (this.skipDegree <= 0L) {
                return false;
            }
            Iterator<Edge> edges = edgesOfVertex(vertex, direction,
                                                 this.labels, this.skipDegree);
            return IteratorUtils.count(edges) >= this.skipDegree;
        }

        private long accessed() {
            return this.record.accessed();
        }
    }
}
