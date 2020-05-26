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

package com.baidu.hugegraph.job.algorithm.comm;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Edge;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.job.Job;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.google.common.collect.ImmutableMap;

public class TriangleCountAlgorithm extends AbstractCommAlgorithm {

    @Override
    public String name() {
        return "triangle_count";
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        directionOutIn(parameters);
        degree(parameters);
    }

    @Override
    public Object call(Job<Object> job, Map<String, Object> parameters) {
        try (Traverser traverser = new Traverser(job)) {
            return traverser.triangleCount(direction(parameters),
                                           degree(parameters));
        }
    }

    protected static class Traverser extends AlgoTraverser {

        protected static final String KEY_TRIANGLES = "triangles";
        protected static final String KEY_TRIADS = "triads";

        public Traverser(Job<Object> job) {
            super(job);
        }

        public Object triangleCount(Directions direction, long degree) {
            Map<String, Long> results = triangles( direction, degree);
            results = InsertionOrderUtil.newMap(results);
            results.remove(KEY_TRIADS);
            return results;
        }

        protected Map<String, Long> triangles(Directions direction,
                                              long degree) {
            if (direction == null || direction == Directions.BOTH) {
                throw new IllegalArgumentException("Direction must be OUT/IN");
            }
            assert direction == Directions.OUT || direction == Directions.IN;

            Iterator<Edge> edges = this.edges(direction);

            long triangles = 0L;
            long triads = 0L;
            long total = 0L;
            long totalVertices = 0L;
            Id vertex = null;

            Set<Id> adjVertices = new HashSet<>();
            while (edges.hasNext()) {
                HugeEdge edge = (HugeEdge) edges.next();
                this.updateProgress(++total);

                Id source = edge.ownerVertex().id();
                Id target = edge.otherVertex().id();
                if (vertex == source) {
                    // Ignore and skip the target vertex if exceed degree
                    if (adjVertices.size() < degree || degree == NO_LIMIT) {
                        adjVertices.add(target);
                    }
                    continue;
                }

                if (vertex != null) {
                    assert vertex != source;
                    /*
                     * Find graph mode like this:
                     * A -> [B,C,D,E,F]
                     *      B -> [D,F]
                     *      E -> [B,C,F]
                     */
                    triangles += this.intersect(direction, degree, adjVertices);
                    triads += this.localTriads(adjVertices.size());
                    totalVertices++;
                    // Reset for the next source
                    adjVertices = new HashSet<>();
                }
                vertex = source;
                adjVertices.add(target);
            }

            if (vertex != null) {
                triangles += this.intersect(direction, degree, adjVertices);
                triads += this.localTriads(adjVertices.size());
                totalVertices++;
            }

            String suffix = "_" + direction.string();
            return ImmutableMap.of("edges" + suffix, total,
                                   "vertices" + suffix, totalVertices,
                                   KEY_TRIANGLES, triangles,
                                   KEY_TRIADS, triads);
        }

        protected long intersect(Directions dir, long degree,
                                 Set<Id> adjVertices) {
            long count = 0L;
            Iterator<Id> vertices;
            for (Id v : adjVertices) {
                vertices = this.adjacentVertices(v, dir, null, degree);
                while (vertices.hasNext()) {
                    Id vertex = vertices.next();
                    if (adjVertices.contains(vertex)) {
                        count++;
                    }
                }
            }
            return count;
        }

        protected long localTriads(int size) {
            return size * (size - 1L) / 2L;
        }
    }
}
