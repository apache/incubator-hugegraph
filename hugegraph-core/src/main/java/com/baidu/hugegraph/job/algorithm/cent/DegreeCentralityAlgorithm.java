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

package com.baidu.hugegraph.job.algorithm.cent;

import java.util.Iterator;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.job.Job;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.type.define.Directions;

public class DegreeCentralityAlgorithm extends AbstractCentAlgorithm {

    @Override
    public String name() {
        return "degree_centrality";
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        direction(parameters);
        edgeLabel(parameters);
        top(parameters);
    }

    @Override
    public Object call(Job<Object> job, Map<String, Object> parameters) {
        try (Traverser traverser = new Traverser(job)) {
            return traverser.degreeCentrality(direction(parameters),
                                              edgeLabel(parameters),
                                              top(parameters));
        }
    }

    private static class Traverser extends AlgoTraverser {

        public Traverser(Job<Object> job) {
            super(job);
        }

        public Object degreeCentrality(Directions direction,
                                       String label,
                                       long topN) {
            if (direction == null || direction == Directions.BOTH) {
                return degreeCentrality(label, topN);
            }
            assert direction == Directions.OUT || direction == Directions.IN;
            assert topN >= 0L;

            Iterator<Edge> edges = this.edges(direction);

            JsonMap degrees = new JsonMap();
            TopMap<Id> tops = new TopMap<>(topN);
            Id vertex = null;
            Id labelId = this.getEdgeLabelId(label);
            long degree = 0L;
            long total = 0L;

            degrees.startObject();
            while (edges.hasNext()) {
                HugeEdge edge = (HugeEdge) edges.next();
                this.updateProgress(++total);

                Id schemaLabel = edge.schemaLabel().id();
                if (labelId != null && !labelId.equals(schemaLabel)) {
                    continue;
                }

                Id source = edge.ownerVertex().id();
                if (source.equals(vertex)) {
                    // edges belong to same source vertex
                    degree++;
                    continue;
                }

                if (vertex != null) {
                    // next vertex found
                    if (topN <= 0L) {
                        degrees.append(vertex, degree);
                    } else {
                        tops.put(vertex, degree);
                    }
                }
                vertex = source;
                degree = 1L;
            }

            if (vertex != null) {
                if (topN <= 0L) {
                    degrees.append(vertex, degree);
                } else {
                    tops.put(vertex, degree);
                    degrees.append(tops.entrySet());
                }
            }

            degrees.endObject();

            return degrees.asJson();
        }

        protected Object degreeCentrality(String label, long topN) {
            assert topN >= 0L;
            long total = 0L;
            JsonMap degrees = new JsonMap();
            TopMap<Id> tops = new TopMap<>(topN);

            Iterator<Vertex> vertices = this.vertices();

            degrees.startObject();
            while (vertices.hasNext()) {
                Id source = (Id) vertices.next().id();
                this.updateProgress(++total);

                long degree = this.degree(source, label);
                if (degree > 0L) {
                    if (topN <= 0L) {
                        degrees.append(source, degree);
                    } else {
                        tops.put(source, degree);
                    }
                }
            }

            if (tops.size() > 0) {
                degrees.append(tops.entrySet());
            }
            degrees.endObject();

            return degrees.asJson();
        }

        private long degree(Id source, String label) {
            Id labelId = this.getEdgeLabelId(label);
            Iterator<Edge> edges = this.edgesOfVertex(source, Directions.BOTH,
                                                      labelId, NO_LIMIT);
            return IteratorUtils.count(edges);
        }
    }
}
