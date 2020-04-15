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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

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
        top(parameters);
    }

    @Override
    public Object call(Job<Object> job, Map<String, Object> parameters) {
        Traverser traverser = new Traverser(job);
        return traverser.degreeCentrality(direction(parameters),
                                          top(parameters));
    }

    private static class Traverser extends AlgoTraverser {

        public Traverser(Job<Object> job) {
            super(job);
        }

        public Object degreeCentrality(Directions direction, long topN) {
            if (direction == null || direction == Directions.BOTH) {
                return degreeCentrality(topN);
            }
            assert direction == Directions.OUT || direction == Directions.IN;
            assert topN >= 0L;

            Iterator<Edge> edges = this.edges(direction);

            JsonMap degrees = new JsonMap();
            TopMap<Id> tops = new TopMap<>(topN);
            Id vertex = null;
            long degree = 0L;
            long total = 0L;

            degrees.startObject();
            while (edges.hasNext()) {
                HugeEdge edge = (HugeEdge) edges.next();
                this.updateProgress(++total);

                Id source = edge.ownerVertex().id();
                if (source.equals(vertex)) {
                    degree++;
                    continue;
                }
                if (vertex != null) {
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

        protected Object degreeCentrality(long topN) {
            assert topN >= 0L;
            long total = 0L;
            JsonMap degrees = new JsonMap();
            TopMap<Id> tops = new TopMap<>(topN);

            GraphTraversalSource traversal = this.graph().traversal();
            Iterator<Vertex> vertices = this.vertices();

            degrees.startObject();
            while (vertices.hasNext()) {
                Vertex source = vertices.next();
                this.updateProgress(++total);

                Long degree = traversal.V(source).bothE().count().next();
                if (topN <= 0L) {
                    degrees.append(source.id(), degree);
                } else {
                    tops.put((Id) source.id(), degree);
                }
            }

            if (tops.size() > 0) {
                degrees.append(tops.entrySet());
            }
            degrees.endObject();

            return degrees.asJson();
        }
    }
}
