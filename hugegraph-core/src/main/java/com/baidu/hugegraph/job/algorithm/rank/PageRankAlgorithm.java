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

package com.baidu.hugegraph.job.algorithm.rank;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.job.Job;
import com.baidu.hugegraph.job.algorithm.comm.AbstractCommAlgorithm;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.ImmutableMap;

public class PageRankAlgorithm extends AbstractCommAlgorithm {

    protected static final Logger LOG = Log.logger(PageRankAlgorithm.class);

    @Override
    public String name() {
        return "page_rank";
    }

    @Override
    public String category() {
        return CATEGORY_RANK;
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        alpha(parameters);
        times(parameters);
        precision(parameters);
        degree(parameters);
        directionOutIn(parameters);
    }

    @Override
    public Object call(Job<Object> job, Map<String, Object> parameters) {
        Traverser traverser = new Traverser(job);
        try {
            return traverser.pageRank(alpha(parameters),
                                      times(parameters),
                                      precision(parameters),
                                      degree(parameters),
                                      directionOutIn(parameters));
        } catch (Throwable e) {
            job.graph().tx().rollback();
            throw e;
        }
    }

    protected static class Traverser extends AlgoTraverser {

        // DoublePair.left is rank computed by previous step, DoublePair.right
        // is rank computed by current step.
        private Map<Id, DoublePair> vertexRankMap;

        public Traverser(Job<Object> job) {
            super(job);
            this.vertexRankMap = new HashMap<>();
        }

        public Object pageRank(double alpha,
                               int maxTimes,
                               double precision,
                               long degree,
                               Directions direction) {
            this.initSchema();

            int times;
            double changedRank = 0.0;
            long numOfVertices = this.initRankMap();

            for (times = 0; times <= maxTimes; times++) {
                Id currentSourceVertexId = null;
                // the edges are ordered by ownerVertex
                Iterator<Edge> edges = this.edges(direction);
                List<Id> adjacentVertices = new ArrayList<>();

                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id sourceVertexId = edge.ownerVertex().id();
                    Id targetVertexId = edge.otherVertex().id();

                    if (currentSourceVertexId == null) {
                        currentSourceVertexId = sourceVertexId;
                        adjacentVertices.add(targetVertexId);
                    } else if (currentSourceVertexId.equals(sourceVertexId)) {
                        if (adjacentVertices.size() < degree) {
                            adjacentVertices.add(targetVertexId);
                        }
                    } else {
                        this.contributeToAdjacentVertices(currentSourceVertexId,
                                                          adjacentVertices);
                        adjacentVertices = new ArrayList<>();
                        currentSourceVertexId = sourceVertexId;
                        adjacentVertices.add(targetVertexId);
                    }
                }

                // deal with the last vertex
                this.contributeToAdjacentVertices(currentSourceVertexId,
                                                  adjacentVertices);

                double sumRank = this.computeRank(alpha, numOfVertices);

                double compensatedRank = 1.0 - sumRank;
                changedRank =
                        this.compensateRank(compensatedRank / numOfVertices);
                LOG.debug("PageRank execution times:{}, changedRank:{} ",
                          times, changedRank);
                if (changedRank < precision) {
                    break;
                }
            }
            this.writeBackRankValue();

            return ImmutableMap.of("alpha", alpha,
                                   "iteration_times", times,
                                   "last_changed_rank", changedRank,
                                   "times", maxTimes);
        }

        private long initRankMap() {
            long vertexCount = 0;
            Iterator<Vertex> vertices = this.vertices();
            while (vertices.hasNext()) {
                Id vertex = ((HugeVertex) vertices.next()).id();
                DoublePair pair = new DoublePair(0.0, 0.0);
                this.vertexRankMap.put(vertex, pair);
                vertexCount++;
            }

            double initValue = 1.0 / vertexCount;
            for (DoublePair pair : this.vertexRankMap.values()) {
                pair.left(initValue);
            }
            return vertexCount;
        }

        private void contributeToAdjacentVertices(Id sourceVertexId,
                                                  List<Id> adjacentVertices) {
            if (adjacentVertices.size() == 0) {
                return;
            }
            DoublePair sourcePair = this.vertexRankMap.get(sourceVertexId);
            // sourceVertexId not in vertices.
            if (sourcePair == null) {
                LOG.info("source vertex {} not exists.", sourceVertexId);
                return;
            }
            double distributedValue = sourcePair.left() /
                                      adjacentVertices.size();
            for (Id targetId : adjacentVertices) {
                DoublePair targetPair = this.vertexRankMap.get(targetId);
                if (targetPair == null) {
                    // targetId not in vertices
                    LOG.warn("target vertex {} not exists.", targetId);
                    continue;
                }
                targetPair.addRight(distributedValue);
            }
        }

        private double compensateRank(double compensatedRank) {
            double changedRank = 0.0;
            for (DoublePair pair : this.vertexRankMap.values()) {
                double previousRank = pair.left();
                double currentRank = pair.right() + compensatedRank;
                changedRank += Math.abs(previousRank - currentRank);
                pair.left(currentRank);
                pair.right(0.0);
            }
            return changedRank;
        }

        private void initSchema() {
            SchemaManager schema = this.graph().schema();
            schema.propertyKey(R_RANK).asDouble().ifNotExist().create();
            for (VertexLabel vl : schema.getVertexLabels()) {
                schema.vertexLabel(vl.name()).properties(R_RANK)
                      .nullableKeys(R_RANK).append();
            }
        }

        private void writeBackRankValue() {
            for (Map.Entry<Id, DoublePair> entry :
                    this.vertexRankMap.entrySet()) {
                Id vertexId = entry.getKey();
                Iterator<Vertex> vertices = this.graph().vertices(vertexId);
                if (vertices.hasNext()) {
                    Vertex vertex = vertices.next();
                    vertex.property(R_RANK, entry.getValue().left());
                    this.commitIfNeeded();
                }
            }
            this.graph().tx().commit();
        }

        private double computeRank(double alpha, long numOfVertices) {
            double oneMinusAlpha = 1.0 - alpha;
            double sum = 0.0;
            double baseRank = alpha / numOfVertices;
            for (DoublePair pair : this.vertexRankMap.values()) {
                double rankValue = baseRank + pair.right() * oneMinusAlpha;
                pair.right(rankValue);
                sum += rankValue;
            }
            return sum;
        }
    }

    public static class DoublePair {

        private double left;
        private double right;

        public DoublePair(double left, double right) {
            this.left = left;
            this.right = right;
        }

        public void addLeft(double value) {
            this.left += value;
        }

        public void addRight(double value) {
            this.right += value;
        }

        public double left() {
            return left;
        }

        public void left(double value) {
            this.left = value;
        }

        public double right() {
            return right;
        }

        public void right(double value) {
            this.right = value;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("left:").append(left)
              .append(", right: ").append(right);
            return sb.toString();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof DoublePair)) {
                return false;
            }
            DoublePair other = (DoublePair) obj;
            return this.left == other.left && right == other.right;
        }

        @Override
        public int hashCode() {
            return Double.hashCode(left) ^ Double.hashCode(right);
        }
    }
}
