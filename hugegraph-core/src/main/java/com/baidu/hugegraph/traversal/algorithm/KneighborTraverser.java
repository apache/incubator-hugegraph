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
import java.util.Set;
import java.util.function.Consumer;

import org.apache.tinkerpop.gremlin.structure.Edge;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.traversal.algorithm.records.KneighborRecords;
import com.baidu.hugegraph.traversal.algorithm.records.record.RecordType;
import com.baidu.hugegraph.traversal.algorithm.steps.EdgeStep;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;

public class KneighborTraverser extends OltpTraverser {

    public KneighborTraverser(HugeGraph graph) {
        super(graph);
    }

    public Set<Id> kneighbor(Id sourceV, Directions dir,
                             String label, int depth,
                             long degree, long limit) {
        E.checkNotNull(sourceV, "source vertex id");
        this.checkVertexExist(sourceV, "source vertex");
        E.checkNotNull(dir, "direction");
        checkPositive(depth, "k-neighbor max_depth");
        checkDegree(degree);
        checkLimit(limit);

        Id labelId = this.getEdgeLabelId(label);

        Set<Id> latest = newSet();
        Set<Id> all = newSet();

        latest.add(sourceV);

        while (depth-- > 0) {
            long remaining = limit == NO_LIMIT ? NO_LIMIT : limit - all.size();
            latest = this.adjacentVertices(sourceV, latest, dir, labelId,
                                           all, degree, remaining);
            all.addAll(latest);
            if (limit != NO_LIMIT && all.size() >= limit) {
                break;
            }
        }

        return all;
    }

    public KneighborRecords customizedKneighbor(Id source, EdgeStep step,
                                                int maxDepth, long limit) {
        E.checkNotNull(source, "source vertex id");
        this.checkVertexExist(source, "source vertex");
        checkPositive(maxDepth, "k-neighbor max_depth");
        checkLimit(limit);

        boolean single = maxDepth < this.concurrentDepth() ||
                         step.direction() != Directions.BOTH;
        Traverser traverser = new Traverser(source, step, maxDepth, limit,
                                            single);
        return traverser.customizedKneighbor();
    }

    private class Traverser {

        private final KneighborRecords record;

        private final EdgeStep step;
        private final long limit;
        private final boolean single;
        private int depth;

        private boolean stop;

        public Traverser(Id source, EdgeStep step, int maxDepth,
                         long limit, boolean single) {
            this.record = new KneighborRecords(source, RecordType.INT,
                                               true, single);
            this.step = step;
            this.depth = maxDepth;
            this.limit = limit;
            this.single = single;
            this.stop = false;
        }

        public KneighborRecords customizedKneighbor() {
            Consumer<Id> consumer = v -> {
                Iterator<Edge> edges = edgesOfVertex(v, step);
                while (edges.hasNext()) {
                    Id target = ((HugeEdge) edges.next()).id().otherVertexId();
                    this.record.addPath(v, target);
                    this.checkLimit(this.limit, this.depth, this.record.size());
                }
            };

            while (this.depth-- > 0) {
                this.record.startOneLayer(true);
                traverseIds(this.record.keys(), consumer,
                            this.single, this.stop);
                this.record.finishOneLayer();
            }
            return this.record;
        }

        private void checkLimit(long limit, long depth, int size) {
            if (limit == NO_LIMIT || depth > 0) {
                return;
            }
            if (size >= limit) {
                this.stop = true;
            }
        }
    }
}
