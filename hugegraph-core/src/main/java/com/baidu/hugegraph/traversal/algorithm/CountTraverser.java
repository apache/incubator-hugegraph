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

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Edge;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Aggregate;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.iterator.FilterIterator;
import com.baidu.hugegraph.iterator.FlatMapperIterator;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;

public class CountTraverser extends HugeTraverser {

    private boolean containsTraversed = false;
    private long dedup = 1000000L;
    private final Set<Id> dedupSet = new HashSet<>();
    private final long[] count = {0L};

    public CountTraverser(HugeGraph graph) {
        super(graph);
    }

    public long count(Id source, List<Step> steps,
                      boolean containsTraversed, long dedup) {
        E.checkArgumentNotNull(source, "The source can't be null");
        E.checkArgument(steps != null && !steps.isEmpty(),
                        "The steps can't be empty");
        checkDedup(dedup);

        this.containsTraversed = containsTraversed;
        this.dedup = dedup;
        if (this.containsTraversed) {
            count[0]++;
        }

        int stepNum = steps.size();
        Step firstStep = steps.get(0);
        if (stepNum == 1) {
            // Just one step, query count and return
            count[0] += this.edgesCount(source, firstStep.direction,
                                        firstStep.labels);
            return count[0];
        }

        // Multiple steps, construct first step to iterator
        Iterator<Edge> edges = this.edgesOfVertex(source, firstStep);
        for (int i = 1; i < stepNum; i++) {
            Step currentStep = steps.get(i);
            if (i != stepNum - 1) {
                edges = new FlatMapperIterator<>(edges, (edge) -> {
                    Id target = ((HugeEdge) edge).id().otherVertexId();
                    return this.edgesOfVertex(target, currentStep);
                });
            } else {
                // The last step, just query count
                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();
                    if (this.dedup(target)) {
                        continue;
                    }
                    // Count last layer vertices(without dedup)
                    this.count[0] += this.edgesCount(target,
                                                     currentStep.direction,
                                                     currentStep.labels);
                }
            }
        }

        return this.count[0];
    }

    private Iterator<Edge> edgesOfVertex(Id source, Step step) {
        if (this.dedup(source)) {
            return QueryResults.emptyIterator();
        }
        Iterator<Edge> flatten = this.edgesOfVertex(source, step.direction,
                                                    step.labels, step.degree,
                                                    step.skipDegree);
        return new FilterIterator<>(flatten, e -> {
            if (this.containsTraversed) {
                // Count intermediate vertices
                this.count[0]++;
            }
            return true;
        });
    }

    private Iterator<Edge> edgesOfVertex(Id source, Directions dir,
                                         Map<Id, String> labels,
                                         long degree, long skipDegree) {
        checkSkipDegree(skipDegree, degree, NO_LIMIT);
        long queryLimit = skipDegree > 0 ? skipDegree : degree;
        Iterator<Edge> edges = this.edgesOfVertex(source, dir, labels,
                                                  queryLimit);
        return ShortestPathTraverser.skipSuperNodeIfNeeded(edges, degree,
                                                           skipDegree);
    }

    private Iterator<Edge> edgesOfVertex(Id source, Directions dir,
                                         Map<Id, String> labels, long limit) {
        Id[] els = labels.keySet().toArray(new Id[labels.size()]);
        Query query = GraphTransaction.constructEdgesQuery(source, dir, els);
        query.capacity(Query.NO_CAPACITY);
        if (limit != NO_LIMIT) {
            query.limit(limit);
        }
        return this.graph().edges(query);
    }

    private long edgesCount(Id source, Directions dir, Map<Id, String> labels) {
        Id[] els = labels.keySet().toArray(new Id[labels.size()]);
        Query query = GraphTransaction.constructEdgesQuery(source, dir, els);
        query.aggregate(Aggregate.AggregateFunc.COUNT, null);
        query.capacity(Query.NO_CAPACITY);
        query.limit(Query.NO_LIMIT);
        return graph().queryNumber(query).longValue();
    }

    private void checkDedup(long dedup) {
        checkNonNegativeOrNoLimit(dedup, "dedup");
    }

    private boolean dedup(Id vertex) {
        if (!this.needDedup()) {
            return false;
        }

        if (this.dedupSet.contains(vertex)) {
            // Skip vertex already traversed
            return true;
        } else if (!this.reachDedup()) {
            // Record vertex not traversed before if not reach dedup
            this.dedupSet.add(vertex);
        }
        return false;
    }

    private boolean needDedup() {
        return this.dedup != 0L;
    }

    private boolean reachDedup() {
        return this.dedup != NO_LIMIT && this.dedupSet.size() >= this.dedup;
    }

    public static class Step {

        private Directions direction;
        private Map<Id, String> labels;
        private long degree;
        private long skipDegree;

        public Step(Directions direction, Map<Id, String> labels,
                    long degree, long skipDegree) {
            this.direction = direction;
            this.labels = labels;
            this.degree = degree;
            this.skipDegree = skipDegree;
        }
    }
}
