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

import java.util.Set;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;

public class KoutTraverser extends TpTraverser {

    public KoutTraverser(HugeGraph graph) {
        super(graph, "kout");
    }

    public Set<Id> kout(Id sourceV, Directions dir, String label,
                        int depth, boolean nearest,
                        long degree, long capacity, long limit) {
        E.checkNotNull(sourceV, "source vertex id");
        this.checkVertexExist(sourceV, "source vertex");
        E.checkNotNull(dir, "direction");
        checkPositive(depth, "k-out max_depth");
        checkDegree(degree);
        checkCapacity(capacity);
        checkLimit(limit);
        if (capacity != NO_LIMIT) {
            // Capacity must > limit because sourceV is counted in capacity
            E.checkArgument(capacity >= limit && limit != NO_LIMIT,
                            "Capacity can't be less than limit, " +
                            "but got capacity '%s' and limit '%s'",
                            capacity, limit);
        }

        Id labelId = this.getEdgeLabelId(label);

        Set<Id> latest = newSet();
        latest.add(sourceV);

        Set<Id> all = newSet();
        all.add(sourceV);

        long remaining = capacity == NO_LIMIT ?
                         NO_LIMIT : capacity - latest.size();
        while (depth-- > 0) {
            // Just get limit nodes in last layer if limit < remaining capacity
            if (depth == 0 && limit != NO_LIMIT &&
                (limit < remaining || remaining == NO_LIMIT)) {
                remaining = limit;
            }
            if (nearest) {
                latest = this.adjacentVertices(latest, dir, labelId, all,
                                               degree, remaining);
                all.addAll(latest);
            } else {
                latest = this.adjacentVertices(latest, dir, labelId, null,
                                               degree, remaining);
            }
            if (capacity != NO_LIMIT) {
                // Update 'remaining' value to record remaining capacity
                remaining -= latest.size();

                if (remaining <= 0 && depth > 0) {
                    throw new HugeException(
                              "Reach capacity '%s' while remaining depth '%s'",
                              capacity, depth);
                }
            }
        }

        return latest;
    }

    public Set<Node> customizedKout(Id source, EdgeStep step, int maxDepth,
                                    boolean nearest, long capacity,
                                    long limit) {
        E.checkNotNull(source, "source vertex id");
        this.checkVertexExist(source, "source vertex");
        checkPositive(maxDepth, "k-out max_depth");
        checkCapacity(capacity);
        checkLimit(limit);

        Set<Node> results;
        boolean single = maxDepth < this.concurrentDepth() ||
                         step.direction != Directions.BOTH;
        results = this.customizedKout(source, step, maxDepth, nearest,
                                      capacity, single);

        if (limit != NO_LIMIT && results.size() > limit) {
            results = CollectionUtil.subSet(results, 0, (int) limit);
        }

        return results;
    }

    public Set<Node> customizedKout(Id source, EdgeStep step, int maxDepth,
                                    boolean nearest, long capacity,
                                    boolean single) {
        Set<Node> latest = newSet(single);
        Set<Node> all = newSet(single);

        Node sourceV = new KNode(source, null);

        latest.add(sourceV);
        all.add(sourceV);

        int depth = maxDepth;
        long remaining = capacity == NO_LIMIT ?
                         NO_LIMIT : capacity - latest.size();
        while (depth-- > 0) {
            if (nearest) {
                latest = this.adjacentVertices(latest, step, all,
                                               remaining, single);
                all.addAll(latest);
            } else {
                latest = this.adjacentVertices(latest, step, null,
                                               remaining, single);
            }
            if (capacity != NO_LIMIT) {
                // Update 'remaining' value to record remaining capacity
                remaining -= latest.size();
                reachCapacity(remaining, capacity, depth);
            }
        }

        return latest;
    }

    private static void reachCapacity(long remaining, long capacity,
                                      int depth) {
        if (remaining <= 0 && depth > 0) {
            throw new HugeException(
                      "Reach capacity '%s' while remaining depth '%s'",
                      capacity, depth);
        }
    }
}
