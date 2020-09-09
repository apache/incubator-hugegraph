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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;

public class CustomizedKoutTraverser extends TpTraverser {

    public CustomizedKoutTraverser(HugeGraph graph) {
        super(graph, "kout");
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
        if (maxDepth >= this.concurrentDepth() &&
            step.direction == Directions.BOTH) {
            results = this.customizedKoutConcurrent(source, step, maxDepth,
                                                    nearest, capacity);
        } else {
            results = this.customizedKoutSingle(source, step, maxDepth,
                                                nearest, capacity);
        }

        if (limit != NO_LIMIT && results.size() > limit) {
            results = CollectionUtil.subSet(results, 0, (int) limit);
        }

        return results;
    }

    public Set<Node> customizedKoutConcurrent(Id source, EdgeStep step,
                                              int maxDepth, boolean nearest,
                                              long capacity) {
        Set<Node> latest = ConcurrentHashMap.newKeySet();
        Set<Node> all = ConcurrentHashMap.newKeySet();

        Node sourceV = new KNode(source, null);

        latest.add(sourceV);
        all.add(sourceV);

        int depth = maxDepth;
        long remaining = capacity == NO_LIMIT ?
                         NO_LIMIT : capacity - latest.size();
        while (depth-- > 0) {
            AtomicLong remain = new AtomicLong(remaining);
            if (nearest) {
                latest = this.adjacentVertices(latest, step, all, remain);
                all.addAll(latest);
            } else {
                latest = this.adjacentVertices(latest, step, null, remain);
            }
            if (capacity != NO_LIMIT) {
                // Update 'remaining' value to record remaining capacity
                remaining -= latest.size();
                reachCapacity(remaining, capacity, depth);
            }
        }

        return latest;
    }

    public Set<Node> customizedKoutSingle(Id source, EdgeStep step,
                                          int maxDepth, boolean nearest,
                                          long capacity) {
        Set<Node> latest = newSet();
        Set<Node> all = newSet();

        Node sourceV = new KNode(source, null);

        latest.add(sourceV);
        all.add(sourceV);

        int depth = maxDepth;
        long remaining = capacity == NO_LIMIT ?
                         NO_LIMIT : capacity - latest.size();
        while (depth-- > 0) {
            if (nearest) {
                latest = this.adjacentVertices(latest, step, all, remaining);
                all.addAll(latest);
            } else {
                latest = this.adjacentVertices(latest, step, null, remaining);
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
