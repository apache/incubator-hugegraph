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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;

public class CustomizedKneighborTraverser extends TpTraverser {

    public CustomizedKneighborTraverser(HugeGraph graph) {
        super(graph, "kneighbor");
    }

    public Set<Node> customizedKneighbor(Id source, EdgeStep step,
                                         int maxDepth, long limit) {
        E.checkNotNull(source, "source vertex id");
        this.checkVertexExist(source, "source vertex");
        checkPositive(maxDepth, "k-neighbor max_depth");
        checkLimit(limit);

        if (step.direction == Directions.BOTH && maxDepth > 3) {
            return this.customizedKneighborConcurrent(source, step,
                                                      maxDepth, limit);
        } else {
            return this.customizedKneighborSingle(source, step,
                                                  maxDepth, limit);
        }
    }

    public Set<Node> customizedKneighborConcurrent(Id source, EdgeStep step,
                                                   int maxDepth, long limit) {
        Set<Node> latest = ConcurrentHashMap.newKeySet();
        Set<Node> all = ConcurrentHashMap.newKeySet();

        Node sourceV = new KNode(source, null);

        latest.add(sourceV);
        all.add(sourceV);

        while (maxDepth-- > 0) {
            long remaining = limit == NO_LIMIT ? NO_LIMIT : limit - all.size();
            AtomicLong remain = new AtomicLong(remaining);
            latest = this.adjacentVertices(latest, step, all, remain);
            int size = all.size() + latest.size();
            if (limit != NO_LIMIT && size >= limit) {
                int subLength = (int) limit - all.size();
                Iterator<Node> iterator = latest.iterator();
                for (int i = 0; i < subLength && iterator.hasNext(); i++) {
                    all.add(iterator.next());
                }
                break;
            } else {
                all.addAll(latest);
            }
        }

        return all;
    }

    public Set<Node> customizedKneighborSingle(Id source, EdgeStep step,
                                               int maxDepth, long limit) {
        Set<Node> latest = newSet();
        Set<Node> all = newSet();

        Node sourceV = new KNode(source, null);

        latest.add(sourceV);
        all.add(sourceV);

        while (maxDepth-- > 0) {
            long remaining = limit == NO_LIMIT ? NO_LIMIT : limit - all.size();
            latest = this.adjacentVertices(latest, step, all, remaining);
            int size = all.size() + latest.size();
            if (limit != NO_LIMIT && size >= limit) {
                int subLength = (int) limit - all.size();
                Iterator<Node> iterator = latest.iterator();
                for (int i = 0; i < subLength && iterator.hasNext(); i++) {
                    all.add(iterator.next());
                }
                break;
            } else {
                all.addAll(latest);
            }
        }

        return all;
    }
}
