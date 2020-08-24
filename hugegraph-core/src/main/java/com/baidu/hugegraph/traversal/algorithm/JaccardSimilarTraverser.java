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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableMap;

public class JaccardSimilarTraverser extends HugeTraverser {

    public JaccardSimilarTraverser(HugeGraph graph) {
        super(graph);
    }

    public Map<Id, Double> jaccardSimilars(Id source, EdgeStep step,
                                           int top, long capacity) {
        E.checkNotNull(source, "source vertex id");
        this.checkVertexExist(source, "source vertex");
        checkCapacity(capacity);

        long count = 0L;
        Set<Id> accessed = new HashSet<>();
        accessed.add(source);
        reachCapacity(++count, capacity);

        // Query neighbors
        Set<Id> layer1s = this.adjacentVertices(source, step);
        reachCapacity(count + layer1s.size(), capacity);
        count += layer1s.size();
        if (layer1s.isEmpty()) {
            return ImmutableMap.of();
        }

        Map<Id, Double> results = new HashMap<>();
        Set<Id> layer2s;
        Set<Id> layer2All = new HashSet<>();
        double jaccardSimilarity;
        for (Id neighbor : layer1s) {
            // Skip if accessed already
            if (accessed.contains(neighbor)) {
                continue;
            }
            layer2s = this.adjacentVertices(neighbor, step);
            if (layer2s.isEmpty()) {
                continue;
            }

            layer2All.addAll(layer2s);
            reachCapacity(count + layer2All.size(), capacity);
            jaccardSimilarity = this.jaccardSimilarity(layer1s, layer2s);
            results.put(neighbor, jaccardSimilarity);
            accessed.add(neighbor);
        }
        count += layer2All.size();

        Set<Id> layer3s;
        for (Id neighbor : layer2All) {
            // Skip if accessed already
            if (accessed.contains(neighbor)) {
                continue;
            }
            layer3s = this.adjacentVertices(neighbor, step);
            reachCapacity(count + layer3s.size(), capacity);
            if (layer3s.isEmpty()) {
                continue;
            }

            jaccardSimilarity = this.jaccardSimilarity(layer1s, layer3s);
            results.put(neighbor, jaccardSimilarity);
            accessed.add(neighbor);
        }

        if (top > 0) {
            results = HugeTraverser.topN(results, true, top);
        }
        return results;
    }

    private static void reachCapacity(long count, long capacity) {
        if (capacity != NO_LIMIT && count > capacity) {
            throw new HugeException("Reach capacity '%s'", capacity);
        }
    }
}
