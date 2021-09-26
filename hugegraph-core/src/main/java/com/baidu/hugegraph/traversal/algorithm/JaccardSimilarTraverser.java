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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.traversal.algorithm.steps.EdgeStep;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableMap;

public class JaccardSimilarTraverser extends OltpTraverser {

    public JaccardSimilarTraverser(HugeGraph graph) {
        super(graph);
    }


    public double jaccardSimilarity(Id vertex, Id other, Directions dir,
                                    String label, long degree) {
        E.checkNotNull(vertex, "vertex id");
        E.checkNotNull(other, "the other vertex id");
        this.checkVertexExist(vertex, "vertex");
        this.checkVertexExist(other, "other vertex");
        E.checkNotNull(dir, "direction");
        checkDegree(degree);

        Id labelId = this.getEdgeLabelId(label);

        Set<Id> sourceNeighbors = IteratorUtils.set(this.adjacentVertices(
                                  vertex, dir, labelId, degree));
        Set<Id> targetNeighbors = IteratorUtils.set(this.adjacentVertices(
                                  other, dir, labelId, degree));
        return jaccardSimilarity(sourceNeighbors, targetNeighbors);
    }

    public double jaccardSimilarity(Set<Id> set1, Set<Id> set2) {
        int interNum = CollectionUtil.intersect(set1, set2).size();
        int unionNum = CollectionUtil.union(set1, set2).size();
        if (unionNum == 0) {
            return 0.0D;
        }
        return (double) interNum / unionNum;
    }

    public Map<Id, Double> jaccardSimilars(Id source, EdgeStep step,
                                           int top, long capacity) {
        E.checkNotNull(source, "source vertex id");
        this.checkVertexExist(source, "source vertex");
        checkCapacity(capacity);

        Map<Id, Double> results;
        int maxDepth = 3;
        if (maxDepth >= this.concurrentDepth()) {
            results = this.jaccardSimilarsConcurrent(source, step, capacity);
        } else {
            results = this.jaccardSimilarsSingle(source, step, capacity);
        }

        if (top > 0) {
            results = HugeTraverser.topN(results, true, top);
        }

        return results;
    }

    public Map<Id, Double> jaccardSimilarsConcurrent(Id source, EdgeStep step,
                                                     long capacity) {
        AtomicLong count = new AtomicLong(0L);
        Set<Id> accessed = ConcurrentHashMap.newKeySet();
        accessed.add(source);
        reachCapacity(count.incrementAndGet(), capacity);

        // Query neighbors
        Set<Id> layer1s = this.adjacentVertices(source, step);
        reachCapacity(count.get() + layer1s.size(), capacity);
        count.addAndGet(layer1s.size());
        if (layer1s.isEmpty()) {
            return ImmutableMap.of();
        }

        Map<Id, Double> results = new ConcurrentHashMap<>();
        Set<Id> layer2All = ConcurrentHashMap.newKeySet();

        this.traverseIds(layer1s.iterator(), id -> {
            // Skip if accessed already
            if (accessed.contains(id)) {
                return;
            }
            Set<Id> layer2s = this.adjacentVertices(id, step);
            if (layer2s.isEmpty()) {
                results.put(id, 0.0D);
            }

            layer2All.addAll(layer2s);
            reachCapacity(count.get() + layer2All.size(), capacity);
            double jaccardSimilarity = this.jaccardSimilarity(layer1s, layer2s);
            results.put(id, jaccardSimilarity);
            accessed.add(id);
        });

        count.addAndGet(layer2All.size());

        this.traverseIds(layer2All.iterator(), id -> {
            // Skip if accessed already
            if (accessed.contains(id)) {
                return;
            }
            Set<Id> layer3s = this.adjacentVertices(id, step);
            reachCapacity(count.get() + layer3s.size(), capacity);
            if (layer3s.isEmpty()) {
                results.put(id, 0.0D);
            }

            double jaccardSimilarity = this.jaccardSimilarity(layer1s, layer3s);
            results.put(id, jaccardSimilarity);
            accessed.add(id);
        });

        return results;
    }

    public Map<Id, Double> jaccardSimilarsSingle(Id source, EdgeStep step,
                                                 long capacity) {
        long count = 0L;
        Set<Id> accessed = newIdSet();
        accessed.add(source);
        reachCapacity(++count, capacity);

        // Query neighbors
        Set<Id> layer1s = this.adjacentVertices(source, step);
        reachCapacity(count + layer1s.size(), capacity);
        count += layer1s.size();
        if (layer1s.isEmpty()) {
            return ImmutableMap.of();
        }

        Map<Id, Double> results = newMap();
        Set<Id> layer2s;
        Set<Id> layer2All = newIdSet();
        double jaccardSimilarity;
        for (Id neighbor : layer1s) {
            // Skip if accessed already
            if (accessed.contains(neighbor)) {
                continue;
            }
            layer2s = this.adjacentVertices(neighbor, step);
            if (layer2s.isEmpty()) {
                results.put(neighbor, 0.0D);
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
                results.put(neighbor, 0.0D);
                continue;
            }

            jaccardSimilarity = this.jaccardSimilarity(layer1s, layer3s);
            results.put(neighbor, jaccardSimilarity);
            accessed.add(neighbor);
        }

        return results;
    }

    private static void reachCapacity(long count, long capacity) {
        if (capacity != NO_LIMIT && count > capacity) {
            throw new HugeException("Reach capacity '%s'", capacity);
        }
    }
}
