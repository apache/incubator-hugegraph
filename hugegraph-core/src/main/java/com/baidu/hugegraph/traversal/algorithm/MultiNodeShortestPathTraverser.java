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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;

import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.Path.EMPTY_PATH;

public class MultiNodeShortestPathTraverser extends OltpTraverser {

    public MultiNodeShortestPathTraverser(HugeGraph graph) {
        super(graph);
    }

    public List<Path> multiNodeShortestPath(Iterator<Vertex> vertices,
                                            EdgeStep step, int maxDepth,
                                            long capacity) {
        List<Vertex> vertexList = IteratorUtils.list(vertices);
        int vertexCount = vertexList.size();
        E.checkState(vertexCount >= 2 && vertexCount <= MAX_VERTICES,
                     "The number of vertices of multiple node shortest path " +
                     "must in [2, %s], but got: %s",
                     MAX_VERTICES, vertexList.size());
        List<Pair<Id, Id>> pairs = new ArrayList<>();
        cmn(vertexList, vertexCount, 2, 0, null, r -> {
            Id source = ((HugeVertex) r.get(0)).id();
            Id target = ((HugeVertex) r.get(1)).id();
            Pair<Id, Id> pair = Pair.of(source, target);
            pairs.add(pair);
        });

        if (maxDepth >= this.concurrentDepth() &&
            step.direction == Directions.BOTH ||
            vertexCount > 10) {
            return this.multiNodeShortestPathConcurrent(pairs, step,
                                                        maxDepth, capacity);
        } else {
            return this.multiNodeShortestPathSingle(pairs, step,
                                                    maxDepth, capacity);
        }
    }

    public List<Path> multiNodeShortestPathConcurrent(List<Pair<Id, Id>> pairs,
                                                      EdgeStep step,
                                                      int maxDepth,
                                                      long capacity) {
        List<Path> results = new CopyOnWriteArrayList<>();
        ShortestPathTraverser traverser =
                              new ShortestPathTraverser(this.graph());
        this.traversePairs(pairs.iterator(), pair -> {
            Path path = traverser.shortestPath(pair.getLeft(), pair.getRight(),
                                               step, maxDepth, capacity);
            if (!EMPTY_PATH.equals(path)) {
                results.add(path);
            }
        });

        return results;
    }

    public List<Path> multiNodeShortestPathSingle(List<Pair<Id, Id>> pairs,
                                                  EdgeStep step, int maxDepth,
                                                  long capacity) {
        List<Path> results = new ArrayList<>();
        ShortestPathTraverser traverser =
                              new ShortestPathTraverser(this.graph());
        for (Pair<Id, Id> pair : pairs) {
            Path path = traverser.shortestPath(pair.getLeft(), pair.getRight(),
                                               step, maxDepth, capacity);
            if (!EMPTY_PATH.equals(path)) {
                results.add(path);
            }
        }
        return results;
    }

    private static <T> void cmn(List<T> all, int m, int n, int current,
                                List<T> result, Consumer<List<T>> consumer) {
        assert m <= all.size();
        assert current < all.size();
        if (result == null) {
            result = new ArrayList<>(n);
        }
        if (n == 0) {
            // All n items are selected
            consumer.accept(result);
            return;
        }
        if (m < n || current >= all.size()) {
            return;
        }

        // Select current item, continue to select C(m-1, n-1)
        int index = result.size();
        result.add(all.get(current));
        cmn(all, m - 1, n - 1, ++current, result, consumer);
        // Not select current item, continue to select C(m-1, n)
        result.remove(index);
        cmn(all, m - 1, n, current, result, consumer);
    }
}
