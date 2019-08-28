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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.Frequency;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;

public class FusiformSimilarityTraverser extends HugeTraverser {

    private Directions direction;
    private EdgeLabel edgeLabel;
    private int minNeighborCount;
    private long degree;
    private float alpha;
    private int top;
    private String groupProperty;
    private int minGroupCount;
    private long capacity;
    private long limit;

    private long accessed = 0;
    private int loop = 0;

    public FusiformSimilarityTraverser(HugeGraph graph) {
        super(graph);
    }

    private void init(Directions direction, EdgeLabel edgeLabel,
                      int minNeighborCount, long degree, float alpha, int top,
                      String groupProperty, int minGroupCount, long capacity,
                      long limit) {
        this.direction = direction;
        this.edgeLabel = edgeLabel;
        this.minNeighborCount = minNeighborCount;
        this.degree = degree;
        this.alpha = alpha;
        this.top = top;
        this.groupProperty = groupProperty;
        this.minGroupCount = minGroupCount;
        this.capacity = capacity;
        this.limit = limit;
    }

    public Map<Id, Set<Id>> fusiformSimilarity(Iterator<Vertex> vertices,
                                               Directions direction,
                                               EdgeLabel edgeLabel,
                                               int minNeighborCount,
                                               long degree, float alpha,
                                               int top, String groupProperty,
                                               int minGroupCount, long capacity,
                                               long limit) {
        checkCapacity(capacity);
        checkLimit(limit);
        checkGroupArgs(groupProperty, minGroupCount);

        this.init(direction, edgeLabel, minNeighborCount, degree, alpha, top,
                  groupProperty, minGroupCount, capacity, limit);

        Map<Id, Set<Id>> results = new LinkedHashMap<>();
        while (vertices.hasNext()) {
            checkCapacity(this.capacity, ++this.accessed,
                          "fusiform similarity");
            HugeVertex vertex = (HugeVertex) vertices.next();
            // Find fusiform similarity for current vertex
            this.fusiformSimilarityForVertex(vertex, results);
            // Reach limit
            if (limit != NO_LIMIT && this.loop >= limit) {
                break;
            }
        }
        return results;
    }

    private void fusiformSimilarityForVertex(HugeVertex vertex,
                                             Map<Id, Set<Id>> results) {
        if (!this.matchMinNeighborCount(vertex)) {
            // Ignore current vertex if its neighbors number is not enough
            return;
        }
        Id label = this.edgeLabel.id();
        // Get similar nodes and counts
        Iterator<Edge> edges = this.edgesOfVertex(vertex.id(), this.direction,
                                                  label, this.degree);
        Map<Id, MutableInteger> similars = new HashMap<>();
        Set<Id> neighbors = new HashSet<>();
        while (edges.hasNext()) {
            Id target = ((HugeEdge) edges.next()).id().otherVertexId();
            if (neighbors.contains(target)) {
                continue;
            }
            neighbors.add(target);
            checkCapacity(this.capacity, ++this.accessed,
                          "fusiform similarity");
            Directions backDir = this.direction.opposite();
            Iterator<Edge> backEdges = this.edgesOfVertex(target, backDir,
                                                          label, this.degree);
            Set<Id> currentSimilars = new HashSet<>();
            while (backEdges.hasNext()) {
                Id node = ((HugeEdge) backEdges.next()).id().otherVertexId();
                if (currentSimilars.contains(node)) {
                    continue;
                }
                currentSimilars.add(node);
                MutableInteger count = similars.get(node);
                if (count == null) {
                    count = new MutableInteger(0);
                    similars.put(node, count);
                    checkCapacity(this.capacity, ++this.accessed,
                                  "fusiform similarity");
                }
                count.increase();
            }
        }

        // Delete source vertex
        assert similars.containsKey(vertex.id());
        similars.remove(vertex.id());
        if (similars.isEmpty()) {
            return;
        }

        // Match alpha
        int alphaCount = (int) (neighbors.size() * this.alpha);
        Map<Id, Integer> matchedAlpha = new HashMap<>();
        for (Map.Entry<Id, MutableInteger> entry : similars.entrySet()) {
            int count = entry.getValue().value();
            if (count >= alphaCount) {
                matchedAlpha.put(entry.getKey(), count);
            }
        }
        if (matchedAlpha.isEmpty()) {
            return;
        }

        // Sorted and topN if needed
        Map<Id, Integer> topN;
        if (this.top > 0) {
            topN = CollectionUtil.sortByValue(matchedAlpha, false);
            topN = topN(topN, true, this.top);
        } else {
            topN = matchedAlpha;
        }

        // Filter by groupCount by property
        if (this.groupProperty != null) {
            Set<Object> values = new HashSet<>();
            // Add groupProperty value of source vertex
            values.add(vertex.value(this.groupProperty));
            for (Id id : topN.keySet()) {
                Vertex v = graph().vertices(id).next();
                values.add(v.value(this.groupProperty));
            }
            if (values.size() < this.minGroupCount) {
                return;
            }
        }
        results.put(vertex.id(), topN.keySet());
        if (this.limit != NO_LIMIT) {
            ++this.loop;
        }
    }

    private static void checkGroupArgs(String groupProperty,
                                       int minGroupCount) {
        if (groupProperty != null) {
            E.checkArgument(minGroupCount > 0,
                            "Must set min group count when " +
                            "group property set");
        }
    }

    private boolean matchMinNeighborCount(HugeVertex vertex) {
        Iterator<Edge> edges;
        Id label = this.edgeLabel.id();
        long neighborCount;
        if (this.edgeLabel.frequency() == Frequency.SINGLE) {
            edges = this.edgesOfVertex(vertex.id(), this.direction,
                                       label, this.minNeighborCount);
            neighborCount = IteratorUtils.count(edges);
        } else {
            edges = this.edgesOfVertex(vertex.id(), this.direction,
                                       label, this.degree);
            Set<Id> neighbors = new HashSet<>();
            while (edges.hasNext()) {
                Id target = ((HugeEdge) edges.next()).id().otherVertexId();
                neighbors.add(target);
                if (neighbors.size() >= this.minNeighborCount) {
                    break;
                }
            }
            neighborCount = neighbors.size();
        }
        return neighborCount >= this.minNeighborCount;
    }

    public static class MutableInteger{

        private int count;

        private MutableInteger(int initial) {
            this.count = initial;
        }

        public void increase() {
            this.count++;
        }

        public int value() {
            return this.count;
        }
    }
}
