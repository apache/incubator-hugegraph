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
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

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
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class FusiformSimilarityTraverser extends HugeTraverser {

    private long accessed = 0;

    public FusiformSimilarityTraverser(HugeGraph graph) {
        super(graph);
    }

    public SimilarsMap fusiformSimilarity(Iterator<Vertex> vertices,
                                          Directions direction, EdgeLabel label,
                                          int minNeighborCount, long degree,
                                          double alpha, int top,
                                          String groupProperty,
                                          int minGroupCount,
                                          long capacity, long limit,
                                          boolean withIntermediary) {
        checkCapacity(capacity);
        checkLimit(limit);
        checkGroupArgs(groupProperty, minGroupCount);

        int foundCount = 0;
        SimilarsMap results = new SimilarsMap();
        while (vertices.hasNext()) {
            checkCapacity(capacity, ++this.accessed, "fusiform similarity");
            HugeVertex vertex = (HugeVertex) vertices.next();
            // Find fusiform similarity for current vertex
            Set<Similar> result = this.fusiformSimilarityForVertex(
                                  vertex, direction, label, minNeighborCount,
                                  degree, alpha, top, groupProperty,
                                  minGroupCount, capacity, withIntermediary);
            if (result.isEmpty()) {
                continue;
            }
            results.put(vertex.id(), result);
            // Reach limit
            if (limit != NO_LIMIT && ++foundCount >= limit) {
                break;
            }
        }
        return results;
    }

    private Set<Similar> fusiformSimilarityForVertex(
                         HugeVertex vertex, Directions direction,
                         EdgeLabel label, int minNeighborCount, long degree,
                         double alpha, int top, String groupProperty,
                         int minGroupCount, long capacity,
                         boolean withIntermediary) {
        boolean matched = this.matchMinNeighborCount(vertex, direction, label,
                                                     minNeighborCount, degree);
        if (!matched) {
            // Ignore current vertex if its neighbors number is not enough
            return ImmutableSet.of();
        }
        Id labelId = label == null ? null : label.id();
        // Get similar nodes and counts
        Iterator<Edge> edges = this.edgesOfVertex(vertex.id(), direction,
                                                  labelId, degree);
        Map<Id, MutableInteger> similars = new HashMap<>();
        MultivaluedMap<Id, Id> intermediaries = new MultivaluedHashMap<>();
        Set<Id> neighbors = new HashSet<>();
        while (edges.hasNext()) {
            Id target = ((HugeEdge) edges.next()).id().otherVertexId();
            if (neighbors.contains(target)) {
                continue;
            }
            neighbors.add(target);
            checkCapacity(capacity, ++this.accessed, "fusiform similarity");

            Directions backDir = direction.opposite();
            Iterator<Edge> backEdges = this.edgesOfVertex(target, backDir,
                                                          labelId, degree);
            Set<Id> currentSimilars = new HashSet<>();
            while (backEdges.hasNext()) {
                Id node = ((HugeEdge) backEdges.next()).id().otherVertexId();
                if (currentSimilars.contains(node)) {
                    continue;
                }
                currentSimilars.add(node);
                if (withIntermediary) {
                    intermediaries.add(node, target);
                }

                MutableInteger count = similars.get(node);
                if (count == null) {
                    count = new MutableInteger(0);
                    similars.put(node, count);
                    checkCapacity(capacity, ++this.accessed,
                                  "fusiform similarity");
                }
                count.increase();
            }
        }
        // Delete source vertex
        assert similars.containsKey(vertex.id());
        similars.remove(vertex.id());
        if (similars.isEmpty()) {
            return ImmutableSet.of();
        }
        // Match alpha
        double neighborNum = neighbors.size();
        Map<Id, Double> matchedAlpha = new HashMap<>();
        for (Map.Entry<Id, MutableInteger> entry : similars.entrySet()) {
            double score = entry.getValue().value() / neighborNum;
            if (score >= alpha) {
                matchedAlpha.put(entry.getKey(), score);
            }
        }
        if (matchedAlpha.isEmpty()) {
            return ImmutableSet.of();
        }
        // Sorted and topN if needed
        Map<Id, Double> topN;
        if (top > 0) {
            topN = topN(matchedAlpha, true, top);
        } else {
            topN = matchedAlpha;
        }
        // Filter by groupCount by property
        if (groupProperty != null) {
            Set<Object> values = new HashSet<>();
            // Add groupProperty value of source vertex
            values.add(vertex.value(groupProperty));
            for (Id id : topN.keySet()) {
                Vertex v = graph().vertices(id).next();
                values.add(v.value(groupProperty));
            }
            if (values.size() < minGroupCount) {
                return ImmutableSet.of();
            }
        }
        // Construct result
        Set<Similar> result = InsertionOrderUtil.newSet();
        for (Map.Entry<Id, Double> entry : topN.entrySet()) {
            Id similar = entry.getKey();
            double score = entry.getValue();
            List<Id> inters = withIntermediary ?
                              intermediaries.get(similar) :
                              ImmutableList.of();
            result.add(new Similar(similar, score, inters));
        }
        return result;
    }

    private static void checkGroupArgs(String groupProperty,
                                       int minGroupCount) {
        if (groupProperty != null) {
            E.checkArgument(minGroupCount > 0,
                            "Must set min group count when " +
                            "group property set");
        }
    }

    private boolean matchMinNeighborCount(HugeVertex vertex,
                                          Directions direction,
                                          EdgeLabel edgeLabel,
                                          int minNeighborCount,
                                          long degree) {
        Iterator<Edge> edges;
        long neighborCount;
        Id labelId = edgeLabel == null ? null : edgeLabel.id();
        if (edgeLabel != null && edgeLabel.frequency() == Frequency.SINGLE) {
            edges = this.edgesOfVertex(vertex.id(), direction,
                                       labelId, minNeighborCount);
            neighborCount = IteratorUtils.count(edges);
        } else {
            edges = this.edgesOfVertex(vertex.id(), direction, labelId, degree);
            Set<Id> neighbors = new HashSet<>();
            while (edges.hasNext()) {
                Id target = ((HugeEdge) edges.next()).id().otherVertexId();
                neighbors.add(target);
                if (neighbors.size() >= minNeighborCount) {
                    break;
                }
            }
            neighborCount = neighbors.size();
        }
        return neighborCount >= minNeighborCount;
    }

    public static class Similar {

        private final Id id;
        private final double score;
        private final List<Id> intermediaries;

        public Similar(Id id, double score, List<Id> intermediaries) {
            this.id = id;
            this.score = score;
            assert new HashSet<>(intermediaries).size() ==
                   intermediaries.size() : "Invalid intermediaries";
            this.intermediaries = intermediaries;
        }

        public Id id() {
            return this.id;
        }

        public double score() {
            return this.score;
        }

        public List<Id> intermediaries() {
            return this.intermediaries;
        }

        public Map<String, Object> toMap() {
            return ImmutableMap.of("id", this.id, "score", this.score,
                                   "intermediaries", this.intermediaries);
        }
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

    public static class SimilarsMap extends HashMap<Id, Set<Similar>> {

        public Set<Id> vertices() {
            Set<Id> vertices = new HashSet<>();
            vertices.addAll(this.keySet());
            for (Set<Similar> similars : this.values()) {
                for (Similar similar : similars) {
                    vertices.add(similar.id());
                    vertices.addAll(similar.intermediaries());
                }
            }
            return vertices;
        }

        public Map<Id, Set<Map<String, Object>>> toMap() {
            Map<Id, Set<Map<String, Object>>> results = new HashMap<>();
            for (Map.Entry<Id, Set<Similar>> entry : this.entrySet()) {
                Id source = entry.getKey();
                Set<Similar> similars = entry.getValue();
                Set<Map<String, Object>> result = InsertionOrderUtil.newSet();
                for (Similar similar : similars) {
                    result.add(similar.toMap());
                }
                results.put(source, result);
            }
            return results;
        }
    }
}
