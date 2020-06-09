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

package com.baidu.hugegraph.job.algorithm.similarity;

import java.util.Map;

import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.job.Job;
import com.baidu.hugegraph.job.algorithm.AbstractAlgorithm;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.traversal.algorithm.FusiformSimilarityTraverser;
import com.baidu.hugegraph.traversal.algorithm.FusiformSimilarityTraverser.SimilarsMap;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.JsonUtil;

public class FusiformSimilarityAlgorithm extends AbstractAlgorithm {

    public static final String KEY_MIN_NEIGHBORS = "min_neighbors";
    public static final String KEY_MIN_SIMILARS = "min_similars";
    public static final String KEY_GROUP_PROPERTY = "group_property";
    public static final String KEY_MIN_GROUPS = "min_groups";

    public static final int DEFAULT_MIN_NEIGHBORS = 10;
    public static final int DEFAULT_MIN_SIMILARS = 6;
    public static final int DEFAULT_MIN_GROUPS = 1;
    public static final long DEFAULT_LIMIT = -1L;

    @Override
    public String name() {
        return "fusiform_similarity";
    }

    @Override
    public String category() {
        return CATEGORY_SIMI;
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        minNeighbors(parameters);
        alpha(parameters);
        minSimilars(parameters);
        top(parameters);
        groupProperty(parameters);
        minGroups(parameters);
        degree(parameters);
        capacity(parameters);
        limit(parameters);
        sourceLabel(parameters);
        sourceCLabel(parameters);
        direction(parameters);
        edgeLabel(parameters);
        workers(parameters);
    }

    @Override
    public Object call(Job<Object> job, Map<String, Object> parameters) {
        int workers = workers(parameters);
        try (Traverser traverser = new Traverser(job, workers)) {
            return traverser.fusiformSimilars(sourceLabel(parameters),
                                              sourceCLabel(parameters),
                                              direction(parameters),
                                              edgeLabel(parameters),
                                              minNeighbors(parameters),
                                              alpha(parameters),
                                              minSimilars(parameters),
                                              top(parameters),
                                              groupProperty(parameters),
                                              minGroups(parameters),
                                              degree(parameters),
                                              capacity(parameters),
                                              limit(parameters));
        }
    }

    protected static int minNeighbors(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_MIN_NEIGHBORS)) {
            return DEFAULT_MIN_NEIGHBORS;
        }
        int minNeighbors = parameterInt(parameters, KEY_MIN_NEIGHBORS);
        HugeTraverser.checkPositive(minNeighbors, "min neighbors");
        return minNeighbors;
    }

    protected static int minSimilars(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_MIN_SIMILARS)) {
            return DEFAULT_MIN_SIMILARS;
        }
        int minSimilars = parameterInt(parameters, KEY_MIN_SIMILARS);
        HugeTraverser.checkPositive(minSimilars, "min similars");
        return minSimilars;
    }

    protected static String groupProperty(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_GROUP_PROPERTY)) {
            return null;
        }
        return parameterString(parameters, KEY_GROUP_PROPERTY);
    }

    protected static int minGroups(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_MIN_GROUPS)) {
            return DEFAULT_MIN_GROUPS;
        }
        int minGroups = parameterInt(parameters, KEY_MIN_GROUPS);
        HugeTraverser.checkPositive(minGroups, "min groups");
        return minGroups;
    }

    protected static long limit(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_LIMIT)) {
            return DEFAULT_LIMIT;
        }
        long limit = parameterLong(parameters, KEY_LIMIT);
        HugeTraverser.checkLimit(limit);
        return limit;
    }

    private static class Traverser extends AlgoTraverser {

        public Traverser(Job<Object> job, int workers) {
            super(job, "fusiform", workers);
        }

        public Object fusiformSimilars(String sourceLabel, String sourceCLabel,
                                       Directions direction, String label,
                                       int minNeighbors, double alpha,
                                       int minSimilars, long topSimilars,
                                       String groupProperty, int minGroups,
                                       long degree, long capacity, long limit) {
            HugeGraph graph = this.graph();
            EdgeLabel edgeLabel = label == null ? null : graph.edgeLabel(label);

            FusiformSimilarityTraverser traverser =
                                        new FusiformSimilarityTraverser(graph);
            JsonMap similarsJson = new JsonMap();
            similarsJson.startObject();

            this.traverse(sourceLabel, sourceCLabel, v -> {
                SimilarsMap similars = traverser.fusiformSimilarity(
                                       IteratorUtils.of(v), direction,
                                       edgeLabel, minNeighbors, alpha,
                                       minSimilars, (int) topSimilars,
                                       groupProperty, minGroups, degree,
                                       capacity, NO_LIMIT, true);
                if (similars.isEmpty()) {
                    return;
                }
                String result = JsonUtil.toJson(similars.toMap());
                result = result.substring(1, result.length() - 1);
                synchronized (similarsJson) {
                    similarsJson.appendRaw(result);
                }
            }, null, limit);
            similarsJson.endObject();

            return similarsJson.asJson();
        }
    }
}
