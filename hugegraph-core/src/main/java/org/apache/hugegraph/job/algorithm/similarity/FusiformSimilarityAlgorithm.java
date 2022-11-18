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

package org.apache.hugegraph.job.algorithm.similarity;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.job.UserJob;
import org.apache.hugegraph.job.algorithm.AbstractAlgorithm;
import org.apache.hugegraph.job.algorithm.Consumers.StopExecution;
import org.apache.hugegraph.traversal.algorithm.FusiformSimilarityTraverser;
import org.apache.hugegraph.traversal.algorithm.FusiformSimilarityTraverser.SimilarsMap;
import org.apache.hugegraph.traversal.algorithm.HugeTraverser;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.hugegraph.util.ParameterUtil;

public class FusiformSimilarityAlgorithm extends AbstractAlgorithm {

    public static final String ALGO_NAME = "fusiform_similarity";

    public static final String KEY_MIN_NEIGHBORS = "min_neighbors";
    public static final String KEY_MIN_SIMILARS = "min_similars";
    public static final String KEY_TOP_SIMILARS = "top_similars";
    public static final String KEY_GROUP_PROPERTY = "group_property";
    public static final String KEY_MIN_GROUPS = "min_groups";

    public static final int DEFAULT_MIN_NEIGHBORS = 10;
    public static final int DEFAULT_MIN_SIMILARS = 6;
    public static final int DEFAULT_TOP_SIMILARS = 0;
    public static final int DEFAULT_MIN_GROUPS = 0;

    @Override
    public String category() {
        return CATEGORY_SIMI;
    }

    @Override
    public String name() {
        return ALGO_NAME;
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        minNeighbors(parameters);
        alpha(parameters);
        minSimilars(parameters);
        topSimilars(parameters);
        groupProperty(parameters);
        minGroups(parameters);
        degree(parameters);
        limit(parameters);
        sourceLabel(parameters);
        sourceCLabel(parameters);
        direction(parameters);
        edgeLabel(parameters);
        workers(parameters);
    }

    @Override
    public Object call(UserJob<Object> job, Map<String, Object> parameters) {
        int workers = workers(parameters);
        try (Traverser traverser = new Traverser(job, workers)) {
            return traverser.fusiformSimilars(sourceLabel(parameters),
                                              sourceCLabel(parameters),
                                              direction(parameters),
                                              edgeLabel(parameters),
                                              minNeighbors(parameters),
                                              alpha(parameters),
                                              minSimilars(parameters),
                                              topSimilars(parameters),
                                              groupProperty(parameters),
                                              minGroups(parameters),
                                              degree(parameters),
                                              limit(parameters));
        }
    }

    protected static int minNeighbors(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_MIN_NEIGHBORS)) {
            return DEFAULT_MIN_NEIGHBORS;
        }
        int minNeighbors = ParameterUtil.parameterInt(parameters,
                                                      KEY_MIN_NEIGHBORS);
        HugeTraverser.checkPositive(minNeighbors, KEY_MIN_NEIGHBORS);
        return minNeighbors;
    }

    protected static int minSimilars(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_MIN_SIMILARS)) {
            return DEFAULT_MIN_SIMILARS;
        }
        int minSimilars = ParameterUtil.parameterInt(parameters,
                                                     KEY_MIN_SIMILARS);
        HugeTraverser.checkPositive(minSimilars, KEY_MIN_SIMILARS);
        return minSimilars;
    }

    protected static int topSimilars(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_TOP_SIMILARS)) {
            return DEFAULT_TOP_SIMILARS;
        }
        int minSimilars = ParameterUtil.parameterInt(parameters,
                                                     KEY_TOP_SIMILARS);
        HugeTraverser.checkNonNegative(minSimilars, KEY_TOP_SIMILARS);
        return minSimilars;
    }

    protected static String groupProperty(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_GROUP_PROPERTY)) {
            return null;
        }
        return ParameterUtil.parameterString(parameters, KEY_GROUP_PROPERTY);
    }

    protected static int minGroups(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_MIN_GROUPS)) {
            return DEFAULT_MIN_GROUPS;
        }
        int minGroups = ParameterUtil.parameterInt(parameters, KEY_MIN_GROUPS);
        HugeTraverser.checkPositive(minGroups, KEY_MIN_GROUPS);
        return minGroups;
    }

    protected static long limit(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_LIMIT)) {
            return DEFAULT_LIMIT;
        }
        long limit = ParameterUtil.parameterLong(parameters, KEY_LIMIT);
        HugeTraverser.checkLimit(limit);
        return limit;
    }

    private static class Traverser extends AlgoTraverser {

        public Traverser(UserJob<Object> job, int workers) {
            super(job, ALGO_NAME, workers);
        }

        public Object fusiformSimilars(String sourceLabel, String sourceCLabel,
                                       Directions direction, String label,
                                       int minNeighbors, double alpha,
                                       int minSimilars, long topSimilars,
                                       String groupProperty, int minGroups,
                                       long degree, long limit) {
            HugeGraph graph = this.graph();

            FusiformSimilarityTraverser traverser =
                                        new FusiformSimilarityTraverser(graph);

            AtomicLong count = new AtomicLong(0L);
            JsonMap similarsJson = new JsonMap();
            similarsJson.startObject();

            this.traverse(sourceLabel, sourceCLabel, v -> {
                SimilarsMap similars = traverser.fusiformSimilarity(
                                       IteratorUtils.of(v), direction,
                                       label, minNeighbors, alpha,
                                       minSimilars, (int) topSimilars,
                                       groupProperty, minGroups, degree,
                                       MAX_CAPACITY, NO_LIMIT, true);
                if (similars.isEmpty()) {
                    return;
                }
                String result = JsonUtil.toJson(similars.toMap());
                result = result.substring(1, result.length() - 1);
                synchronized (similarsJson) {
                    if (count.incrementAndGet() > limit && limit != NO_LIMIT) {
                        throw new StopExecution("exceed limit %s", limit);
                    }
                    similarsJson.appendRaw(result);
                }
            });

            similarsJson.endObject();

            return similarsJson.asJson();
        }
    }
}
