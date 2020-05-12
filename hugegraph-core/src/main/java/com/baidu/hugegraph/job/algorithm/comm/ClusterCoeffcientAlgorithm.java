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

package com.baidu.hugegraph.job.algorithm.comm;

import java.util.Map;

import com.baidu.hugegraph.job.Job;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.InsertionOrderUtil;

public class ClusterCoeffcientAlgorithm extends AbstractCommAlgorithm {

    @Override
    public String name() {
        return "cluster_coeffcient";
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        direction(parameters);
        degree(parameters);
    }

    @Override
    public Object call(Job<Object> job, Map<String, Object> parameters) {
        try (Traverser traverser = new Traverser(job)) {
            return traverser.clusterCoeffcient(direction(parameters),
                                               degree(parameters));
        }
    }

    private static class Traverser extends TriangleCountAlgorithm.Traverser {

        public Traverser(Job<Object> job) {
            super(job);
        }

        public Object clusterCoeffcient(Directions direction, long degree) {
            Map<String, Long> results = this.triangles(direction, degree);
            results = InsertionOrderUtil.newMap(results);

            long triangles = results.remove(KEY_TRIANGLES);
            long triads = results.remove(KEY_TRIADS);
            assert triangles <= triads;
            double coeffcient = triads == 0L ? 0d : 1d * triangles / triads;

            @SuppressWarnings({ "unchecked", "rawtypes" })
            Map<String, Double> converted = (Map) results;
            converted.put("cluster_coeffcient", coeffcient);

            return results;
        }
    }
}
