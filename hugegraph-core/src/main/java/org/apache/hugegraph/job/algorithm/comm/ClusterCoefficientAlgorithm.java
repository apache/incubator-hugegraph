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

package org.apache.hugegraph.job.algorithm.comm;

import java.util.Map;

import org.apache.hugegraph.job.UserJob;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.InsertionOrderUtil;

public class ClusterCoefficientAlgorithm extends AbstractCommAlgorithm {

    public static final String ALGO_NAME = "cluster_coefficient";

    @Override
    public String name() {
        return ALGO_NAME;
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        direction(parameters);
        degree(parameters);
        workersWhenBoth(parameters);
    }

    @Override
    public Object call(UserJob<Object> job, Map<String, Object> parameters) {
        int workers = workersWhenBoth(parameters);
        try (Traverser traverser = new Traverser(job, workers)) {
            return traverser.clusterCoefficient(direction(parameters), degree(parameters));
        }
    }

    protected static int workersWhenBoth(Map<String, Object> parameters) {
        Directions direction = direction(parameters);
        int workers = workers(parameters);
        E.checkArgument(direction == Directions.BOTH || workers <= 0,
                        "The workers must be not set when direction!=BOTH, " +
                        "but got workers=%s and direction=%s",
                        workers, direction);
        return workers;
    }

    private static class Traverser extends TriangleCountAlgorithm.Traverser {

        public Traverser(UserJob<Object> job, int workers) {
            super(job, ALGO_NAME, workers);
        }

        public Object clusterCoefficient(Directions direction, long degree) {
            Map<String, Long> results = this.triangles(direction, degree);
            results = InsertionOrderUtil.newMap(results);

            long triangles = results.remove(KEY_TRIANGLES);
            long triads = results.remove(KEY_TRIADS);
            assert triangles <= triads;
            double coefficient = triads == 0L ? 0d : 1d * triangles / triads;

            @SuppressWarnings({ "unchecked", "rawtypes" })
            Map<String, Double> converted = (Map) results;
            converted.put("cluster_coefficient", coefficient);

            return results;
        }
    }
}
