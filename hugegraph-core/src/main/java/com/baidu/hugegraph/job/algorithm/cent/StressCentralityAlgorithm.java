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

package com.baidu.hugegraph.job.algorithm.cent;

import java.util.Map;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.job.UserJob;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.ParameterUtil;

public class StressCentralityAlgorithm extends AbstractCentAlgorithm {

    public static final String KEY_WITH_BOUNDARY = "with_boundary";

    @Override
    public String name() {
        return "stress_centrality";
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        super.checkParameters(parameters);
        withBoundary(parameters);
    }

    @Override
    public Object call(UserJob<Object> job, Map<String, Object> parameters) {
        try (Traverser traverser = new Traverser(job)) {
            return traverser.stressCentrality(direction(parameters),
                                              edgeLabel(parameters),
                                              depth(parameters),
                                              degree(parameters),
                                              sample(parameters),
                                              withBoundary(parameters),
                                              sourceLabel(parameters),
                                              sourceSample(parameters),
                                              sourceCLabel(parameters),
                                              top(parameters));
        }
    }

    protected static boolean withBoundary(Map<String, Object> parameters) {
        if (!parameters.containsKey(KEY_WITH_BOUNDARY)) {
            return false;
        }
        return ParameterUtil.parameterBoolean(parameters, KEY_WITH_BOUNDARY);
    }

    private static class Traverser extends AbstractCentAlgorithm.Traverser {

        public Traverser(UserJob<Object> job) {
            super(job);
        }

        public Object stressCentrality(Directions direction,
                                       String label,
                                       int depth,
                                       long degree,
                                       long sample,
                                       boolean withBoundary,
                                       String sourceLabel,
                                       long sourceSample,
                                       String sourceCLabel,
                                       long topN) {
            assert depth > 0;
            assert degree > 0L || degree == NO_LIMIT;
            assert topN >= 0L || topN == NO_LIMIT;

            GraphTraversal<Vertex, Vertex> t = constructSource(sourceLabel,
                                                               sourceSample,
                                                               sourceCLabel);
            t = constructPath(t, direction, label, degree, sample,
                              sourceLabel, sourceCLabel);
            t = t.emit().until(__.loops().is(P.gte(depth)));
            t = filterNonShortestPath(t, false);

            GraphTraversal<Vertex, ?> tg = this.substractPath(t, withBoundary)
                                               .groupCount();
            GraphTraversal<Vertex, ?> tLimit = topN(tg, topN);

            return this.execute(tLimit, tLimit::next);
        }
    }
}
