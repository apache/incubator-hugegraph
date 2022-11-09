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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.job.UserJob;
import com.baidu.hugegraph.type.define.Directions;

public class EigenvectorCentralityAlgorithm extends AbstractCentAlgorithm {

    public static final long DEFAULT_DEGREE = 100L;
    public static final long DEFAULT_SAMPLE = 1L;

    @Override
    public String name() {
        return "eigenvector_centrality";
    }

    @Override
    public Object call(UserJob<Object> job, Map<String, Object> parameters) {
        try (Traverser traverser = new Traverser(job)) {
            return traverser.eigenvectorCentrality(direction(parameters),
                                                   edgeLabel(parameters),
                                                   depth(parameters),
                                                   degree(parameters),
                                                   sample(parameters),
                                                   sourceLabel(parameters),
                                                   sourceSample(parameters),
                                                   sourceCLabel(parameters),
                                                   top(parameters));
        }
    }

    private static class Traverser extends AbstractCentAlgorithm.Traverser {

        public Traverser(UserJob<Object> job) {
            super(job);
        }

        public Object eigenvectorCentrality(Directions direction,
                                            String label,
                                            int depth,
                                            long degree,
                                            long sample,
                                            String sourceLabel,
                                            long sourceSample,
                                            String sourceCLabel,
                                            long topN) {
            assert depth > 0;
            assert degree > 0L || degree == NO_LIMIT;
            assert topN >= 0L || topN == NO_LIMIT;

            // TODO: support parameters: Directions dir, String label
            /*
             * g.V().repeat(groupCount('m').by(id)
             *              .local(both().limit(50).sample(1))
             *              .simplePath())
             *      .times(4).cap('m')
             *      .order(local).by(values, desc)
             *      .limit(local, 100)
             */

            GraphTraversal<Vertex, Vertex> t = constructSource(sourceLabel,
                                                               sourceSample,
                                                               sourceCLabel);
            GraphTraversal<?, Vertex> unit = constructPathUnit(direction, label,
                                                               degree, sample,
                                                               sourceLabel,
                                                               sourceCLabel);
            t = t.repeat(__.groupCount("m").by(T.id)
                           .local(unit).simplePath()).times(depth);

            GraphTraversal<Vertex, Object> tCap = t.cap("m");
            GraphTraversal<Vertex, ?> tLimit = topN(tCap, topN);

            return this.execute(tLimit, tLimit::next);
        }
    }
}
