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

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.job.Job;

public class BetweenessCentralityAlgorithm extends AbstractCentAlgorithm {

    @Override
    public String name() {
        return "betweeness_centrality";
    }

    @Override
    public Object call(Job<Object> job, Map<String, Object> parameters) {
        Traverser traverser = new Traverser(job);
        return traverser.betweenessCentrality(depth(parameters),
                                              degree(parameters),
                                              sample(parameters),
                                              sourceLabel(parameters),
                                              sourceSample(parameters),
                                              sourceCLabel(parameters),
                                              top(parameters));
    }

    private static class Traverser extends AbstractCentAlgorithm.Traverser {

        public Traverser(Job<Object> job) {
            super(job);
        }

        public Object betweenessCentrality(int depth,
                                           long degree,
                                           long sample,
                                           String sourceLabel,
                                           long sourceSample,
                                           String sourceCLabel,
                                           long topN) {
            assert depth > 0;
            assert degree > 0L;
            assert topN >= 0L;

            GraphTraversal<Vertex, Vertex> t = constructSource(sourceLabel,
                                                               sourceSample,
                                                               sourceCLabel);
            t = constructPath(t, degree, sample, sourceLabel, sourceCLabel);
            t = t.emit().until(__.loops().is(P.gte(depth)));
            t = filterNonShortestPath(t);

            GraphTraversal<Vertex, ?> tg = t.select(Pop.all, "v")
                                            .unfold().id()
                                            .groupCount().order(Scope.local)
                                            .by(Column.values, Order.desc);
            GraphTraversal<Vertex, ?> tLimit = topN <= 0L ? tg :
                                               tg.limit(Scope.local, topN);

            return this.execute(tLimit, () -> tLimit.next());
        }
    }
}
