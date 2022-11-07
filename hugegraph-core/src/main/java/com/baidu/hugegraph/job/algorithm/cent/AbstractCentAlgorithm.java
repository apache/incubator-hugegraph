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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.job.Job;
import com.baidu.hugegraph.job.algorithm.AbstractAlgorithm;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.type.define.Directions;

public abstract class AbstractCentAlgorithm extends AbstractAlgorithm {

    @Override
    public String category() {
        return CATEGORY_CENT;
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        depth(parameters);
        degree(parameters);
        sample(parameters);
        direction(parameters);
        edgeLabel(parameters);
        sourceSample(parameters);
        sourceLabel(parameters);
        sourceCLabel(parameters);
        top(parameters);
    }

    protected static class Traverser extends AlgoTraverser {

        public Traverser(Job<Object> job) {
            super(job);
        }

        protected GraphTraversal<Vertex, Vertex> constructSource(
                                                 String sourceLabel,
                                                 long sourceSample,
                                                 String sourceCLabel) {
            GraphTraversal<Vertex, Vertex> t = this.graph().traversal()
                                                           .withSack(1f).V();

            if (sourceLabel != null) {
                t = t.hasLabel(sourceLabel);
            }

            t = t.filter(it -> {
                this.updateProgress(++this.progress);
                return sourceCLabel == null ? true :
                       match(it.get(), sourceCLabel);
            });

            if (sourceSample > 0L) {
                t = t.sample((int) sourceSample);
            }

            return t;
        }

        protected GraphTraversal<Vertex, Vertex> constructPath(
                  GraphTraversal<Vertex, Vertex> t, Directions dir,
                  String label, long degree, long sample,
                  String sourceLabel, String sourceCLabel) {
            GraphTraversal<?, Vertex> unit = constructPathUnit(dir, label,
                                                               degree, sample,
                                                               sourceLabel,
                                                               sourceCLabel);
            t = t.as("v").repeat(__.local(unit).simplePath().as("v"));

            return t;
        }

        protected GraphTraversal<Vertex, Vertex> constructPathUnit(
                                                 Directions dir, String label,
                                                 long degree, long sample,
                                                 String sourceLabel,
                                                 String sourceCLabel) {
            if (dir == null) {
                dir = Directions.BOTH;
            }
            Direction direction = dir.direction();

            String[] labels = {};
            if (label != null) {
                labels = new String[]{label};
            }

            GraphTraversal<Vertex, Vertex> unit = __.to(direction, labels);
            if (sourceLabel != null) {
                unit = unit.hasLabel(sourceLabel);
            }
            if (sourceCLabel != null) {
                unit = unit.has(C_LABEL, sourceCLabel);
            }
            if (degree != NO_LIMIT) {
                unit = unit.limit(degree);
            }
            if (sample > 0L) {
                unit = unit.sample((int) sample);
            }
            return unit;
        }

        protected <V> GraphTraversal<V, V> filterNonShortestPath(
                                           GraphTraversal<V, V> t,
                                           boolean keepOneShortestPath) {
            long size = this.graph().traversal().V().limit(100000L)
                                                    .count().next();
            Map<Pair<Id, Id>, Integer> triples = new HashMap<>((int) size);
            return t.filter(it -> {
                Id start = it.<HugeElement>path(Pop.first, "v").id();
                Id end = it.<HugeElement>path(Pop.last, "v").id();
                int len = it.<List<?>>path(Pop.all, "v").size();
                Pair<Id, Id> key = Pair.of(start, end);
                Integer shortest = triples.get(key);
                if (shortest != null && len > shortest) {
                    // ignore non shortest path
                    return false;
                }
                if (shortest == null) {
                    triples.put(key, len);
                } else {
                    assert len == shortest;
                    if (keepOneShortestPath) {
                        return false;
                    }
                }
                return true;
            });
        }

        protected GraphTraversal<Vertex, ?> topN(GraphTraversal<Vertex, ?> t,
                                                 long topN) {
            if (topN > 0L || topN == NO_LIMIT) {
                t = t.order(Scope.local).by(Column.values, Order.desc);
                if (topN > 0L) {
                    assert topN != NO_LIMIT;
                    t = t.limit(Scope.local, topN);
                }
            }
            return t;
        }
    }
}
