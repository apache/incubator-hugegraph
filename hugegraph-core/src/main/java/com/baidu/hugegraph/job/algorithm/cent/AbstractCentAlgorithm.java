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
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.job.Job;
import com.baidu.hugegraph.job.algorithm.AbstractAlgorithm;

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
        sourceSample(parameters);
        sourceLabel(parameters);
        sourceCLabel(parameters);
        top(parameters);
    }

    public static class Traverser extends AlgoTraverser {

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
                       match(it.get(), C_LABEL, sourceCLabel);
            });

            if (sourceSample > 0L) {
                t = t.sample((int) sourceSample);
            }

            return t;
        }

        protected GraphTraversal<Vertex, Vertex> constructPath(
                  GraphTraversal<Vertex, Vertex> t, long degree,
                  long sample, String sourceLabel, String sourceCLabel) {
            GraphTraversal<?, Vertex> unit = constructPathUnit(degree, sample,
                                                               sourceLabel,
                                                               sourceCLabel);
            t = t.as("v").repeat(__.local(unit).simplePath().as("v"));

            return t;
        }

        protected GraphTraversal<Vertex, Vertex> constructPathUnit(
                                                 long degree, long sample,
                                                 String sourceLabel,
                                                 String sourceCLabel) {
            GraphTraversal<Vertex, Vertex> unit = __.both();
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
    }
}
