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

package com.baidu.hugegraph.traversal.algorithm.steps;

import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_DEGREE;
import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_SAMPLE;
import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.NO_LIMIT;

import java.util.List;
import java.util.Map;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class WeightedEdgeStep {

    private final EdgeStep edgeStep;
    private final PropertyKey weightBy;
    private final double defaultWeight;
    private final long sample;

    public WeightedEdgeStep(HugeGraph g, Directions direction) {
        this(g, direction, ImmutableList.of());
    }

    public WeightedEdgeStep(HugeGraph g, List<String> labels) {
        this(g, Directions.BOTH, labels);
    }

    public WeightedEdgeStep(HugeGraph g, Map<String, Object> properties) {
        this(g, Directions.BOTH, ImmutableList.of(), properties);
    }

    public WeightedEdgeStep(HugeGraph g, Directions direction, List<String> labels) {
        this(g, direction, labels, ImmutableMap.of());
    }

    public WeightedEdgeStep(HugeGraph g, Directions direction, List<String> labels,
                            Map<String, Object> properties) {
        this(g, direction, labels, properties,
             Long.valueOf(DEFAULT_DEGREE), 0L, null, 0.0D,
             Long.valueOf(DEFAULT_SAMPLE));
    }

    public WeightedEdgeStep(HugeGraph g, Directions direction, List<String> labels,
                            Map<String, Object> properties,
                            long degree, long skipDegree,
                            String weightBy, double defaultWeight, long sample) {
        E.checkArgument(sample > 0L || sample == NO_LIMIT,
                        "The sample must be > 0 or == -1, but got: %s",
                        sample);
        E.checkArgument(degree == NO_LIMIT || degree >= sample,
                        "Degree must be greater than or equal to sample," +
                        " but got degree %s and sample %s",
                        degree, sample);

        this.edgeStep = new EdgeStep(g, direction, labels, properties,
                                     degree, skipDegree);
        if (weightBy != null) {
            this.weightBy = g.propertyKey(weightBy);
        } else {
            this.weightBy = null;
        }
        this.defaultWeight = defaultWeight;
        this.sample = sample;
    }

    public EdgeStep step() {
        return this.edgeStep;
    }

    public PropertyKey weightBy() {
        return this.weightBy;
    }

    public double defaultWeight() {
        return this.defaultWeight;
    }

    public long sample() {
        return this.sample;
    }
}
