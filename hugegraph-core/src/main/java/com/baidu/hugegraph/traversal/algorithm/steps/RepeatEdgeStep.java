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

import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_MAX_DEGREE;

import java.util.List;
import java.util.Map;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.type.define.Directions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class RepeatEdgeStep extends EdgeStep {

    private int maxTimes = 1;

    public RepeatEdgeStep(HugeGraph g, Directions direction) {
        this(g, direction, ImmutableList.of());
    }

    public RepeatEdgeStep(HugeGraph g, Directions direction, int maxTimes) {
        this(g, direction);
        this.maxTimes = maxTimes;
    }

    public RepeatEdgeStep(HugeGraph g, List<String> labels) {
        this(g, Directions.BOTH, labels);
    }

    public RepeatEdgeStep(HugeGraph g, List<String> labels, int maxTimes) {
        this(g, labels);
        this.maxTimes = maxTimes;
    }

    public RepeatEdgeStep(HugeGraph g, Map<String, Object> properties) {
        this(g, Directions.BOTH, ImmutableList.of(), properties);
    }

    public RepeatEdgeStep(HugeGraph g, Map<String, Object> properties,
                          int maxTimes) {
        this(g, properties);
        this.maxTimes = maxTimes;
    }

    public RepeatEdgeStep(HugeGraph g, Directions direction,
                          List<String> labels) {
        this(g, direction, labels, ImmutableMap.of());
    }

    public RepeatEdgeStep(HugeGraph g, Directions direction,
                          List<String> labels, int maxTimes) {
        this(g, direction, labels);
        this.maxTimes = maxTimes;
    }

    public RepeatEdgeStep(HugeGraph g, Directions direction,
                          List<String> labels,
                          Map<String, Object> properties) {
        this(g, direction, labels, properties,
             Long.parseLong(DEFAULT_MAX_DEGREE), 0L, 1);
    }

    public RepeatEdgeStep(HugeGraph g, Directions direction,
                          List<String> labels,
                          Map<String, Object> properties, int maxTimes) {
        this(g, direction, labels, properties);
        this.maxTimes = maxTimes;
    }

    public RepeatEdgeStep(HugeGraph g, Directions direction,
                          List<String> labels,
                          Map<String, Object> properties, long degree,
                          long skipDegree, int maxTimes) {
        super(g, direction, labels, properties, degree, skipDegree);
        this.maxTimes = maxTimes;
    }

    public int remainTimes() {
        return this.maxTimes;
    }

    public void decreaseTimes() {
        this.maxTimes--;
    }

    public int maxTimes() {
        return this.maxTimes;
    }
}
