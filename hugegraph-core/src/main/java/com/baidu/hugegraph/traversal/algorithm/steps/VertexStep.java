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

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.traversal.optimize.TraversalUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VertexStep {

    protected final Map<Id, String> labels;
    protected final Map<Id, Object> properties;
    protected final Map<String, Object> keyProperties;

    public VertexStep(HugeGraph g) {
        this(g, ImmutableList.of());
    }

    public VertexStep(HugeGraph g, Map<String, Object> properties) {
        this(g, ImmutableList.of(), properties);
    }

    public VertexStep(HugeGraph g, List<String> labels) {
        this(g, labels, ImmutableMap.of());
    }

    public VertexStep(HugeGraph g, List<String> labels,
                      Map<String, Object> properties) {
        // Parse vertex labels
        Map<Id, String> labelIds = new HashMap<>();
        if (labels != null) {
            for (String label : labels) {
                VertexLabel vl = g.vertexLabel(label);
                labelIds.put(vl.id(), label);
            }
        }
        this.labels = labelIds;

        // Parse properties
        if (properties == null || properties.isEmpty()) {
            this.properties = null;
            this.keyProperties = null;
        } else {
            this.properties = TraversalUtil.transProperties(g, properties);
            this.keyProperties = properties;
        }
    }

    public Map<Id, String> labels() {
        return this.labels;
    }

    public Map<Id, Object> properties() {
        return this.properties;
    }

    public Map<String, Object> keyProperties() {
        return this.keyProperties;
    }

    public Id[] vertexLabels() {
        int vlsSize = this.labels.size();
        Id[] vertexLabels = this.labels.keySet().toArray(new Id[vlsSize]);
        return vertexLabels;
    }

    @Override
    public String toString() {
        return String.format("VertexStep{labels=%s,properties=%s}",
                             this.labels, this.properties);
    }
}
