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
import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.NO_LIMIT;
import static com.baidu.hugegraph.traversal.optimize.TraversalUtil.transProperties;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Edge;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableMap;

public class Steps {

    protected Directions direction;
    protected final Map<Id, StepEntity> edgeSteps;
    protected final Map<Id, StepEntity> vertexSteps;
    protected final long degree;
    protected final long skipDegree;

    public Steps(HugeGraph g, Directions direction) {
        this(g, direction, ImmutableMap.of(), ImmutableMap.of());
    }

    public Steps(HugeGraph g, Directions direction,
                 Map<String, Map<String, Object>> eSteps,
                 Map<String, Map<String, Object>> vSteps) {
        this(g, direction, eSteps, vSteps,
             Long.parseLong(DEFAULT_MAX_DEGREE), 0L);
    }

    public Steps(HugeGraph g, Directions direction,
                 Map<String, Map<String, Object>> eSteps,
                 Map<String, Map<String, Object>> vSteps,
                 long degree, long skipDegree) {
        E.checkArgument(degree == NO_LIMIT || degree > 0L,
                        "The max degree must be > 0 or == -1, but got: %s",
                        degree);
        HugeTraverser.checkSkipDegree(skipDegree, degree,
                                      HugeTraverser.NO_LIMIT);
        this.direction = direction;

        // Parse vertex steps
        vertexSteps = new HashMap<>();
        if (vSteps != null && !vSteps.isEmpty()) {
            initVertexFilter(g, vSteps);
        }

        // Parse edge steps
        edgeSteps = new HashMap<>();
        if (eSteps != null && !eSteps.isEmpty()) {
            initEdgeFilter(g, eSteps);
        }

        this.degree = degree;
        this.skipDegree = skipDegree;
    }

    private void initVertexFilter(HugeGraph g,
                                  Map<String, Map<String, Object>> vSteps) {
        for (Map.Entry<String, Map<String, Object>> entry : vSteps.entrySet()) {
            if (checkEntryEmpty(entry)) {
                continue;
            }
            E.checkArgument(entry.getKey() != null && !entry.getKey().isEmpty(),
                            "The vertex step label could not be null");

            VertexLabel vl = g.vertexLabel(entry.getKey());
            handleStepEnitiy(g, entry, vl.id(), vertexSteps);
        }
    }

    private void initEdgeFilter(HugeGraph g,
                                Map<String, Map<String, Object>> eSteps) {
        for (Map.Entry<String, Map<String, Object>> entry :
             eSteps.entrySet()) {
            if (checkEntryEmpty(entry)) {
                continue;
            }
            E.checkArgument(entry.getKey() != null && !entry.getKey().isEmpty(),
                            "The edge step label could not be null");

            EdgeLabel el = g.edgeLabel(entry.getKey());
            handleStepEnitiy(g, entry, el.id(), edgeSteps);
        }
    }

    private void handleStepEnitiy(HugeGraph g,
                                  Map.Entry<String, Map<String, Object>> entry,
                                  Id id, Map<Id, StepEntity> steps) {
        StepEntity stepEntity;
        if (entry.getValue() != null) {
            Map<Id, Object> props = transProperties(g, entry.getValue());
            stepEntity = new StepEntity(id, entry.getKey(), props);
        } else {
            stepEntity = new StepEntity(id, entry.getKey(), null);
        }
        steps.put(id, stepEntity);
    }

    private boolean checkEntryEmpty(
            Map.Entry<String, Map<String, Object>> entry) {
        return (entry.getKey() == null || entry.getKey().isEmpty()) &&
               (entry.getValue() == null || entry.getValue().isEmpty());
    }

    public Directions direction() {
        return this.direction;
    }

    public Map<Id, Steps.StepEntity> edgeSteps() {
        return this.edgeSteps;
    }

    public Map<Id, Steps.StepEntity> vertexSteps() {
        return this.vertexSteps;
    }

    public long degree() {
        return this.degree;
    }

    public long skipDegree() {
        return this.skipDegree;
    }

    public Id[] edgeLabels() {
        int elsSize = this.edgeSteps.size();
        return this.edgeSteps.keySet().toArray(new Id[elsSize]);
    }

    public boolean isEdgeStepPropertiesEmpty() {
        boolean result = true;
        if (this.edgeSteps == null || this.edgeSteps.isEmpty()) {
            return true;
        }

        for (Map.Entry<Id, StepEntity> entry : this.edgeSteps.entrySet()) {
            Map<Id, Object> props = entry.getValue().getProperties();
            if (props != null && !props.isEmpty()) {
                result = false;
                break;
            }
        }
        return result;
    }

    public boolean isVertexEmpty() {
        return this.vertexSteps == null || this.vertexSteps.isEmpty();
    }

    public void swithDirection() {
        this.direction = this.direction.opposite();
    }

    public long limit() {
        return this.skipDegree > 0L ? this.skipDegree : this.degree;
    }

    @Override
    public String toString() {
        return String.format("Steps{direction=%s,edgeSteps=%s," +
                             "vertexSteps=%s}", this.direction,
                             this.edgeSteps, this.vertexSteps);
    }

    public Iterator<Edge> skipSuperNodeIfNeeded(Iterator<Edge> edges) {
        return HugeTraverser.skipSuperNodeIfNeeded(edges, this.degree,
                                                   this.skipDegree);
    }

    public static class StepEntity {
        protected final Id id;
        protected final String label;
        protected final Map<Id, Object> properties;

        public StepEntity(Id id, String label, Map<Id, Object> properties) {
            this.id = id;
            this.label = label;
            this.properties = properties;
        }

        public Id getId() {
            return id;
        }

        public String getLabel() {
            return label;
        }

        public Map<Id, Object> getProperties() {
            return properties;
        }

        @Override
        public String toString() {
            return String.format("StepEntity{id=%s,label=%s," +
                                 "properties=%s}", this.id,
                                 this.label, this.properties);
        }
    }
}
