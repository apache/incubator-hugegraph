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

package com.baidu.hugegraph.traversal.algorithm;

import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.NO_LIMIT;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Edge;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.traversal.optimize.TraversalUtil;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;

public class EdgeStep {

    protected Directions direction;
    protected final Map<Id, String> labels;
    protected final Map<Id, Object> properties;
    protected final long degree;
    protected final long skipDegree;

    public EdgeStep(HugeGraph g, Directions direction, List<String> labels,
                    Map<String, Object> properties,
                    long degree, long skipDegree) {
        E.checkArgument(degree == NO_LIMIT || degree > 0L,
                        "The degree must be > 0 or == -1, but got: %s",
                        degree);
        HugeTraverser.checkSkipDegree(skipDegree, degree,
                                      HugeTraverser.NO_LIMIT);
        this.direction = direction;

        // Parse edge labels
        Map<Id, String> labelIds = new HashMap<>();
        if (labels != null) {
            for (String label : labels) {
                EdgeLabel el = g.edgeLabel(label);
                labelIds.put(el.id(), label);
            }
        }
        this.labels = labelIds;

        // Parse properties
        if (properties == null || properties.isEmpty()) {
            this.properties = null;
        } else {
            this.properties = TraversalUtil.transProperties(g, properties);
        }

        this.degree = degree;
        this.skipDegree = skipDegree;
    }

    public Id[] edgeLabels() {
        int elsSize = this.labels.size();
        Id[] edgeLabels = this.labels.keySet().toArray(new Id[elsSize]);
        return edgeLabels;
    }

    public void swithDirection() {
        this.direction = this.direction.opposite();
    }

    public long limit() {
        long limit = this.skipDegree > 0L ? this.skipDegree : this.degree;
        return limit;
    }

    @Override
    public String toString() {
        return String.format("EdgeStep{direction=%s,labels=%s,properties=%s}",
                             this.direction, this.labels, this.properties);
    }

    public Iterator<Edge> skipSuperNodeIfNeeded(Iterator<Edge> edges) {
        return HugeTraverser.skipSuperNodeIfNeeded(edges, this.degree,
                                                   this.skipDegree);
    }
}
