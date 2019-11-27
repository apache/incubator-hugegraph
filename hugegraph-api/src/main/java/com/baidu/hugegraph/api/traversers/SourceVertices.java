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

package com.baidu.hugegraph.api.traversers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SourceVertices {

    @JsonProperty("ids")
    public Set<Object> ids;
    @JsonProperty("label")
    public String label;
    @JsonProperty("properties")
    public Map<String, Object> properties;

    public Iterator<Vertex> sourcesVertices(HugeGraph g) {
        Map<String, Object> props = this.properties;
        E.checkArgument(!((this.ids == null || this.ids.isEmpty()) &&
                        (props == null || props.isEmpty()) &&
                        this.label == null), "No source vertices provided");
        Iterator<Vertex> iterator;
        if (this.ids != null && !this.ids.isEmpty()) {
            List<Id> sourceIds = new ArrayList<>(this.ids.size());
            for (Object id : this.ids) {
                sourceIds.add(HugeVertex.getIdValue(id));
            }
            iterator = g.vertices(sourceIds.toArray());
            E.checkArgument(iterator.hasNext(),
                            "Not exist source vertices with ids %s",
                            this.ids);
        } else {
            ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
            if (this.label != null) {
                Id label = g.vertexLabel(this.label).id();
                query.eq(HugeKeys.LABEL, label);
            }
            if (props != null && !props.isEmpty()) {
                for (Map.Entry<String, Object> entry : props.entrySet()) {
                    Id pkeyId = g.propertyKey(entry.getKey()).id();
                    Object value = entry.getValue();
                    if (value instanceof Collection) {
                        query.query(Condition.in(pkeyId, (List<?>) value));
                    } else {
                        query.query(Condition.eq(pkeyId, value));
                    }
                }
            }
            assert !query.empty();
            iterator = g.vertices(query);
            E.checkArgument(iterator.hasNext(), "Not exist source vertex " +
                            "with label '%s' and properties '%s'",
                            this.label, props);
        }
        return iterator;
    }

    @Override
    public String toString() {
        return String.format("SourceVertex{ids=%s,label=%s,properties=%s}",
                             this.ids, this.label, this.properties);
    }
}
