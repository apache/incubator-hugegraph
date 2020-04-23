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

package com.baidu.hugegraph.schema;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.baidu.hugegraph.GremlinGraph;
import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.Indexfiable;
import com.baidu.hugegraph.type.Propfiable;
import com.baidu.hugegraph.util.E;

public abstract class SchemaLabel extends SchemaElement
                                  implements Indexfiable, Propfiable {

    protected static final Id ZERO = IdGenerator.of(0L);
    protected static final String UNDEF = "~undefined";

    private final Set<Id> properties;
    private final Set<Id> nullableKeys;
    private final Set<Id> indexLabels;
    private boolean enableLabelIndex;

    public SchemaLabel(final HugeGraph graph, Id id, String name) {
        super(graph, id, name);
        this.properties = new HashSet<>();
        this.nullableKeys = new HashSet<>();
        this.indexLabels = new HashSet<>();
        this.enableLabelIndex = true;
    }

    @Override
    public Set<Id> properties() {
        return Collections.unmodifiableSet(this.properties);
    }

    public void properties(Set<Id> properties) {
        this.properties.addAll(properties);
    }

    public SchemaLabel properties(Id... ids) {
        this.properties.addAll(Arrays.asList(ids));
        return this;
    }

    public void property(Id id) {
        this.properties.add(id);
    }

    public Set<Id> nullableKeys() {
        return Collections.unmodifiableSet(this.nullableKeys);
    }

    public void nullableKey(Id id) {
        this.nullableKeys.add(id);
    }

    public void nullableKeys(Id... ids) {
        this.nullableKeys.addAll(Arrays.asList(ids));
    }

    public void nullableKeys(Set<Id> nullableKeys) {
        this.nullableKeys.addAll(nullableKeys);
    }

    @Override
    public Set<Id> indexLabels() {
        return Collections.unmodifiableSet(this.indexLabels);
    }

    public void indexLabel(Id id) {
        this.indexLabels.add(id);
    }

    public void indexLabels(Id... ids) {
        this.indexLabels.addAll(Arrays.asList(ids));
    }

    public void removeIndexLabel(Id id) {
        this.indexLabels.remove(id);
    }

    public boolean enableLabelIndex() {
        return this.enableLabelIndex;
    }

    public void enableLabelIndex(boolean enable) {
        this.enableLabelIndex = enable;
    }

    public boolean undefined() {
        return this.name() == UNDEF;
    }

    public static Id getLabelId(GremlinGraph graph, HugeType type, Object label) {
        E.checkNotNull(graph, "graph");
        E.checkNotNull(type, "type");
        E.checkNotNull(label, "label");
        if (label instanceof Number) {
            return IdGenerator.of(((Number) label).longValue());
        } else if (label instanceof String) {
            if (type.isVertex()) {
                return graph.vertexLabel((String) label).id();
            } else if (type.isEdge()) {
                return graph.edgeLabel((String) label).id();
            } else {
                throw new HugeException(
                          "Not support query from '%s' with label '%s'",
                          type, label);
            }
        } else {
            throw new HugeException(
                      "The label type must be number or string, but got '%s'",
                      label.getClass());
        }
    }
}
