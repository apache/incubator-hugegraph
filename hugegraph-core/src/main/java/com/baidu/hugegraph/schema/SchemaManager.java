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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.tinkerpop.gremlin.structure.Graph;

import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.schema.builder.EdgeLabelBuilder;
import com.baidu.hugegraph.schema.builder.IndexLabelBuilder;
import com.baidu.hugegraph.schema.builder.PropertyKeyBuilder;
import com.baidu.hugegraph.schema.builder.VertexLabelBuilder;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;

public class SchemaManager {

    private final SchemaTransaction transaction;

    public SchemaManager(final SchemaTransaction transaction) {
        E.checkNotNull(transaction, "transaction");
        this.transaction = transaction;
    }

    public PropertyKey.Builder propertyKey(String name) {
        return new PropertyKeyBuilder(name, this.transaction);
    }

    public VertexLabel.Builder vertexLabel(String name) {
        return new VertexLabelBuilder(name, this.transaction);
    }

    public EdgeLabel.Builder edgeLabel(String name) {
        return new EdgeLabelBuilder(name, this.transaction);
    }

    public IndexLabel.Builder indexLabel(String name) {
        return new IndexLabelBuilder(name, this.transaction);
    }

    public PropertyKey getPropertyKey(String name) {
        E.checkArgumentNotNull(name, "Name can't be null");
        PropertyKey propertyKey = this.transaction.getPropertyKey(name);
        checkExists(HugeType.PROPERTY_KEY, propertyKey, name);
        return propertyKey;
    }

    public VertexLabel getVertexLabel(String name) {
        E.checkArgumentNotNull(name, "Name can't be null");
        VertexLabel vertexLabel = this.transaction.getVertexLabel(name);
        checkExists(HugeType.VERTEX_LABEL, vertexLabel, name);
        return vertexLabel;
    }

    public EdgeLabel getEdgeLabel(String name) {
        E.checkArgumentNotNull(name, "Name can't be null");
        EdgeLabel edgeLabel = this.transaction.getEdgeLabel(name);
        checkExists(HugeType.EDGE_LABEL, edgeLabel, name);
        return edgeLabel;
    }

    public IndexLabel getIndexLabel(String name) {
        E.checkArgumentNotNull(name, "Name can't be null");
        IndexLabel indexLabel = this.transaction.getIndexLabel(name);
        checkExists(HugeType.INDEX_LABEL, indexLabel, name);
        return indexLabel;
    }

    public List<PropertyKey> getPropertyKeys() {
        return this.transaction.getPropertyKeys().stream()
                   .filter(pk -> !Graph.Hidden.isHidden(pk.name()))
                   .collect(Collectors.toList());
    }

    public List<VertexLabel> getVertexLabels() {
        return this.transaction.getVertexLabels().stream()
                   .filter(vl -> !Graph.Hidden.isHidden(vl.name()))
                   .collect(Collectors.toList());
    }

    public List<EdgeLabel> getEdgeLabels() {
        return this.transaction.getEdgeLabels().stream()
                   .filter(el -> !Graph.Hidden.isHidden(el.name()))
                   .collect(Collectors.toList());
    }

    public List<IndexLabel> getIndexLabels() {
        return this.transaction.getIndexLabels().stream()
                   .filter(il -> !Graph.Hidden.isHidden(il.name()))
                   .collect(Collectors.toList());
    }

    public List<SchemaElement> getAllSchema() {
        List<SchemaElement> elements = new ArrayList<>();
        elements.addAll(this.getPropertyKeys());
        elements.addAll(this.getVertexLabels());
        elements.addAll(this.getEdgeLabels());
        elements.addAll(this.getIndexLabels());
        return elements;
    }

    private static void checkExists(HugeType type, Object object, String name) {
        if (object == null) {
            throw new NotFoundException("%s with name '%s' does not exist",
                                        type, name);
        }
    }
}
