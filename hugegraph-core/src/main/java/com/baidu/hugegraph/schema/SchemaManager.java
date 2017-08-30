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

import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.type.HugeType;

public class SchemaManager {

    private final SchemaTransaction transaction;

    public SchemaManager(SchemaTransaction transaction) {
        this.transaction = transaction;
    }

    public PropertyKey.Builder propertyKey(String name) {
        return new PropertyKey.Builder(name, this.transaction);
    }

    public PropertyKey.Builder propertyKey(PropertyKey propertyKey) {
        return new PropertyKey.Builder(propertyKey, this.transaction);
    }

    public VertexLabel.Builder vertexLabel(String name) {
        return new VertexLabel.Builder(name, this.transaction);
    }

    public VertexLabel.Builder vertexLabel(VertexLabel vertexLabel) {
        return new VertexLabel.Builder(vertexLabel, this.transaction);
    }

    public EdgeLabel.Builder edgeLabel(String name) {
        return new EdgeLabel.Builder(name, this.transaction);
    }

    public EdgeLabel.Builder edgeLabel(EdgeLabel edgeLabel) {
        return new EdgeLabel.Builder(edgeLabel, this.transaction);
    }

    public IndexLabel.Builder indexLabel(String name) {
        return new IndexLabel.Builder(name, this.transaction);
    }

    public IndexLabel.Builder indexLabel(IndexLabel indexLabel) {
        return new IndexLabel.Builder(indexLabel, this.transaction);
    }

    public PropertyKey getPropertyKey(String name) {
        PropertyKey propertyKey = this.transaction.getPropertyKey(name);
        checkExists(HugeType.PROPERTY_KEY, propertyKey, name);
        return propertyKey;
    }

    public VertexLabel getVertexLabel(String name) {
        VertexLabel vertexLabel = this.transaction.getVertexLabel(name);
        checkExists(HugeType.VERTEX_LABEL, vertexLabel, name);
        return vertexLabel;
    }

    public EdgeLabel getEdgeLabel(String name) {
        EdgeLabel edgeLabel = this.transaction.getEdgeLabel(name);
        checkExists(HugeType.EDGE_LABEL, edgeLabel, name);
        return edgeLabel;
    }

    public IndexLabel getIndexLabel(String name) {
        IndexLabel indexLabel = this.transaction.getIndexLabel(name);
        checkExists(HugeType.INDEX_LABEL, indexLabel, name);
        return indexLabel;
    }

    public List<PropertyKey> getPropertyKeys() {
        return this.transaction.getPropertyKeys();
    }

    public List<VertexLabel> getVertexLabels() {
        return this.transaction.getVertexLabels();
    }

    public List<EdgeLabel> getEdgeLabels() {
        return this.transaction.getEdgeLabels();
    }

    public List<IndexLabel> getIndexLabels() {
        return this.transaction.getIndexLabels();
    }

    public List<SchemaElement> desc() {
        List<SchemaElement> elements = new ArrayList<>();
        elements.addAll(this.getPropertyKeys());
        elements.addAll(this.getVertexLabels());
        elements.addAll(this.getEdgeLabels());
        elements.addAll(this.getIndexLabels());
        return elements;
    }

    public static void checkExists(HugeType type, Object object, String name) {
        if (object == null) {
            throw new NotFoundException("Not found the %s with name '%s'",
                                        type, name);
        }
    }
}
