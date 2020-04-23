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

package com.baidu.hugegraph.auth;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.util.E;

public abstract class SchemaDefine {

    protected final HugeGraph graph;
    protected final String label;

    public SchemaDefine(HugeGraph graph, String label) {
        this.graph = graph;
        this.label = label;
    }

    public abstract void initSchemaIfNeeded();

    protected String createPropertyKey(String name) {
        return this.createPropertyKey(name, DataType.TEXT);
    }

    protected String createPropertyKey(String name, DataType dataType) {
        return this.createPropertyKey(name, dataType, Cardinality.SINGLE);
    }

    protected String createPropertyKey(String name, DataType dataType,
                                       Cardinality cardinality) {
        SchemaManager schema = this.graph.schema();
        PropertyKey propertyKey = schema.propertyKey(name)
                                        .dataType(dataType)
                                        .cardinality(cardinality)
                                        .build();
        this.graph.schemaTransaction().addPropertyKey(propertyKey);
        return name;
    }

    protected IndexLabel createRangeIndex(VertexLabel label, String field) {
        SchemaManager schema = this.graph.schema();
        String name = Hidden.hide(label + "-index-by-" + field);
        IndexLabel indexLabel = schema.indexLabel(name).range()
                                      .on(HugeType.VERTEX_LABEL, this.label)
                                      .by(field)
                                      .build();
        this.graph.schemaTransaction().addIndexLabel(label, indexLabel);
        return indexLabel;
    }

    public static abstract class Element {

        protected Id id;
        protected Date create;
        protected Date update;

        public Element() {
            this.create = new Date();
            this.update = this.create;
        }

        public Id id() {
            return this.id;
        }

        public void id(Id id) {
            this.id = id;
        }

        public Date create() {
            return this.create;
        }

        public void create(Date create) {
            this.create = create;
        }

        public Date update() {
            return this.update;
        }

        public void update(Date update) {
            this.update = update;
        }

        public void onUpdate() {
            this.update = new Date();
        }

        public abstract String label();
    }

    public static abstract class Entity extends Element {

        public abstract Map<String, Object> asMap();

        protected abstract void property(String key, Object value);
        protected abstract Object[] asArray();

        public static <T extends Entity> T fromVertex(Vertex vertex, T entity) {
            E.checkArgument(vertex.label().equals(entity.label()),
                            "Illegal vertex label '%s' for entity '%s'",
                            vertex.label(), entity.label());
            entity.id((Id) vertex.id());
            for (Iterator<VertexProperty<Object>> iter = vertex.properties();
                 iter.hasNext();) {
                VertexProperty<Object> prop = iter.next();
                entity.property(prop.key(), prop.value());
            }
            return entity;
        }
    }

    public static abstract class Relationship extends Element {

        public abstract String sourceLabel();
        public abstract String targetLabel();

        public abstract Id source();
        public abstract Id target();

        public abstract Map<String, Object> asMap();

        protected abstract void property(String key, Object value);
        protected abstract Object[] asArray();

        public static <T extends Relationship> T fromEdge(Edge edge,
                                                          T relationship) {
            E.checkArgument(edge.label().equals(relationship.label()),
                            "Illegal edge label '%s' for relationship '%s'",
                            edge.label(), relationship.label());
            relationship.id((Id) edge.id());
            for (Iterator<Property<Object>> iter = edge.properties();
                 iter.hasNext();) {
                Property<Object> prop = iter.next();
                relationship.property(prop.key(), prop.value());
            }
            return relationship;
        }
    }
}
