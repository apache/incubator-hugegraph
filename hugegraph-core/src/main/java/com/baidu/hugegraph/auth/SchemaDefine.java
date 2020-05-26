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
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.auth.HugeTarget.P;
import com.baidu.hugegraph.auth.ResourceObject.ResourceType;
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

    protected final HugeGraphParams graph;
    protected final String label;

    public SchemaDefine(HugeGraphParams graph, String label) {
        this.graph = graph;
        this.label = label;
    }

    public abstract void initSchemaIfNeeded();

    protected SchemaManager schema() {
         return this.graph.graph().schema();
    }

    protected boolean existVertexLabel(String label) {
        return this.graph.graph().existsVertexLabel(label);
    }

    protected boolean existEdgeLabel(String label) {
        return this.graph.graph().existsEdgeLabel(label);
    }

    protected String createPropertyKey(String name) {
        return this.createPropertyKey(name, DataType.TEXT);
    }

    protected String createPropertyKey(String name, DataType dataType) {
        return this.createPropertyKey(name, dataType, Cardinality.SINGLE);
    }

    protected String createPropertyKey(String name, DataType dataType,
                                       Cardinality cardinality) {
        SchemaManager schema = this.schema();
        PropertyKey propertyKey = schema.propertyKey(name)
                                        .dataType(dataType)
                                        .cardinality(cardinality)
                                        .build();
        this.graph.schemaTransaction().addPropertyKey(propertyKey);
        return name;
    }

    protected String[] initProperties(List<String> props) {
        String label = this.label;
        props.add(createPropertyKey(hideField(label, UserElement.CREATE),
                                    DataType.DATE));
        props.add(createPropertyKey(hideField(label, UserElement.UPDATE),
                                    DataType.DATE));
        props.add(createPropertyKey(hideField(label, UserElement.CREATOR)));

        return props.toArray(new String[0]);
    }

    protected IndexLabel createRangeIndex(VertexLabel label, String field) {
        SchemaManager schema = this.schema();
        String name = Hidden.hide(label + "-index-by-" + field);
        IndexLabel indexLabel = schema.indexLabel(name).range()
                                      .on(HugeType.VERTEX_LABEL, this.label)
                                      .by(field)
                                      .build();
        this.graph.schemaTransaction().addIndexLabel(label, indexLabel);
        return indexLabel;
    }

    protected static String hideField(String label, String key) {
        return label + "_" + key;
    }

    protected static String unhideField(String label, String key) {
        return Hidden.unHide(label) + "_" + key;
    }

    public static abstract class UserElement {

        protected static final String CREATE = "create";
        protected static final String UPDATE = "update";
        protected static final String CREATOR = "creator";

        protected Id id;
        protected Date create;
        protected Date update;
        protected String creator;

        public UserElement() {
            this.create = new Date();
            this.update = this.create;
        }

        public Id id() {
            return this.id;
        }

        public void id(Id id) {
            this.id = id;
        }

        public String idString() {
            return Hidden.unHide(this.label()) + "(" + this.id + ")";
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

        public String creator() {
            return this.creator;
        }

        public void creator(String creator) {
            this.creator = creator;
        }

        protected Map<String, Object> asMap(Map<String, Object> map) {
            E.checkState(this.create != null,
                         "Property %s can't be null", CREATE);
            E.checkState(this.update != null,
                         "Property %s can't be null", UPDATE);
            E.checkState(this.creator != null,
                         "Property %s can't be null", CREATOR);

            if (this.id != null) {
                // The id is null when creating
                map.put(Hidden.unHide(P.ID), this.id);
            }

            map.put(unhideField(this.label(), CREATE), this.create);
            map.put(unhideField(this.label(), UPDATE), this.update);
            map.put(unhideField(this.label(), CREATOR), this.creator);

            return map;
        }

        protected boolean property(String key, Object value) {
            E.checkNotNull(key, "property key");
            if (key.equals(hideField(this.label(), CREATE))) {
                this.create = (Date) value;
                return true;
            }
            if (key.equals(hideField(this.label(), UPDATE))) {
                this.update = (Date) value;
                return true;
            }
            if (key.equals(hideField(this.label(), CREATOR))) {
                this.creator = (String) value;
                return true;
            }
            return false;
        }

        protected Object[] asArray(List<Object> list) {
            E.checkState(this.create != null,
                         "Property %s can't be null", CREATE);
            E.checkState(this.update != null,
                         "Property %s can't be null", UPDATE);
            E.checkState(this.creator != null,
                         "Property %s can't be null", CREATOR);

            list.add(hideField(this.label(), CREATE));
            list.add(this.create);

            list.add(hideField(this.label(), UPDATE));
            list.add(this.update);

            list.add(hideField(this.label(), CREATOR));
            list.add(this.creator);

            return list.toArray();
        }

        public abstract ResourceType type();

        public abstract String label();

        public abstract Map<String, Object> asMap();

        protected abstract Object[] asArray();
    }

    public static abstract class Entity extends UserElement
                           implements com.baidu.hugegraph.type.Namifiable {

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

    public static abstract class Relationship extends UserElement {

        public abstract String sourceLabel();
        public abstract String targetLabel();

        public abstract Id source();
        public abstract Id target();

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
