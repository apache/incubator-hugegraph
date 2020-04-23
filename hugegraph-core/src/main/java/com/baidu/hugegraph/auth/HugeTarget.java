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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.auth.SchemaDefine.Entity;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.util.E;

public class HugeTarget extends Entity {

    private String name;
    private String url;

    public HugeTarget(Id id) {
        this(id, null, null);
    }

    public HugeTarget(String name, String url) {
        this(null, name, url);
    }

    public HugeTarget(Id id, String name, String url) {
        this.id = id;
        this.name = name;
        this.url = url;
    }

    @Override
    public String label() {
        return P.TARGET;
    }

    public String name() {
        return this.name;
    }

    public String url() {
        return this.url;
    }

    public void url(String url) {
        this.url = url;
    }

    @Override
    public String toString() {
        return String.format("HugeTarget(%s)%s", this.id, this.asMap());
    }

    @Override
    protected void property(String key, Object value) {
        E.checkNotNull(key, "property key");
        switch (key) {
            case P.NAME:
                this.name = (String) value;
                break;
            case P.URL:
                this.url = (String) value;
                break;
            case P.CREATE:
                this.create = (Date) value;
                break;
            case P.UPDATE:
                this.update = (Date) value;
                break;
            default:
                throw new AssertionError("Unsupported key: " + key);
        }
    }

    @Override
    protected Object[] asArray() {
        E.checkState(this.name != null, "Target name can't be null");
        E.checkState(this.url != null, "Target url can't be null");
        E.checkState(this.create != null, "Target create can't be null");
        E.checkState(this.update != null, "Target update can't be null");

        List<Object> list = new ArrayList<>(10);

        list.add(T.label);
        list.add(P.TARGET);

        list.add(P.NAME);
        list.add(this.name);

        list.add(P.URL);
        list.add(this.url);

        list.add(P.CREATE);
        list.add(this.create);

        list.add(P.UPDATE);
        list.add(this.update);

        return list.toArray();
    }

    @Override
    public Map<String, Object> asMap() {
        E.checkState(this.name != null, "Target name can't be null");
        E.checkState(this.url != null, "Target url can't be null");
        E.checkState(this.create != null, "Target create can't be null");
        E.checkState(this.update != null, "Target update can't be null");

        Map<String, Object> map = new HashMap<>();

        if (this.id != null) {
            // The id is null when creating target
            map.put(Hidden.unHide(P.ID), this.id);
        }

        map.put(Hidden.unHide(P.NAME), this.name);
        map.put(Hidden.unHide(P.URL), this.url);

        map.put(Hidden.unHide(P.CREATE), this.create);
        map.put(Hidden.unHide(P.UPDATE), this.update);

        return map;
    }

    public static HugeTarget fromVertex(Vertex vertex) {
        HugeTarget target = new HugeTarget((Id) vertex.id());
        return fromVertex(vertex, target);
    }

    public static Schema schema(HugeGraph graph) {
        return new Schema(graph);
    }

    public static final class P {

        public static final String TARGET = Hidden.hide("target");

        public static final String ID = T.id.getAccessor();
        public static final String LABEL = T.label.getAccessor();

        public static final String NAME = "~target_name";
        public static final String URL = "~target_url";
        public static final String CREATE = "~target_create";
        public static final String UPDATE = "~target_update";

        public static String unhide(String key) {
            final String prefix = Hidden.hide("target_");
            if (key.startsWith(prefix)) {
                return key.substring(prefix.length());
            }
            return key;
        }
    }

    public static final class Schema extends SchemaDefine {

        public Schema(HugeGraph graph) {
            super(graph, P.TARGET);
        }

        @Override
        public void initSchemaIfNeeded() {
            VertexLabel label = this.graph.schemaTransaction()
                                          .getVertexLabel(this.label);
            if (label != null) {
                return;
            }

            String[] properties = this.initProperties();

            // Create vertex label
            label = this.graph.schema().vertexLabel(this.label)
                              .properties(properties)
                              .usePrimaryKeyId()
                              .primaryKeys(P.NAME)
                              .enableLabelIndex(true)
                              .build();
            this.graph.schemaTransaction().addVertexLabel(label);

            // Create index
            this.createRangeIndex(label, P.UPDATE);
        }

        private String[] initProperties() {
            List<String> props = new ArrayList<>();

            props.add(createPropertyKey(P.NAME));
            props.add(createPropertyKey(P.URL));
            props.add(createPropertyKey(P.CREATE, DataType.DATE));
            props.add(createPropertyKey(P.UPDATE, DataType.DATE));

            return props.toArray(new String[0]);
        }
    }
}
