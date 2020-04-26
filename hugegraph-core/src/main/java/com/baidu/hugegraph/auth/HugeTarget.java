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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonProperty;
import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.auth.SchemaDefine.Entity;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.traversal.optimize.TraversalUtil;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;

import jersey.repackaged.com.google.common.collect.ImmutableList;

public class HugeTarget extends Entity {

    private String name;
    private String graph;
    private String url;
    private List<HugeResource> resources;

    private static final List<HugeResource> EMPTY = ImmutableList.of();

    public HugeTarget(Id id) {
        this(id, null, null, null, EMPTY);
    }

    public HugeTarget(String name, String url) {
        this(null, name, name, url, EMPTY);
    }

    public HugeTarget(String name, String graph, String url,
                      List<HugeResource> resources) {
        this(null, name, graph, url, resources);
    }

    private HugeTarget(Id id, String name, String graph, String url,
                       List<HugeResource> resources) {
        this.id = id;
        this.name = name;
        this.graph = graph;
        this.url = url;
        this.resources = resources;
    }

    @Override
    public String label() {
        return P.TARGET;
    }

    public String name() {
        return this.name;
    }

    public String graph() {
        return this.graph;
    }

    public String url() {
        return this.url;
    }

    public void url(String url) {
        this.url = url;
    }

    public List<HugeResource> resources() {
        return this.resources;
    }

    public void resources(String resources) {
        try {
            this.resources = HugeResource.parseResources(resources);
        } catch (Exception e) {
            throw new HugeException("Invalid format of resources: %s",
                                    e, resources);
        }
    }

    public void resources(List<HugeResource> resources) {
        this.resources = resources;
    }

    @Override
    public String toString() {
        return String.format("HugeTarget(%s)%s", this.id, this.asMap());
    }

    @Override
    protected boolean property(String key, Object value) {
        if (super.property(key, value)) {
            return true;
        }
        switch (key) {
            case P.NAME:
                this.name = (String) value;
                break;
            case P.GRAPH:
                this.graph = (String) value;
                break;
            case P.URL:
                this.url = (String) value;
                break;
            case P.RESS:
                this.resources = HugeResource.parseResources((String) value);
                break;
            default:
                throw new AssertionError("Unsupported key: " + key);
        }
        return true;
    }

    @Override
    protected Object[] asArray() {
        E.checkState(this.name != null, "Target name can't be null");
        E.checkState(this.url != null, "Target url can't be null");

        List<Object> list = new ArrayList<>(16);

        list.add(T.label);
        list.add(P.TARGET);

        list.add(P.NAME);
        list.add(this.name);

        list.add(P.GRAPH);
        list.add(this.graph);

        list.add(P.URL);
        list.add(this.url);

        if (this.resources != null && this.resources != EMPTY) {
            list.add(P.RESS);
            list.add(JsonUtil.toJson(this.resources));
        }

        return super.asArray(list);
    }

    @Override
    public Map<String, Object> asMap() {
        E.checkState(this.name != null, "Target name can't be null");
        E.checkState(this.url != null, "Target url can't be null");

        Map<String, Object> map = new HashMap<>();

        if (this.id != null) {
            // The id is null when creating target
            map.put(Hidden.unHide(P.ID), this.id);
        }

        map.put(Hidden.unHide(P.NAME), this.name);
        map.put(Hidden.unHide(P.GRAPH), this.graph);
        map.put(Hidden.unHide(P.URL), this.url);

        if (this.resources != null && this.resources != EMPTY) {
            map.put(Hidden.unHide(P.RESS), this.resources);
        }

        return super.asMap(map);
    }

    public static HugeTarget fromVertex(Vertex vertex) {
        HugeTarget target = new HugeTarget((Id) vertex.id());
        return fromVertex(vertex, target);
    }

    public static Schema schema(HugeGraphParams graph) {
        return new Schema(graph);
    }

    public static final class P {

        public static final String TARGET = Hidden.hide("target");

        public static final String ID = T.id.getAccessor();
        public static final String LABEL = T.label.getAccessor();

        public static final String NAME = "~target_name";
        public static final String GRAPH = "~target_graph";
        public static final String URL = "~target_url";
        public static final String RESS = "~target_resources";

        public static String unhide(String key) {
            final String prefix = Hidden.hide("target_");
            if (key.startsWith(prefix)) {
                return key.substring(prefix.length());
            }
            return key;
        }
    }

    public static final class Schema extends SchemaDefine {

        public Schema(HugeGraphParams graph) {
            super(graph, P.TARGET);
        }

        @Override
        public void initSchemaIfNeeded() {
            if (this.existVertexLabel(this.label)) {
                return;
            }

            String[] properties = this.initProperties();

            // Create vertex label
            VertexLabel label = this.schema().vertexLabel(this.label)
                                    .properties(properties)
                                    .usePrimaryKeyId()
                                    .primaryKeys(P.NAME)
                                    .nullableKeys(P.RESS)
                                    .enableLabelIndex(true)
                                    .build();
            this.graph.schemaTransaction().addVertexLabel(label);
        }

        private String[] initProperties() {
            List<String> props = new ArrayList<>();

            props.add(createPropertyKey(P.NAME));
            props.add(createPropertyKey(P.GRAPH));
            props.add(createPropertyKey(P.URL));
            props.add(createPropertyKey(P.RESS));

            return super.initProperties(props);
        }
    }

    public static class HugeResource {

        public static final String ANY = "*";

        @JsonProperty("type")
        private HugeType type;

        @JsonProperty("label")
        private String label;

        @JsonProperty("properties")
        private Map<String, String> properties; // value can be predicate

        public HugeResource() {
            // pass
        }

        public HugeResource(HugeType type, String label,
                            Map<String, String> properties) {
            this.type = type;
            this.label = label;
            this.properties = properties;
        }

        public boolean filter(HugeElement element) {
            if ( this.type != null) {
                if (this.type.isEdge()) {
                    if (!element.type().isEdge()) {
                        return false;
                    }
                } else if (this.type != element.type()) {
                    return false;
                }
            }

            if (this.label != null && !this.label.equals(element.label())) {
                return false;
            }
            if (this.properties == null) {
                return true;
            }
            for (Map.Entry<String, String> entry : this.properties.entrySet()) {
                String propName = entry.getKey();
                String expected = entry.getValue();
                if (propName.equals(ANY) && expected.equals(ANY)) {
                    return true;
                }
                Property<Object> prop = element.property(propName);
                if (!prop.isPresent()) {
                    return false;
                }
                Object actual = prop.value();
                if (expected.startsWith(TraversalUtil.P_CALL)) {
                    if (!TraversalUtil.parsePredicate(expected).test(actual)) {
                        return false;
                    }
                } else {
                    if (!expected.equals(actual)) {
                        return false;
                    }
                }
            }
            return true;
        }

        @Override
        public String toString() {
            return JsonUtil.toJson(this);
        }

        public static HugeResource parseResource(String resource) {
            return JsonUtil.fromJson(resource, HugeResource.class);
        }

        public static List<HugeResource> parseResources(String resources) {
            TypeReference<?> typeRef = new TypeReference<List<HugeResource>>() {};
            return JsonUtil.fromJson(resources, typeRef);
        }
    }
}
