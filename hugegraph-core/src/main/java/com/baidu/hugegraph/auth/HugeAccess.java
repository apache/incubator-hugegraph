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

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.T;

import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.auth.ResourceObject.ResourceType;
import com.baidu.hugegraph.auth.SchemaDefine.Relationship;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.util.E;

public class HugeAccess extends Relationship {

    private final Id group;
    private final Id target;
    private HugePermission permission;
    private String description;

    public HugeAccess(Id group, Id target) {
        this(group, target, null);
    }

    public HugeAccess(Id group, Id target, HugePermission permission) {
        this.group = group;
        this.target = target;
        this.permission = permission;
        this.description = null;
    }

    @Override
    public ResourceType type() {
        return ResourceType.GRANT;
    }

    @Override
    public String label() {
        return P.ACCESS;
    }

    @Override
    public String sourceLabel() {
        return P.GROUP;
    }

    @Override
    public String targetLabel() {
        return P.TARGET;
    }

    @Override
    public Id source() {
        return this.group;
    }

    @Override
    public Id target() {
        return this.target;
    }

    public HugePermission permission() {
        return this.permission;
    }

    public void permission(HugePermission permission) {
        this.permission = permission;
    }

    public String description() {
        return this.description;
    }

    public void description(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return String.format("HugeAccess(%s->%s)%s",
                             this.group, this.target, this.asMap());
    }

    @Override
    protected boolean property(String key, Object value) {
        if (super.property(key, value)) {
            return true;
        }
        switch (key) {
            case P.PERMISSION:
                this.permission = HugePermission.fromCode((Byte) value);
                break;
            case P.DESCRIPTION:
                this.description = (String) value;
                break;
            default:
                throw new AssertionError("Unsupported key: " + key);
        }
        return true;
    }

    @Override
    protected Object[] asArray() {
        E.checkState(this.permission != null,
                     "Access permission can't be null");

        List<Object> list = new ArrayList<>(12);

        list.add(T.label);
        list.add(P.ACCESS);

        list.add(P.PERMISSION);
        list.add(this.permission.code());

        if (this.description != null) {
            list.add(P.DESCRIPTION);
            list.add(this.description);
        }

        return super.asArray(list);
    }

    @Override
    public Map<String, Object> asMap() {
        E.checkState(this.permission != null,
                     "Access permission can't be null");

        Map<String, Object> map = new HashMap<>();

        map.put(Hidden.unHide(P.GROUP), this.group);
        map.put(Hidden.unHide(P.TARGET), this.target);

        map.put(Hidden.unHide(P.PERMISSION), this.permission.string());

        if (this.description != null) {
            map.put(Hidden.unHide(P.DESCRIPTION), this.description);
        }

        return super.asMap(map);
    }

    public static <V> HugeAccess fromEdge(Edge edge) {
        HugeAccess access = new HugeAccess((Id) edge.outVertex().id(),
                                           (Id) edge.inVertex().id());
        return fromEdge(edge, access);
    }

    public static Schema schema(HugeGraphParams graph) {
        return new Schema(graph);
    }

    public static final class P {

        public static final String ACCESS = Hidden.hide("access");

        public static final String LABEL = T.label.getAccessor();

        public static final String GROUP = HugeGroup.P.GROUP;
        public static final String TARGET = HugeTarget.P.TARGET;

        public static final String PERMISSION = "~access_permission";
        public static final String DESCRIPTION = "~access_description";

        public static String unhide(String key) {
            final String prefix = Hidden.hide("access_");
            if (key.startsWith(prefix)) {
                return key.substring(prefix.length());
            }
            return key;
        }
    }

    public static final class Schema extends SchemaDefine {

        public Schema(HugeGraphParams graph) {
            super(graph, P.ACCESS);
        }

        @Override
        public void initSchemaIfNeeded() {
            if (this.existEdgeLabel(this.label)) {
                return;
            }

            String[] properties = this.initProperties();

            // Create edge label
            EdgeLabel label = this.schema().edgeLabel(this.label)
                                  .sourceLabel(P.GROUP)
                                  .targetLabel(P.TARGET)
                                  .properties(properties)
                                  .nullableKeys(P.DESCRIPTION)
                                  .sortKeys(P.PERMISSION)
                                  .enableLabelIndex(true)
                                  .build();
            this.graph.schemaTransaction().addEdgeLabel(label);
        }

        private String[] initProperties() {
            List<String> props = new ArrayList<>();

            props.add(createPropertyKey(P.PERMISSION, DataType.BYTE));
            props.add(createPropertyKey(P.DESCRIPTION));

            return super.initProperties(props);
        }
    }
}
