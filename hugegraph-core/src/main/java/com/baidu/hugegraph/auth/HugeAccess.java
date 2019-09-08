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

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.T;

import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.auth.SchemaDefine.Relationship;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.util.E;

public class HugeAccess extends Relationship {

    private final Id group;
    private final Id target;
    private HugePermission permission;

    public HugeAccess(Id group, Id target) {
        this(group, target, null);
    }

    public HugeAccess(Id group, Id target, HugePermission permission) {
        this.group = group;
        this.target = target;
        this.permission = permission;
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


    @Override
    public String toString() {
        return String.format("HugeAccess(%s->%s)%s",
                             this.group, this.target, this.asMap());
    }

    @Override
    protected void property(String key, Object value) {
        E.checkNotNull(key, "property key");
        switch (key) {
            case P.PERMISSION:
                this.permission = HugePermission.fromCode((Byte) value);
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
        E.checkState(this.permission != null,
                     "Access permission can't be null");
        E.checkState(this.create != null, "Access create can't be null");
        E.checkState(this.update != null, "Access update can't be null");

        List<Object> list = new ArrayList<>(8);

        list.add(T.label);
        list.add(P.ACCESS);

        list.add(P.PERMISSION);
        list.add(this.permission.code());

        list.add(P.CREATE);
        list.add(this.create);

        list.add(P.UPDATE);
        list.add(this.update);

        return list.toArray();
    }

    @Override
    public Map<String, Object> asMap() {
        E.checkState(this.permission != null,
                     "Access permission can't be null");
        E.checkState(this.create != null, "Access create can't be null");
        E.checkState(this.update != null, "Access update can't be null");

        Map<String, Object> map = new HashMap<>();

        map.put(Hidden.unHide(P.GROUP), this.group);
        map.put(Hidden.unHide(P.TARGET), this.target);

        map.put(Hidden.unHide(P.PERMISSION), this.permission.string());
        map.put(Hidden.unHide(P.CREATE), this.create);
        map.put(Hidden.unHide(P.UPDATE), this.update);

        return map;
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
        public static final String CREATE = "~access_create";
        public static final String UPDATE = "~access_update";

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
                                  .sortKeys(P.PERMISSION)
                                  .enableLabelIndex(true)
                                  .build();
            this.graph.schemaTransaction().addEdgeLabel(label);
        }

        private String[] initProperties() {
            List<String> props = new ArrayList<>();

            props.add(createPropertyKey(P.PERMISSION, DataType.BYTE));
            props.add(createPropertyKey(P.CREATE, DataType.DATE));
            props.add(createPropertyKey(P.UPDATE, DataType.DATE));

            return props.toArray(new String[0]);
        }
    }
}
