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

public class HugeBelong extends Relationship {

    private final Id user;
    private final Id group;
    private String description;

    public HugeBelong(Id user, Id group) {
        this.user = user;
        this.group = group;
        this.description = null;
    }

    @Override
    public String label() {
        return P.BELONG;
    }

    @Override
    public String sourceLabel() {
        return P.USER;
    }

    @Override
    public String targetLabel() {
        return P.GROUP;
    }

    @Override
    public Id source() {
        return this.user;
    }

    @Override
    public Id target() {
        return this.group;
    }

    public String description() {
        return this.description;
    }

    public void description(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return String.format("HugeBelong(%s->%s)%s",
                             this.user, this.group, this.asMap());
    }

    @Override
    protected void property(String key, Object value) {
        E.checkNotNull(key, "property key");
        switch (key) {
            case P.DESCRIPTION:
                this.description = (String) value;
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
        E.checkState(this.create != null, "Belong create can't be null");
        E.checkState(this.update != null, "Belong update can't be null");

        List<Object> list = new ArrayList<>(8);

        list.add(T.label);
        list.add(P.BELONG);

        if (this.description != null) {
            list.add(P.DESCRIPTION);
            list.add(this.description);
        }

        list.add(P.CREATE);
        list.add(this.create);

        list.add(P.UPDATE);
        list.add(this.update);

        return list.toArray();
    }

    @Override
    public Map<String, Object> asMap() {
        E.checkState(this.create != null, "Belong create can't be null");
        E.checkState(this.update != null, "Belong update can't be null");

        Map<String, Object> map = new HashMap<>();

        map.put(Hidden.unHide(P.USER), this.user);
        map.put(Hidden.unHide(P.GROUP), this.group);

        if (this.description != null) {
            map.put(Hidden.unHide(P.DESCRIPTION), this.description);
        }
        map.put(Hidden.unHide(P.CREATE), this.create);
        map.put(Hidden.unHide(P.UPDATE), this.update);

        return map;
    }

    public static <V> HugeBelong fromEdge(Edge edge) {
        HugeBelong belong = new HugeBelong((Id) edge.outVertex().id(),
                                           (Id) edge.inVertex().id());
        return fromEdge(edge, belong);
    }

    public static Schema schema(HugeGraphParams graph) {
        return new Schema(graph);
    }

    public static final class P {

        public static final String BELONG = Hidden.hide("belong");

        public static final String LABEL = T.label.getAccessor();

        public static final String USER = HugeUser.P.USER;
        public static final String GROUP = HugeGroup.P.GROUP;

        public static final String DESCRIPTION = "~belong_description";
        public static final String CREATE = "~belong_create";
        public static final String UPDATE = "~belong_update";

        public static String unhide(String key) {
            final String prefix = Hidden.hide("belong_");
            if (key.startsWith(prefix)) {
                return key.substring(prefix.length());
            }
            return key;
        }
    }

    public static final class Schema extends SchemaDefine {

        public Schema(HugeGraphParams graph) {
            super(graph, P.BELONG);
        }

        @Override
        public void initSchemaIfNeeded() {
            if (this.existEdgeLabel(this.label)) {
                return;
            }

            String[] properties = this.initProperties();

            // Create edge label
            EdgeLabel label = this.schema().edgeLabel(this.label)
                                  .sourceLabel(P.USER)
                                  .targetLabel(P.GROUP)
                                  .properties(properties)
                                  .nullableKeys(P.DESCRIPTION)
                                  .enableLabelIndex(true)
                                  .build();
            this.graph.schemaTransaction().addEdgeLabel(label);
        }

        private String[] initProperties() {
            List<String> props = new ArrayList<>();

            props.add(createPropertyKey(P.DESCRIPTION));
            props.add(createPropertyKey(P.CREATE, DataType.DATE));
            props.add(createPropertyKey(P.UPDATE, DataType.DATE));

            return props.toArray(new String[0]);
        }
    }
}
