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

import com.baidu.hugegraph.backend.id.IdGenerator;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.T;

import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.auth.SchemaDefine.Relationship;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.schema.EdgeLabel;

public class HugeBelong extends Relationship {

    private static final long serialVersionUID = -7242751631755533423L;

    private String graphSpace;
    private Id user;
    private Id group;
    private String description;

    public HugeBelong(String graphSpace, Id user, Id group) {
        this.graphSpace = graphSpace;
        this.user = user;
        this.group = group;
        this.description = null;
    }

    @Override
    public ResourceType type() {
        return ResourceType.GRANT;
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
    public String graphSpace() {
        return this.graphSpace;
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
        return String.format("HugeBelong(%s->%s)",
                             this.user, this.group);
    }

    @Override
    protected boolean property(String key, Object value) {
        if (super.property(key, value)) {
            return true;
        }
        switch (key) {
            case P.GRAPHSPACE:
                this.graphSpace = (String) value;
                break;
            case P.USER:
                this.user = IdGenerator.of((String) value);
                break;
            case P.GROUP:
                this.group = IdGenerator.of((String) value);
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
        List<Object> list = new ArrayList<>(10);

        list.add(T.label);
        list.add(P.BELONG);

        list.add(P.GRAPHSPACE);
        list.add(this.graphSpace);

        list.add(P.USER);
        list.add(this.user);

        list.add(P.GROUP);
        list.add(this.group);

        if (this.description != null) {
            list.add(P.DESCRIPTION);
            list.add(this.description);
        }

        return super.asArray(list);
    }

    @Override
    public Map<String, Object> asMap() {
        Map<String, Object> map = new HashMap<>();

        map.put(Hidden.unHide(P.GRAPHSPACE), this.graphSpace);
        map.put(Hidden.unHide(P.USER), this.user);
        map.put(Hidden.unHide(P.GROUP), this.group);

        if (this.description != null) {
            map.put(Hidden.unHide(P.DESCRIPTION), this.description);
        }

        return super.asMap(map);
    }

    public static HugeBelong fromMap(Map<String, Object> map) {
        HugeBelong belong = new HugeBelong("", null, null);
        return fromMap(map, belong);
    }

    public static Schema schema(HugeGraphParams graph) {
        return new Schema(graph);
    }

    public static final class P {

        public static final String BELONG = Hidden.hide("belong");

        public static final String LABEL = T.label.getAccessor();

        public static final String GRAPHSPACE = "~graphspace";

        public static final String USER = "~user";      //  HugeUser.P.USER;
        public static final String GROUP = "~group";     //HugeGroup.P.GROUP;

        public static final String DESCRIPTION = "~belong_description";

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

            return super.initProperties(props);
        }
    }
}
