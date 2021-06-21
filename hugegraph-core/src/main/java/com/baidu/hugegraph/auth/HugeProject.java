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
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.auth.SchemaDefine.Entity;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.util.E;
import com.google.common.base.Strings;

public class HugeProject extends Entity {

    private String name;
    private String desc;
    private String adminGroupId;
    private String opGroupId;
    private Set<String> graphs;
    private String targetId;

    public HugeProject(Id id) {
        this(id, null, null, null, null, null, null);
    }

    public HugeProject(Id id, String name, String desc, String adminGroupId,
                       String opGroupId, Set<String> graphs,
                       String updateTargetId) {
        this.name = name;
        this.desc = desc;
        this.adminGroupId = adminGroupId;
        this.opGroupId = opGroupId;
        this.graphs = graphs;
        this.id = id;
        this.targetId = updateTargetId;
    }

    @Override
    public ResourceType type() {
        return ResourceType.PROJECT;
    }

    @Override
    public String label() {
        return P.PROJECT;
    }

    public String opGroupName() {
        return "op_" + this.name;
    }

    public String adminGroupName() {
        return "admin_" + this.name;
    }

    public String targetName() {
        return "target_auth_" + this.name;
    }

    public String description() {
        return this.desc;
    }

    public void description(String desc) {
        this.desc = desc;
    }

    public String adminGroupId() {
        return this.adminGroupId;
    }

    public void adminGroupId(String id) {
        this.adminGroupId = id;
    }

    public String opGroupId() {
        return this.opGroupId;
    }

    public void opGroupId(String id) {
        this.opGroupId = id;
    }

    public Set<String> graphs() {
        return graphs;
    }

    public void graphs(Set<String> graphs) {
        this.graphs = graphs;
    }

    public String targetId() {
        return this.targetId;
    }

    public void targetId(String targetId) {
        this.targetId = targetId;
    }

    @Override
    public Map<String, Object> asMap() {
        E.checkState(!Strings.isNullOrEmpty(this.name),
                     "Project name can't be null");
        E.checkState(!Strings.isNullOrEmpty(this.adminGroupId),
                     "Admin group id can't be null");
        E.checkState(!Strings.isNullOrEmpty(this.opGroupId),
                     "Op group id can't be null");

        Map<String, Object> map = new HashMap<>();

        map.put(Graph.Hidden.unHide(HugeProject.P.NAME), this.name);
        map.put(Graph.Hidden.unHide(HugeProject.P.ADMIN_GROUP),
                this.adminGroupId);
        map.put(Graph.Hidden.unHide(HugeProject.P.OP_GROUP),
                this.opGroupId);
        if (this.graphs != null && !this.graphs.isEmpty()) {
            map.put(Graph.Hidden.unHide(HugeProject.P.GRAPHS), this.graphs);
        }
        if (!Strings.isNullOrEmpty(this.desc)) {
            map.put(Graph.Hidden.unHide(HugeProject.P.DESCRIPTIONS),
                    this.desc);
        }
        if (!Strings.isNullOrEmpty(this.targetId)) {
            map.put(Graph.Hidden.unHide(HugeProject.P.TARGET),
                    this.targetId);
        }

        return super.asMap(map);
    }

    @Override
    protected Object[] asArray() {
        E.checkState(this.name != null, "Project name can't be null");
        E.checkState(this.adminGroupId != null,
                     "Admin group id can't be null");
        E.checkState(this.opGroupId != null,
                     "Op group id can't be null");

        List<Object> list = new ArrayList<>(16);

        list.add(T.label);
        list.add(HugeProject.P.PROJECT);

        list.add(HugeProject.P.NAME);
        list.add(this.name);

        if (!Strings.isNullOrEmpty(this.desc)) {
            list.add(HugeProject.P.DESCRIPTIONS);
            list.add(this.desc);
        }

        if (this.graphs != null && !this.graphs.isEmpty()) {
            list.add(HugeProject.P.GRAPHS);
            list.add(this.graphs);
        }

        list.add(HugeProject.P.ADMIN_GROUP);
        list.add(this.adminGroupId);

        list.add(HugeProject.P.OP_GROUP);
        list.add(this.opGroupId);

        if (!Strings.isNullOrEmpty(this.targetId)) {
            list.add(HugeProject.P.TARGET);
            list.add(this.targetId);
        }

        return super.asArray(list);
    }

    @Override
    protected boolean property(String key, Object value) {
        if (super.property(key, value)) {
            return true;
        }
        switch (key) {
            case HugeProject.P.NAME:
                this.name = (String) value;
                break;
            case HugeProject.P.GRAPHS:
                this.graphs = (Set<String>) value;
                break;
            case HugeProject.P.DESCRIPTIONS:
                this.desc = (String) value;
                break;
            case P.ADMIN_GROUP:
                this.adminGroupId = (String) value;
                break;
            case P.OP_GROUP:
                this.opGroupId = (String) value;
                break;
            case P.TARGET:
                this.targetId = (String) value;
                break;
            default:
                throw new AssertionError("Unsupported key: " + key);
        }
        return true;
    }

    public static HugeProject fromVertex(Vertex vertex) {
        HugeProject target = new HugeProject((Id) vertex.id());
        return fromVertex(vertex, target);
    }

    @Override
    public String name() {
        return this.name;
    }

    public static HugeProject.Schema schema(HugeGraphParams graph) {
        return new HugeProject.Schema(graph);
    }

    public static final class P {

        public static final String PROJECT = Graph.Hidden.hide("project");
        public static final String LABEL = T.label.getAccessor();
        public static final String ADMIN_GROUP = "~project_admin_group";
        public static final String OP_GROUP = "~project_op_group";
        public static final String GRAPHS = "~project_graphs";
        public static final String NAME = "~project_name";
        public static final String DESCRIPTIONS = "~project_description";
        public static final String TARGET = "~project_target";

        public static String unhide(String key) {
            final String prefix = Graph.Hidden.hide("project_");
            if (key.startsWith(prefix)) {
                return key.substring(prefix.length());
            }
            return key;
        }
    }

    public static final class Schema extends SchemaDefine {

        public Schema(HugeGraphParams graph) {
            super(graph, HugeProject.P.PROJECT);
        }

        @Override
        public void initSchemaIfNeeded() {
            if (this.existEdgeLabel(this.label)) {
                return;
            }

            String[] properties = this.initProperties();

            VertexLabel label = this.schema().vertexLabel(this.label)
                                    .enableLabelIndex(true)
                                    .usePrimaryKeyId()
                                    .primaryKeys(HugeProject.P.NAME)
                                    .nullableKeys(HugeProject.P.DESCRIPTIONS,
                                                  HugeProject.P.GRAPHS,
                                                  HugeProject.P.TARGET)
                                    .properties(properties)
                                    .build();
            this.graph.schemaTransaction().addVertexLabel(label);
        }

        private String[] initProperties() {
            List<String> props = new ArrayList<>();

            props.add(createPropertyKey(HugeProject.P.ADMIN_GROUP,
                                        DataType.TEXT));
            props.add(createPropertyKey(HugeProject.P.OP_GROUP,
                                        DataType.TEXT));
            props.add(createPropertyKey(HugeProject.P.GRAPHS, DataType.TEXT,
                                        Cardinality.SET));
            props.add(createPropertyKey(HugeProject.P.NAME, DataType.TEXT));
            props.add(createPropertyKey(HugeProject.P.DESCRIPTIONS,
                                        DataType.TEXT));
            props.add(createPropertyKey(HugeProject.P.TARGET,
                                        DataType.TEXT));

            return super.initProperties(props);
        }
    }
}
