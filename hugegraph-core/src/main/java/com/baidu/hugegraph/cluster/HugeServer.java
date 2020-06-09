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

package com.baidu.hugegraph.cluster;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.task.HugeTask;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.type.define.GraphRole;
import com.baidu.hugegraph.util.DateUtil;
import com.baidu.hugegraph.util.E;

public class HugeServer {

    public static final int MAX_LOAD = 1000;

    private Id id;
    private GraphRole role;
    private int load;
    private Date updateTime;

    public HugeServer(String name, GraphRole role) {
        this(IdGenerator.of(name), role);
    }

    public HugeServer(Id id) {
        this.id = id;
        this.load = 0;
        this.role = GraphRole.WORKER;
        this.updateTime = DateUtil.now();
    }

    public HugeServer(Id id, GraphRole role) {
        this.id = id;
        this.load = 0;
        this.role = role;
        this.updateTime = DateUtil.now();
    }

    public Id id() {
        return this.id;
    }

    public String name() {
        return this.id.asString();
    }

    public GraphRole role() {
        return this.role;
    }

    public void role(GraphRole role) {
        this.role = role;
    }

    public int load() {
        return this.load;
    }

    public void load(int load) {
        this.load = load;
    }

    public Date updateTime() {
        return this.updateTime;
    }

    public void updateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    @Override
    public String toString() {
        return String.format("HugeServer(%s)%s", this.id, this.asMap());
    }

    protected boolean property(String key, Object value) {
        switch (key) {
            case P.ROLE:
                this.role = GraphRole.valueOf((String) value);
                break;
            case P.LOAD:
                this.load = (int) value;
                break;
            case P.UPDATE_TIME:
                this.updateTime = (Date) value;
                break;
            default:
                throw new AssertionError("Unsupported key: " + key);
        }
        return true;
    }

    protected Object[] asArray() {
        E.checkState(this.id != null, "Server id can't be null");

        List<Object> list = new ArrayList<>(8);

        list.add(T.label);
        list.add(P.SERVER);

        list.add(T.id);
        list.add(this.id);

        list.add(P.ROLE);
        list.add(this.role.code());

        list.add(P.LOAD);
        list.add(this.load);

        list.add(P.UPDATE_TIME);
        list.add(this.updateTime);

        return list.toArray();
    }

    public Map<String, Object> asMap() {
        E.checkState(this.id != null, "Server id can't be null");

        Map<String, Object> map = new HashMap<>();

        map.put(Graph.Hidden.unHide(P.ID), this.id);
        map.put(Graph.Hidden.unHide(P.LABEL), P.SERVER);
        map.put(Graph.Hidden.unHide(P.ROLE), this.role);
        map.put(Graph.Hidden.unHide(P.LOAD), this.load);
        map.put(Graph.Hidden.unHide(P.UPDATE_TIME), this.updateTime);

        return map;
    }

    public static HugeServer fromVertex(Vertex vertex) {
        HugeServer server = new HugeServer((Id) vertex.id());
        for (Iterator<VertexProperty<Object>> iter = vertex.properties();
             iter.hasNext();) {
            VertexProperty<Object> prop = iter.next();
            server.property(prop.key(), prop.value());
        }
        return server;
    }

    public <V> boolean suitableFor(HugeTask<V> task, long now) {
        if (this.updateTime.getTime() + 5000L < now ||
            this.load() + task.load() > MAX_LOAD) {
            return false;
        }
        return true;
    }

    public static Schema schema(HugeGraphParams graph) {
        return new Schema(graph);
    }

    public static final class P {

        public static final String SERVER = Graph.Hidden.hide("server");

        public static final String ID = T.id.getAccessor();
        public static final String LABEL = T.label.getAccessor();

        public static final String NAME = "~server_name";
        public static final String ROLE = "~server_role";
        public static final String LOAD = "~server_load";
        public static final String UPDATE_TIME = "~server_update_time";

        public static String unhide(String key) {
            final String prefix = Graph.Hidden.hide("server_");
            if (key.startsWith(prefix)) {
                return key.substring(prefix.length());
            }
            return key;
        }
    }

    public static final class Schema {

        public static final String SERVER = P.SERVER;

        protected final HugeGraphParams graph;

        public Schema(HugeGraphParams graph) {
            this.graph = graph;
        }

        public void initSchemaIfNeeded() {
            if (this.existVertexLabel(SERVER)) {
                return;
            }

            HugeGraph graph = this.graph.graph();
            String[] properties = this.initProperties();

            // Create vertex label '~server'
            VertexLabel label = graph.schema().vertexLabel(SERVER)
                                     .properties(properties)
                                     .useCustomizeStringId()
                                     .nullableKeys(P.ROLE, P.LOAD,
                                                   P.UPDATE_TIME)
                                     .enableLabelIndex(true)
                                     .build();
            this.graph.schemaTransaction().addVertexLabel(label);

            // Create index
            this.createIndexLabel(label, P.ROLE);
            this.createIndexLabel(label, P.UPDATE_TIME);
        }

        private String[] initProperties() {
            List<String> props = new ArrayList<>();

            props.add(createPropertyKey(P.ROLE, DataType.BYTE));
            props.add(createPropertyKey(P.LOAD, DataType.INT));
            props.add(createPropertyKey(P.UPDATE_TIME, DataType.DATE));

            return props.toArray(new String[0]);
        }

        public boolean existVertexLabel(String label) {
            return this.graph.schemaTransaction()
                       .getVertexLabel(label) != null;
        }

        private String createPropertyKey(String name) {
            return this.createPropertyKey(name, DataType.TEXT);
        }

        private String createPropertyKey(String name, DataType dataType) {
            return this.createPropertyKey(name, dataType, Cardinality.SINGLE);
        }

        private String createPropertyKey(String name, DataType dataType,
                                         Cardinality cardinality) {
            SchemaManager schema = this.graph.graph().schema();
            PropertyKey propertyKey = schema.propertyKey(name)
                                            .dataType(dataType)
                                            .cardinality(cardinality)
                                            .build();
            this.graph.schemaTransaction().addPropertyKey(propertyKey);
            return name;
        }

        private IndexLabel createIndexLabel(VertexLabel label, String field) {
            SchemaManager schema = this.graph.graph().schema();
            String name = Graph.Hidden.hide("server-index-by-" + field);
            IndexLabel indexLabel = schema.indexLabel(name)
                                          .on(HugeType.VERTEX_LABEL, SERVER)
                                          .by(field)
                                          .build();
            this.graph.schemaTransaction().addIndexLabel(label, indexLabel);
            return indexLabel;
        }

        private IndexLabel indexLabel(String field) {
            String name = Graph.Hidden.hide("server-index-by-" + field);
            return this.graph.graph().indexLabel(name);
        }
    }
}
