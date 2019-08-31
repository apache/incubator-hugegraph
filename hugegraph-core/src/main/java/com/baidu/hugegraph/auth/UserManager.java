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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.auth.HugeUser.P;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.event.EventListener;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.iterator.MapperIterator;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Events;
import com.baidu.hugegraph.util.JsonUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class UserManager {

    private final HugeGraph graph;
    private final EventListener eventListener;

    private static final long NO_LIMIT = -1L;

    public UserManager(HugeGraph graph) {
        E.checkNotNull(graph, "graph");

        this.graph = graph;
        this.eventListener = this.listenChanges();
    }

    public HugeGraph graph() {
        return this.graph;
    }

    private GraphTransaction tx() {
        return this.graph.systemTransaction();
    }

    private EventListener listenChanges() {
        // Listen store event: "store.inited"
        Set<String> storeEvents = ImmutableSet.of(Events.STORE_INITED);
        EventListener eventListener = event -> {
            // Ensure user schema create after system info initialized
            if (storeEvents.contains(event.name())) {
                try {
                    this.initSchemaIfNeeded();
                } finally {
                    this.graph().closeTx();
                }
                return true;
            }
            return false;
        };
        this.graph.loadSystemStore().provider().listen(eventListener);
        return eventListener;
    }

    private void unlistenChanges() {
        this.graph.loadSystemStore().provider().unlisten(this.eventListener);
    }

    public boolean close() {
        this.unlistenChanges();
        return true;
    }

    public Id createUser(HugeUser user) {
        E.checkArgumentNotNull(user, "User can't be null");
        return this.save(user);
    }

    public void updateUser(HugeUser user) {
        E.checkArgumentNotNull(user, "User can't be null");
        this.save(user);
    }

    public HugeUser deleteUser(Id id) {
        HugeUser user = null;
        Iterator<Vertex> vertices = this.tx().queryVertices(id);
        if (vertices.hasNext()) {
            HugeVertex vertex = (HugeVertex) vertices.next();
            user = HugeUser.fromVertex(vertex);
            this.tx().removeVertex(vertex);
            assert !vertices.hasNext();
        }
        return user;
    }

    public HugeUser matchUser(String name, String password) {
        E.checkArgumentNotNull(name, "User name can't be null");
        E.checkArgumentNotNull(password, "User password can't be null");
        HugeUser user = this.queryUser(name);
        if (user != null && user.password().equals(password)) {
            return user;
        }
        return null;
    }

    public HugeUser getUser(Id id) {
        HugeUser user = null;
        Iterator<Vertex> vertices = this.tx().queryVertices(id);
        if (vertices.hasNext()) {
            user = HugeUser.fromVertex(vertices.next());
            assert !vertices.hasNext();
        }
        if (user == null) {
            throw new NotFoundException("Can't find user with id '%s'", id);
        }
        return user;
    }

    public List<HugeUser> listUsers(List<Id> ids) {
        return this.queryUser(ids);
    }

    public List<HugeUser> listAllUsers(long limit) {
        return this.queryUser(ImmutableMap.of(), limit);
    }

    private HugeUser queryUser(String name) {
        List<HugeUser> users = this.queryUser(P.NAME, name, 2L);
        if (users.size() > 0) {
            assert users.size() == 1;
            return users.get(0);
        }
        return null;
    }

    private List<HugeUser> queryUser(String key, Object value, long limit) {
        return this.queryUser(ImmutableMap.of(key, value), limit);
    }

    private List<HugeUser> queryUser(Map<String, Object> conditions,
                                     long limit) {
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        VertexLabel vl = this.graph.vertexLabel(P.USER);
        query.eq(HugeKeys.LABEL, vl.id());
        for (Map.Entry<String, Object> entry : conditions.entrySet()) {
            PropertyKey pk = this.graph.propertyKey(entry.getKey());
            query.query(Condition.eq(pk.id(), entry.getValue()));
        }
        query.showHidden(true);
        if (limit != NO_LIMIT) {
            query.limit(limit);
        }
        Iterator<Vertex> vertices = this.tx().queryVertices(query);
        // Convert iterator to list to avoid across thread tx accessed
        return IteratorUtils.list(new MapperIterator<>(vertices,
                                                       HugeUser::fromVertex));
    }

    private List<HugeUser> queryUser(List<Id> ids) {
        Object[] idArray = ids.toArray(new Id[ids.size()]);
        Iterator<Vertex> vertices = this.tx().queryVertices(idArray);
        // Convert iterator to list to avoid across thread tx accessed
        return IteratorUtils.list(new MapperIterator<>(vertices,
                                                       HugeUser::fromVertex));
    }

    private Id save(HugeUser user) {
        // Construct vertex from task
        HugeVertex vertex = this.constructVertex(user);
        // Add or update user in backend store, stale index might exist
        return this.tx().addVertex(vertex).id();
    }

    private HugeVertex constructVertex(HugeUser user) {
        if (this.graph().schemaTransaction().getVertexLabel(P.USER) == null) {
            throw new HugeException("Schema is missing for user(%s) '%s'",
                                    user.id(), user.name());
        }
        return this.tx().constructVertex(false, user.asArray());
    }

    public String roleAction(HugeUser user) {
        // TODO: improve
        Object role = ImmutableMap.of("owners",
                                      ImmutableList.of("hugegraph", "hugegraph1"),
                                      "actions",
                                      ImmutableList.of(".*read", ".*write", "gremlin"));
        return JsonUtil.toJson(role);
    }

    public void initSchemaIfNeeded() {
        HugeGraph graph = this.graph();
        VertexLabel label = graph.schemaTransaction().getVertexLabel(P.USER);
        if (label != null) {
            return;
        }

        String[] properties = this.initProperties();

        // Create vertex label '~user'
        label = graph.schema().vertexLabel(P.USER)
                     .properties(properties)
                     .usePrimaryKeyId()
                     .primaryKeys(P.NAME)
                     .nullableKeys(P.PHONE, P.EMAIL, P.AVATAR)
                     .enableLabelIndex(true)
                     .build();
        graph.schemaTransaction().addVertexLabel(label);

        // Create index
        this.createIndex(label, P.UPDATE);
    }

    private String[] initProperties() {
        List<String> props = new ArrayList<>();

        props.add(createPropertyKey(P.NAME));
        props.add(createPropertyKey(P.PASSWORD));
        props.add(createPropertyKey(P.PHONE));
        props.add(createPropertyKey(P.EMAIL));
        props.add(createPropertyKey(P.AVATAR));
        props.add(createPropertyKey(P.CREATE, DataType.DATE));
        props.add(createPropertyKey(P.UPDATE, DataType.DATE));

        return props.toArray(new String[0]);
    }

    private String createPropertyKey(String name) {
        return this.createPropertyKey(name, DataType.TEXT);
    }

    private String createPropertyKey(String name, DataType dataType) {
        return this.createPropertyKey(name, dataType, Cardinality.SINGLE);
    }

    private String createPropertyKey(String name, DataType dataType,
                                     Cardinality cardinality) {
        HugeGraph graph = this.graph();
        SchemaManager schema = graph.schema();
        PropertyKey propertyKey = schema.propertyKey(name)
                                        .dataType(dataType)
                                        .cardinality(cardinality)
                                        .build();
        graph.schemaTransaction().addPropertyKey(propertyKey);
        return name;
    }

    private IndexLabel createIndex(VertexLabel label, String field) {
        HugeGraph graph = this.graph();
        SchemaManager schema = graph.schema();
        String name = Hidden.hide("task-index-by-" + field);
        IndexLabel indexLabel = schema.indexLabel(name)
                                      .on(HugeType.VERTEX_LABEL, P.USER)
                                      .by(field)
                                      .build();
        graph.schemaTransaction().addIndexLabel(label, indexLabel);
        return indexLabel;
    }
}
