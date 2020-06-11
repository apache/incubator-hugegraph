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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.event.EventListener;
import com.baidu.hugegraph.iterator.MapperIterator;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.GraphRole;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.DateUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Events;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import static com.baidu.hugegraph.backend.page.PageInfo.PAGE_NONE;
import static com.baidu.hugegraph.backend.query.Query.NO_LIMIT;

public class ServerInfoManager {

    private final HugeGraphParams graph;
    private final EventListener eventListener;
    private Id serverId;
    private GraphRole serverRole;

    public ServerInfoManager(HugeGraphParams graph) {
        E.checkNotNull(graph, "graph");
        this.graph = graph;
        this.eventListener = this.listenChanges();
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
                    this.graph.closeTx();
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

    public void initServerInfo(String server, String role) {
        E.checkArgument(server != null && !server.isEmpty(),
                        "The server name can't be null or empty");
        HugeServerInfo existed = this.serverInfo(server);
        E.checkArgument(existed == null || !existed.alive(),
                        "The server with name '%s' already in cluster",
                        server);
        E.checkArgument(role != null && !role.isEmpty(),
                        "The server role can't be null or empty");
        GraphRole graphRole = GraphRole.valueOf(role.toUpperCase());
        if (graphRole.master()) {
            Iterator<HugeServerInfo> servers = this.serverInfos(ImmutableMap.of(
                                               HugeServerInfo.P.ROLE,
                                               GraphRole.MASTER.code()),
                                               1, PAGE_NONE);
            if (servers.hasNext()) {
                existed = servers.next();
                E.checkArgument(!existed.alive(),
                                "Already existed master '%s' in current " +
                                "cluster", existed.id());
            }
        }
        HugeServerInfo serverInfo = new HugeServerInfo(server, graphRole);
        this.serverId = serverInfo.id();
        this.serverRole = graphRole;
        this.save(serverInfo);
    }

    public Id serverId() {
        return this.serverId;
    }

    public GraphRole serverRole() {
        return this.serverRole;
    }

    public boolean master() {
        return this.serverRole() != null && this.serverRole().master();
    }

    public void heartbeat() {
        HugeServerInfo server = this.serverInfo();
        server.updateTime(DateUtil.now());
        this.save(server);
    }

    private void initSchemaIfNeeded() {
        HugeServerInfo.schema(this.graph).initSchemaIfNeeded();
    }

    private GraphTransaction tx() {
        return this.graph.systemTransaction();
    }

    public Id save(HugeServerInfo server) {
        // Construct vertex from task
        HugeVertex vertex = this.constructVertex(server);
        // Add or update user in backend store, stale index might exist
        vertex = this.tx().addVertex(vertex);
        this.commitOrRollback();
        return vertex.id();
    }

    private HugeVertex constructVertex(HugeServerInfo server) {
        HugeServerInfo.Schema schema = HugeServerInfo.schema(this.graph);
        if (!schema.existVertexLabel(HugeServerInfo.P.SERVER)) {
            throw new HugeException("Schema is missing for %s '%s'",
                                    HugeServerInfo.P.SERVER, server);
        }
        return this.tx().constructVertex(false, server.asArray());
    }

    private void commitOrRollback() {
        this.tx().commitOrRollback();
    }

    public HugeServerInfo serverInfo() {
        Iterator<Vertex> vertices = this.tx().queryVertices(this.serverId);
        Vertex vertex = QueryResults.one(vertices);
        if (vertex == null) {
            return null;
        }
        return HugeServerInfo.fromVertex(vertex);
    }

    public HugeServerInfo serverInfo(String name) {
        E.checkArgument(name != null && !name.isEmpty(),
                        "The server name can't be null or emtpy");
        Id server = IdGenerator.of(name);
        Iterator<Vertex> vertices = this.tx().queryVertices(server);
        Vertex vertex = QueryResults.one(vertices);
        if (vertex == null) {
            return null;
        }
        return HugeServerInfo.fromVertex(vertex);
    }

    public Iterator<HugeServerInfo> serverInfos(long limit, String page) {
        return this.serverInfos(ImmutableMap.of(), limit, page);
    }

    private Iterator<HugeServerInfo> serverInfos(Map<String, Object> conditions,
                                                 long limit, String page) {
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        if (page != null) {
            query.page(page);
        }
        VertexLabel vl = this.graph.graph().vertexLabel(HugeServerInfo.P.SERVER);


        query.eq(HugeKeys.LABEL, vl.id());
        for (Map.Entry<String, Object> entry : conditions.entrySet()) {
            PropertyKey pk = this.graph.graph().propertyKey(entry.getKey());
            query.query(Condition.eq(pk.id(), entry.getValue()));
        }
        query.showHidden(true);
        if (limit != NO_LIMIT) {
            query.limit(limit);
        }
        Iterator<Vertex> vertices = this.tx().queryVertices(query);
        Iterator<HugeServerInfo> servers =
                new MapperIterator<>(vertices, HugeServerInfo::fromVertex);
        // Convert iterator to list to avoid across thread tx accessed
        return QueryResults.toList(servers);
    }
}
