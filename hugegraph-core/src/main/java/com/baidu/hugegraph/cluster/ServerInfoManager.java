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
        E.checkArgument(role != null && !role.isEmpty(),
                        "The server role can't be null or empty");
        GraphRole graphRole = GraphRole.valueOf(role.toUpperCase());
        HugeServer hugeServer = new HugeServer(server, graphRole);
        this.serverId = hugeServer.id();
        this.serverRole = graphRole;
        this.save(hugeServer);
    }

    public Id serverId() {
        return this.serverId;
    }

    public GraphRole serverRole() {
        return this.serverRole;
    }

    public boolean master() {
        return this.serverRole != null && this.serverRole.master();
    }

    public void heartbeat() {
        HugeServer server = this.server();
        server.updateTime(DateUtil.now());
        this.save(server);
    }

    private void initSchemaIfNeeded() {
        HugeServer.schema(this.graph).initSchemaIfNeeded();
    }

    private GraphTransaction tx() {
        return this.graph.systemTransaction();
    }

    public Id save(HugeServer server) {
        // Construct vertex from task
        HugeVertex vertex = this.constructVertex(server);
        // Add or update user in backend store, stale index might exist
        vertex = this.tx().addVertex(vertex);
        this.commitOrRollback();
        return vertex.id();
    }

    private HugeVertex constructVertex(HugeServer server) {
        HugeServer.Schema schema = HugeServer.schema(this.graph);
        if (!schema.existVertexLabel(HugeServer.P.SERVER)) {
            throw new HugeException("Schema is missing for %s '%s'",
                                    HugeServer.P.SERVER, server);
        }
        return this.tx().constructVertex(false, server.asArray());
    }

    private void commitOrRollback() {
        this.tx().commitOrRollback();
    }

    public HugeServer server() {
        Iterator<Vertex> vertices = this.tx().queryVertices(this.serverId);
        Vertex vertex = QueryResults.one(vertices);
        if (vertex == null) {
            return null;
        }
        return HugeServer.fromVertex(vertex);
    }

    public Iterator<HugeServer> servers(long limit, String page) {
        return this.servers(ImmutableMap.of(), limit, page);
    }

    private Iterator<HugeServer> servers(Map<String, Object> conditions,
                                         long limit, String page) {
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        if (page != null) {
            query.page(page);
        }
        VertexLabel vl = this.graph.graph().vertexLabel(HugeServer.P.SERVER);
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
        Iterator<HugeServer> servers =
                new MapperIterator<>(vertices, HugeServer::fromVertex);
        // Convert iterator to list to avoid across thread tx accessed
        return QueryResults.toList(servers);
    }
}
