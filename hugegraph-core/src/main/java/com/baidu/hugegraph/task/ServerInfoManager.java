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

package com.baidu.hugegraph.task;

import static com.baidu.hugegraph.backend.query.Query.NO_LIMIT;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.page.PageInfo;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.event.EventListener;
import com.baidu.hugegraph.exception.ConnectionException;
import com.baidu.hugegraph.iterator.ListIterator;
import com.baidu.hugegraph.iterator.MapperIterator;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.type.define.NodeRole;
import com.baidu.hugegraph.util.DateUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Events;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class ServerInfoManager {

    private static final Logger LOG = Log.logger(ServerInfoManager.class);

    public static final long MAX_SERVERS = 100000L;
    public static final long PAGE_SIZE = 10L;

    private final HugeGraphParams graph;
    private final ExecutorService dbExecutor;
    private final EventListener eventListener;

    private Id selfServerId;
    private NodeRole selfServerRole;

    private volatile boolean onlySingleNode;

    public ServerInfoManager(HugeGraphParams graph,
                             ExecutorService dbExecutor) {
        E.checkNotNull(graph, "graph");
        E.checkNotNull(dbExecutor, "db executor");

        this.graph = graph;
        this.dbExecutor = dbExecutor;

        this.eventListener = this.listenChanges();

        this.selfServerId = null;
        this.selfServerRole = null;
        this.onlySingleNode = false;
    }

    private EventListener listenChanges() {
        // Listen store event: "store.inited"
        Set<String> storeEvents = ImmutableSet.of(Events.STORE_INITED);
        EventListener eventListener = event -> {
            // Ensure server info schema create after system info initialized
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
        if (!this.dbExecutor.isShutdown()) {
            this.removeSelfServerInfo();
            this.call(() -> {
                try {
                    this.tx().close();
                } catch (ConnectionException ignored) {
                    // ConnectionException means no connection established
                }
                this.graph.closeTx();
                return null;
            });
        }
        return true;
    }

    public synchronized void initServerInfo(Id server, NodeRole role) {
        this.selfServerId = server;
        this.selfServerRole = role;

        HugeServerInfo existed = this.serverInfo(server);
        E.checkArgument(existed == null || !existed.alive(),
                        "The server with name '%s' already in cluster",
                        server);
        if (role.master()) {
            String page = this.supportsPaging() ? PageInfo.PAGE_NONE : null;
            do {
                Iterator<HugeServerInfo> servers = this.serverInfos(PAGE_SIZE,
                                                                    page);
                while (servers.hasNext()) {
                    existed = servers.next();
                    E.checkArgument(!existed.role().master() ||
                                    !existed.alive(),
                                    "Already existed master '%s' in current " +
                                    "cluster", existed.id());
                }
                if (page != null) {
                    page = PageInfo.pageInfo(servers);
                }
            } while (page != null);
        }

        HugeServerInfo serverInfo = new HugeServerInfo(server, role);
        serverInfo.maxLoad(this.calcMaxLoad());
        this.save(serverInfo);
    }

    public Id selfServerId() {
        return this.selfServerId;
    }

    public NodeRole selfServerRole() {
        return this.selfServerRole;
    }

    public boolean master() {
        return this.selfServerRole != null && this.selfServerRole.master();
    }

    public boolean onlySingleNode() {
        // Only has one master node
        return this.onlySingleNode;
    }

    public void heartbeat() {
        HugeServerInfo serverInfo = this.selfServerInfo();
        if (serverInfo == null) {
            return;
        }
        serverInfo.updateTime(DateUtil.now());
        this.save(serverInfo);
    }

    public void decreaseLoad(int load) {
        try {
            HugeServerInfo serverInfo = this.selfServerInfo();
            serverInfo.increaseLoad(-load);
            this.save(serverInfo);
        } catch (Throwable t) {
            LOG.error("Exception occurred when decrease server load", t);
        }
    }

    public int calcMaxLoad() {
        // TODO: calc max load based on CPU and Memory resources
        return 10000;
    }

    protected synchronized HugeServerInfo pickWorkerNode(
                                          Collection<HugeServerInfo> servers,
                                          HugeTask<?> task) {
        HugeServerInfo master = null;
        HugeServerInfo serverWithMinLoad = null;
        int minLoad = Integer.MAX_VALUE;
        boolean hasWorkerNode = false;
        long now = DateUtil.now().getTime();

        // Iterate servers to find suitable one
        for (HugeServerInfo server : servers) {
            if (!server.alive()) {
                continue;
            }

            if (server.role().master()) {
                master = server;
                continue;
            }

            hasWorkerNode = true;
            if (!server.suitableFor(task, now)) {
                continue;
            }
            if (server.load() < minLoad) {
                minLoad = server.load();
                serverWithMinLoad = server;
            }
        }

        this.onlySingleNode = !hasWorkerNode;

        // Only schedule to master if there is no workers and master is suitable
        if (!hasWorkerNode) {
            if (master != null && master.suitableFor(task, now)) {
                serverWithMinLoad = master;
            }
        }

        return serverWithMinLoad;
    }

    private void initSchemaIfNeeded() {
        HugeServerInfo.schema(this.graph).initSchemaIfNeeded();
    }

    private GraphTransaction tx() {
        assert Thread.currentThread().getName().contains("server-info-db-worker");
        return this.graph.systemTransaction();
    }

    private Id save(HugeServerInfo server) {
        return this.call(() -> {
            // Construct vertex from server info
            HugeServerInfo.Schema schema = HugeServerInfo.schema(this.graph);
            if (!schema.existVertexLabel(HugeServerInfo.P.SERVER)) {
                throw new HugeException("Schema is missing for %s '%s'",
                                        HugeServerInfo.P.SERVER, server);
            }
            HugeVertex vertex = this.tx().constructVertex(false,
                                                          server.asArray());
            // Add or update server info in backend store
            vertex = this.tx().addVertex(vertex);
            return vertex.id();
        });
    }

    private int save(Collection<HugeServerInfo> servers) {
        return this.call(() -> {
            if (servers.isEmpty()) {
                return servers.size();
            }
            HugeServerInfo.Schema schema = HugeServerInfo.schema(this.graph);
            if (!schema.existVertexLabel(HugeServerInfo.P.SERVER)) {
                throw new HugeException("Schema is missing for %s",
                                        HugeServerInfo.P.SERVER);
            }
            // Save server info in batch
            GraphTransaction tx = this.tx();
            int updated = 0;
            for (HugeServerInfo server : servers) {
                if (!server.updated()) {
                    continue;
                }
                HugeVertex vertex = tx.constructVertex(false, server.asArray());
                tx.addVertex(vertex);
                updated++;
            }
            // NOTE: actually it is auto-commit, to be improved
            tx.commitOrRollback();

            return updated;
        });
    }

    private <V> V call(Callable<V> callable) {
        assert !Thread.currentThread().getName().startsWith(
               "server-info-db-worker") : "can't call by itself";
        try {
            // Pass context for db thread
            callable = new TaskManager.ContextCallable<>(callable);
            // Ensure all db operations are executed in dbExecutor thread(s)
            return this.dbExecutor.submit(callable).get();
        } catch (Throwable e) {
            throw new HugeException("Failed to update/query server info: %s",
                                    e, e.toString());
        }
    }

    private HugeServerInfo selfServerInfo() {
        return this.serverInfo(this.selfServerId);
    }

    private HugeServerInfo serverInfo(Id server) {
        return this.call(() -> {
            Iterator<Vertex> vertices = this.tx().queryVertices(server);
            Vertex vertex = QueryResults.one(vertices);
            if (vertex == null) {
                return null;
            }
            return HugeServerInfo.fromVertex(vertex);
        });
    }

    private HugeServerInfo removeSelfServerInfo() {
        if (this.call(() -> this.graph.graph()
                                .backendStoreSystemInfo().exists())) {
            return this.removeServerInfo(this.selfServerId);
        }
        return null;
    }

    private HugeServerInfo removeServerInfo(Id server) {
        if (server == null) {
            return null;
        }
        return this.call(() -> {
            Iterator<Vertex> vertices = this.tx().queryVertices(server);
            Vertex vertex = QueryResults.one(vertices);
            if (vertex == null) {
                return null;
            }
            this.tx().removeVertex((HugeVertex) vertex);
            return HugeServerInfo.fromVertex(vertex);
        });
    }

    public void updateServerInfos(Collection<HugeServerInfo> serverInfos) {
        this.save(serverInfos);
    }

    public Collection<HugeServerInfo> allServerInfos() {
        @SuppressWarnings("resource")
        ListIterator<HugeServerInfo> iter = new ListIterator<>(MAX_SERVERS,
                                            this.serverInfos(NO_LIMIT, null));
        return iter.list();
    }

    public Iterator<HugeServerInfo> serverInfos(String page) {
        return this.serverInfos(ImmutableMap.of(), PAGE_SIZE, page);
    }

    public Iterator<HugeServerInfo> serverInfos(long limit, String page) {
        return this.serverInfos(ImmutableMap.of(), limit, page);
    }

    private Iterator<HugeServerInfo> serverInfos(Map<String, Object> conditions,
                                                 long limit, String page) {
        return this.call(() -> {
            ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
            if (page != null) {
                query.page(page);
            }

            HugeGraph graph = this.graph.graph();
            VertexLabel vl = graph.vertexLabel(HugeServerInfo.P.SERVER);
            query.eq(HugeKeys.LABEL, vl.id());
            for (Map.Entry<String, Object> entry : conditions.entrySet()) {
                PropertyKey pk = graph.propertyKey(entry.getKey());
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
        });
    }

    private boolean supportsPaging() {
        return this.graph.graph().backendStoreFeatures().supportsQueryByPage();
    }
}
