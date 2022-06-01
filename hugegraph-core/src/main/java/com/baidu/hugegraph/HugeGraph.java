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

package com.baidu.hugegraph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import com.alipay.remoting.rpc.RpcServer;
import com.baidu.hugegraph.auth.AuthManager;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendFeatures;
import com.baidu.hugegraph.backend.store.BackendStoreInfo;
import com.baidu.hugegraph.backend.store.raft.RaftGroupManager;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.TypedOption;
import com.baidu.hugegraph.rpc.RpcServiceConfig4Client;
import com.baidu.hugegraph.rpc.RpcServiceConfig4Server;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.schema.SchemaLabel;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeFeatures;
import com.baidu.hugegraph.task.TaskScheduler;
import com.baidu.hugegraph.traversal.optimize.HugeCountStepStrategy;
import com.baidu.hugegraph.traversal.optimize.HugeGraphStepStrategy;
import com.baidu.hugegraph.traversal.optimize.HugeVertexStepStrategy;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.GraphMode;
import com.baidu.hugegraph.type.define.GraphReadMode;
import com.baidu.hugegraph.type.define.NodeRole;

/**
 * Graph interface for Gremlin operations
 */
public interface HugeGraph extends Graph {

    HugeGraph hugegraph();

    SchemaManager schema();

    Id getNextId(HugeType type);

    Id addPropertyKey(PropertyKey key);

    Id removePropertyKey(Id key);

    Id clearPropertyKey(PropertyKey propertyKey);

    Collection<PropertyKey> propertyKeys();

    PropertyKey propertyKey(String key);

    PropertyKey propertyKey(Id key);

    boolean existsPropertyKey(String key);

    void addVertexLabel(VertexLabel vertexLabel);

    Id removeVertexLabel(Id label);

    Collection<VertexLabel> vertexLabels();

    VertexLabel vertexLabel(String label);

    VertexLabel vertexLabel(Id label);

    VertexLabel vertexLabelOrNone(Id id);

    boolean existsVertexLabel(String label);

    boolean existsLinkLabel(Id vertexLabel);

    void addEdgeLabel(EdgeLabel edgeLabel);

    Id removeEdgeLabel(Id label);

    Collection<EdgeLabel> edgeLabels();

    EdgeLabel edgeLabel(String label);

    EdgeLabel edgeLabel(Id label);

    EdgeLabel edgeLabelOrNone(Id label);

    boolean existsEdgeLabel(String label);

    void addIndexLabel(SchemaLabel schemaLabel, IndexLabel indexLabel);

    Id removeIndexLabel(Id label);

    Id rebuildIndex(SchemaElement schema);

    Collection<IndexLabel> indexLabels();

    IndexLabel indexLabel(String label);

    IndexLabel indexLabel(Id id);

    boolean existsIndexLabel(String label);

    @Override
    Vertex addVertex(Object... keyValues);

    void removeVertex(Vertex vertex);

    void removeVertex(String label, Object id);

    <V> void addVertexProperty(VertexProperty<V> property);

    <V> void removeVertexProperty(VertexProperty<V> property);

    Edge addEdge(Edge edge);

    void canAddEdge(Edge edge);

    void removeEdge(Edge edge);

    void removeEdge(String label, Object id);

    <V> void addEdgeProperty(Property<V> property);

    <V> void removeEdgeProperty(Property<V> property);

    Vertex vertex(Object object);

    @Override
    Iterator<Vertex> vertices(Object... objects);

    Iterator<Vertex> vertices(Query query);

    Iterator<Vertex> adjacentVertex(Object id);

    boolean checkAdjacentVertexExist();

    Edge edge(Object object);

    @Override
    Iterator<Edge> edges(Object... objects);

    Iterator<Edge> edges(Query query);

    Iterator<Vertex> adjacentVertices(Iterator<Edge> edges);

    Iterator<Edge> adjacentEdges(Id vertexId);

    Number queryNumber(Query query);

    String name();

    String backend();

    BackendFeatures backendStoreFeatures();

    BackendStoreInfo backendStoreInfo();

    GraphMode mode();

    void mode(GraphMode mode);

    GraphReadMode readMode();

    void readMode(GraphReadMode readMode);

    void waitReady(RpcServer rpcServer);

    void serverStarted(Id serverId, NodeRole serverRole);

    boolean started();

    boolean closed();

    <T> T metadata(HugeType type, String meta, Object... args);

    void initBackend();

    void clearBackend();

    void truncateBackend();

    void initSystemInfo();

    void createSnapshot();

    void resumeSnapshot();

    void create(String configPath, Id server, NodeRole role);

    void drop();

    HugeConfig cloneConfig(String newGraph);

    @Override
    HugeFeatures features();

    AuthManager authManager();

    void switchAuthManager(AuthManager authManager);

    TaskScheduler taskScheduler();

    RaftGroupManager raftGroupManager();

    void proxy(HugeGraph graph);

    boolean sameAs(HugeGraph graph);

    long now();

    <K, V> V option(TypedOption<K, V> option);

    void registerRpcServices(RpcServiceConfig4Server serverConfig,
                                    RpcServiceConfig4Client clientConfig);

    default List<String> mapPkId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.propertyKey(id);
            names.add(schema.name());
        }
        return names;
    }

    default List<String> mapVlId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.vertexLabel(id);
            names.add(schema.name());
        }
        return names;
    }

    default List<String> mapElId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.edgeLabel(id);
            names.add(schema.name());
        }
        return names;
    }

    default List<String> mapIlId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.indexLabel(id);
            names.add(schema.name());
        }
        return names;
    }

    default List<Id> mapPkName2Id(Collection<String> pkeys) {
        List<Id> ids = new ArrayList<>(pkeys.size());
        for (String pkey : pkeys) {
            PropertyKey propertyKey = this.propertyKey(pkey);
            ids.add(propertyKey.id());
        }
        return ids;
    }

    default Id[] mapElName2Id(String[] edgeLabels) {
        Id[] ids = new Id[edgeLabels.length];
        for (int i = 0; i < edgeLabels.length; i++) {
            EdgeLabel edgeLabel = this.edgeLabel(edgeLabels[i]);
            ids[i] = edgeLabel.id();
        }
        return ids;
    }

    default Id[] mapVlName2Id(String[] vertexLabels) {
        Id[] ids = new Id[vertexLabels.length];
        for (int i = 0; i < vertexLabels.length; i++) {
            VertexLabel vertexLabel = this.vertexLabel(vertexLabels[i]);
            ids[i] = vertexLabel.id();
        }
        return ids;
    }

    static void registerTraversalStrategies(Class<?> clazz) {
        TraversalStrategies strategies = null;
        strategies = TraversalStrategies.GlobalCache
                                        .getStrategies(Graph.class)
                                        .clone();
        strategies.addStrategies(HugeVertexStepStrategy.instance(),
                                 HugeGraphStepStrategy.instance(),
                                 HugeCountStepStrategy.instance());
        TraversalStrategies.GlobalCache.registerStrategies(clazz, strategies);
    }
}
