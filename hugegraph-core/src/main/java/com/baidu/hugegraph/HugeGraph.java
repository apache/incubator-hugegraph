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

import com.baidu.hugegraph.auth.UserManager;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendFeatures;
import com.baidu.hugegraph.backend.store.BackendStoreSystemInfo;
import com.baidu.hugegraph.backend.store.raft.RaftGroupManager;
import com.baidu.hugegraph.config.ConfigOption;
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

    public HugeGraph hugegraph();

    public SchemaManager schema();

    public Id getNextId(HugeType type);

    public void addPropertyKey(PropertyKey key);
    public void removePropertyKey(Id key);
    public Collection<PropertyKey> propertyKeys();
    public PropertyKey propertyKey(String key);
    public PropertyKey propertyKey(Id key);
    public boolean existsPropertyKey(String key);

    public void addVertexLabel(VertexLabel vertexLabel);
    public Id removeVertexLabel(Id label);
    public Collection<VertexLabel> vertexLabels();
    public VertexLabel vertexLabel(String label);
    public VertexLabel vertexLabel(Id label);
    public VertexLabel vertexLabelOrNone(Id id);
    public boolean existsVertexLabel(String label);
    public boolean existsLinkLabel(Id vertexLabel);

    public void addEdgeLabel(EdgeLabel edgeLabel);
    public Id removeEdgeLabel(Id label);
    public Collection<EdgeLabel> edgeLabels();
    public EdgeLabel edgeLabel(String label);
    public EdgeLabel edgeLabel(Id label);
    public EdgeLabel edgeLabelOrNone(Id label);
    public boolean existsEdgeLabel(String label);

    public void addIndexLabel(SchemaLabel schemaLabel, IndexLabel indexLabel);
    public Id removeIndexLabel(Id label);
    public Id rebuildIndex(SchemaElement schema);
    public Collection<IndexLabel> indexLabels();
    public IndexLabel indexLabel(String label);
    public IndexLabel indexLabel(Id id);
    public boolean existsIndexLabel(String label);

    @Override
    public Vertex addVertex(Object... keyValues);
    public void removeVertex(Vertex vertex);
    public void removeVertex(String label, Object id);
    public <V> void addVertexProperty(VertexProperty<V> property);
    public <V> void removeVertexProperty(VertexProperty<V> property);

    public Edge addEdge(Edge edge);
    public void canAddEdge(Edge edge);
    public void removeEdge(Edge edge);
    public void removeEdge(String label, Object id);
    public <V> void addEdgeProperty(Property<V> property);
    public <V> void removeEdgeProperty(Property<V> property);

    public Vertex vertex(Object object);
    @Override
    public Iterator<Vertex> vertices(Object... objects);
    public Iterator<Vertex> vertices(Query query);
    public Iterator<Vertex> adjacentVertex(Object id);
    public boolean checkAdjacentVertexExist();

    public Edge edge(Object object);
    @Override
    public Iterator<Edge> edges(Object... objects);
    public Iterator<Edge> edges(Query query);
    public Iterator<Vertex> adjacentVertices(Iterator<Edge> edges) ;
    public Iterator<Edge> adjacentEdges(Id vertexId);

    public Number queryNumber(Query query);

    public String name();
    public String backend();
    public String backendVersion();
    public BackendStoreSystemInfo backendStoreSystemInfo();
    public BackendFeatures backendStoreFeatures();

    public GraphMode mode();
    public void mode(GraphMode mode);

    public GraphReadMode readMode();
    public void readMode(GraphReadMode readMode);

    public void waitStarted();
    public void serverStarted(Id serverId, NodeRole serverRole);
    public boolean started();
    public boolean closed();

    public <T> T metadata(HugeType type, String meta, Object... args);

    public void initBackend();
    public void clearBackend();
    public void truncateBackend();

    @Override
    public HugeFeatures features();

    public UserManager userManager();
    public void swichUserManager(UserManager userManager);
    public TaskScheduler taskScheduler();
    public RaftGroupManager raftGroupManager(String group);

    public void proxy(HugeGraph graph);

    public boolean sameAs(HugeGraph graph);

    public long now();

    public <V> V option(ConfigOption<V> option);

    public default List<String> mapPkId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.propertyKey(id);
            names.add(schema.name());
        }
        return names;
    }

    public default List<String> mapVlId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.vertexLabel(id);
            names.add(schema.name());
        }
        return names;
    }

    public default List<String> mapElId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.edgeLabel(id);
            names.add(schema.name());
        }
        return names;
    }

    public default List<String> mapIlId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.indexLabel(id);
            names.add(schema.name());
        }
        return names;
    }

    public default List<Id> mapPkName2Id(Collection<String> pkeys) {
        List<Id> ids = new ArrayList<>(pkeys.size());
        for (String pkey : pkeys) {
            PropertyKey propertyKey = this.propertyKey(pkey);
            ids.add(propertyKey.id());
        }
        return ids;
    }

    public default Id[] mapElName2Id(String[] edgeLabels) {
        Id[] ids = new Id[edgeLabels.length];
        for (int i = 0; i < edgeLabels.length; i++) {
            EdgeLabel edgeLabel = this.edgeLabel(edgeLabels[i]);
            ids[i] = edgeLabel.id();
        }
        return ids;
    }

    public default Id[] mapVlName2Id(String[] vertexLabels) {
        Id[] ids = new Id[vertexLabels.length];
        for (int i = 0; i < vertexLabels.length; i++) {
            VertexLabel vertexLabel = this.vertexLabel(vertexLabels[i]);
            ids[i] = vertexLabel.id();
        }
        return ids;
    }

    public static void registerTraversalStrategies(Class<?> clazz) {
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
