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
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeFeatures;
import com.baidu.hugegraph.task.TaskScheduler;
import com.baidu.hugegraph.traversal.optimize.HugeGraphStepStrategy;
import com.baidu.hugegraph.traversal.optimize.HugeVertexStepStrategy;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.GraphMode;

/**
 * Graph interface for Gremlin operations
 */
public interface HugeGraph extends Graph {

    public HugeGraph hugegraph();
    public HugeGraph hugegraph(String permission);

    public SchemaManager schema();

    public Id getNextId(HugeType type);

    public PropertyKey propertyKey(String key);
    public PropertyKey propertyKey(Id key);

    public VertexLabel vertexLabel(String label);
    public VertexLabel vertexLabel(Id label);

    public EdgeLabel edgeLabel(String label);
    public EdgeLabel edgeLabel(Id label);

    public IndexLabel indexLabel(String name);
    public IndexLabel indexLabel(Id id);

    @Override
    public Vertex addVertex(Object... keyValues);
    public void removeVertex(Vertex vertex);
    public <V> void addVertexProperty(VertexProperty<V> property);
    public <V> void removeVertexProperty(VertexProperty<V> property);

    public Edge addEdge(Edge edge);
    public void removeEdge(Edge edge);
    public <V> void addEdgeProperty(Property<V> property);
    public <V> void removeEdgeProperty(Property<V> property);

    public Iterator<Vertex> vertices(Query query);
    public Iterator<Edge> edges(Query query);
    public Iterator<Vertex> adjacentVertices(Iterator<Edge> edges) ;
    public Iterator<Edge> adjacentEdges(Id vertexId);

    public String name();
    public String backend();
    public String backendVersion();
    public BackendStoreSystemInfo backendStoreSystemInfo();
    public BackendFeatures backendStoreFeatures();

    public GraphMode mode();
    public void mode(GraphMode mode);

    public boolean closed();

    public <T> T metadata(HugeType type, String meta, Object... args);

    public void initBackend();
    public void clearBackend();
    public void truncateBackend();

    @Override
    public HugeFeatures features();

    public String matchUser(String username, String password);

    public UserManager userManager();
    public TaskScheduler taskScheduler();

    public void proxy(HugeGraph graph);

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
        TraversalStrategies strategies = TraversalStrategies.GlobalCache
                                         .getStrategies(Graph.class).clone();
        strategies.addStrategies(HugeVertexStepStrategy.instance(),
                                 HugeGraphStepStrategy.instance());
        TraversalStrategies.GlobalCache.registerStrategies(clazz, strategies);
    }
}
