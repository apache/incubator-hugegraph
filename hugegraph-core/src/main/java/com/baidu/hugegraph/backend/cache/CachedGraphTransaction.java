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

package com.baidu.hugegraph.backend.cache;

import java.util.ArrayList;
import java.util.List;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.cache.CachedBackendStore.QueryId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeEdgeProperty;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.structure.HugeVertexProperty;

public class CachedGraphTransaction extends GraphTransaction {

    private final Cache verticesCache;
    private final Cache edgesCache;

    public CachedGraphTransaction(HugeGraph graph, BackendStore store,
                                  BackendStore indexStore) {
        super(graph, store, indexStore);
        this.verticesCache = this.cache("vertex");
        this.edgesCache = this.cache("edge");
    }

    private Cache cache(String prefix) {
        HugeConfig conf = super.graph().configuration();

        final String name = prefix + "-" + super.graph().name();
        final int capacity = conf.get(CoreOptions.GRAPH_CACHE_CAPACITY);
        final int expire = conf.get(CoreOptions.GRAPH_CACHE_EXPIRE);

        Cache cache = CacheManager.instance().cache(name, capacity);
        cache.expire(expire);
        return cache;
    }

    @Override
    public Iterable<Vertex> queryVertices(Object... vertexIds) {
        List<Vertex> vertices = new ArrayList<>(vertexIds.length);
        for (Object i : vertexIds) {
            Id vid = HugeElement.getIdValue(i);
            Object v = this.verticesCache.getOrFetch(vid, id -> {
                return super.queryVertices(id).iterator().next();
            });
            vertices.add(((HugeVertex) v).copy());
        }
        return vertices;
    }

    @Override
    public Iterable<Vertex> queryVertices(Query query) {
        if (!query.ids().isEmpty() && query.conditions().isEmpty()) {
            return this.queryVertices(query.ids().toArray());
        } else {
            return super.queryVertices(query);
        }
    }

    @Override
    public Iterable<Edge> queryEdges(Query query) {
        if (query.empty()) {
            // Query all edges, don't cache it
            return super.queryEdges(query);
        }

        Object result = this.edgesCache.getOrFetch(new QueryId(query), id -> {
            return super.queryEdges(query);
        });
        @SuppressWarnings("unchecked")
        Iterable<Edge> edges = (Iterable<Edge>) result;
        return edges;
    }

    @Override
    public Vertex addVertex(HugeVertex vertex) {
        // Update vertex cache
        this.verticesCache.invalidate(vertex.id());

        return super.addVertex(vertex);
    }

    @Override
    public void removeVertex(HugeVertex vertex) {
        // Update vertex cache
        this.verticesCache.invalidate(vertex.id());

        // TODO: Use a more precise strategy to update the edge cache
        this.edgesCache.clear();

        super.removeVertex(vertex);
    }

    @Override
    public <V> void addVertexProperty(HugeVertexProperty<V> prop) {
        // Update vertex cache
        this.verticesCache.invalidate(prop.element().id());

        super.addVertexProperty(prop);
    }

    @Override
    public <V> void removeVertexProperty(HugeVertexProperty<V> prop) {
        // Update vertex cache
        this.verticesCache.invalidate(prop.element().id());

        super.removeVertexProperty(prop);
    }

    @Override
    public Edge addEdge(HugeEdge edge) {
        // TODO: Use a more precise strategy to update the edge cache
        this.edgesCache.clear();

        return super.addEdge(edge);
    }

    @Override
    public void removeEdge(HugeEdge edge) {
        // TODO: Use a more precise strategy to update the edge cache
        this.edgesCache.clear();

        super.removeEdge(edge);
    }

    @Override
    public void removeEdges(EdgeLabel edgeLabel) {
        // TODO: Use a more precise strategy to update the edge cache
        this.edgesCache.clear();

        super.removeEdges(edgeLabel);
    }

    @Override
    public <V> void addEdgeProperty(HugeEdgeProperty<V> prop) {
        // TODO: Use a more precise strategy to update the edge cache
        this.edgesCache.clear();

        super.addEdgeProperty(prop);
    }

    @Override
    public <V> void removeEdgeProperty(HugeEdgeProperty<V> prop) {
        // TODO: Use a more precise strategy to update the edge cache
        this.edgesCache.clear();

        super.removeEdgeProperty(prop);
    }

    @Override
    public void removeIndex(IndexLabel indexLabel) {
        // TODO: Use a more precise strategy to update the edge cache
        this.edgesCache.clear();

        super.removeIndex(indexLabel);
    }
}
