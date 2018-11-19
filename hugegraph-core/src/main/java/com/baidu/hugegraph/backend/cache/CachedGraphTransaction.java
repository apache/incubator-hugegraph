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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.cache.CachedBackendStore.QueryId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeType;
import com.google.common.collect.ImmutableList;

public class CachedGraphTransaction extends GraphTransaction {

    private final static int MAX_CACHE_EDGES_PER_QUERY = 100;

    private final Cache verticesCache;
    private final Cache edgesCache;

    public CachedGraphTransaction(HugeGraph graph, BackendStore store) {
        super(graph, store);

        HugeConfig conf = graph.configuration();

        int capacity = conf.get(CoreOptions.VERTEX_CACHE_CAPACITY);
        int expire = conf.get(CoreOptions.VERTEX_CACHE_EXPIRE);
        this.verticesCache = this.cache("vertex", capacity, expire);

        capacity = conf.get(CoreOptions.EDGE_CACHE_CAPACITY);
        expire = conf.get(CoreOptions.EDGE_CACHE_EXPIRE);
        this.edgesCache = this.cache("edge", capacity, expire);
    }

    private Cache cache(String prefix, int capacity, long expire) {
        String name = prefix + "-" + super.graph().name();
        Cache cache = CacheManager.instance().cache(name, capacity);
        cache.expire(expire);
        return cache;
    }

    @Override
    protected Iterator<HugeVertex> queryVerticesFromBackend(Query query) {
        if (!query.ids().isEmpty() && query.conditions().isEmpty()) {
            return this.queryVerticesByIds((IdQuery) query);
        } else {
            return super.queryVerticesFromBackend(query);
        }
    }

    private Iterator<HugeVertex> queryVerticesByIds(IdQuery query) {
        IdQuery newQuery = new IdQuery(HugeType.VERTEX, query);
        List<HugeVertex> vertices = new ArrayList<>(query.ids().size());
        for (Id vertexId : query.ids()) {
            Object vertex = this.verticesCache.get(vertexId);
            if (vertex != null) {
                vertices.add((HugeVertex) vertex);
            } else {
                newQuery.query(vertexId);
            }
        }
        if (vertices.isEmpty()) {
            // Just use the origin query if find none from the cache
            newQuery = query;
        }
        if (!newQuery.empty()) {
            Iterator<HugeVertex> rs = super.queryVerticesFromBackend(newQuery);
            while (rs.hasNext()) {
                HugeVertex vertex = rs.next();
                vertices.add(vertex);
                this.verticesCache.update(vertex.id(), vertex);
            }
        }
        return vertices.iterator();
    }

    @Override
    protected Iterator<HugeEdge> queryEdgesFromBackend(Query query) {
        if (query.empty()) {
            // Query all edges, don't cache it
            return super.queryEdgesFromBackend(query);
        }

        Id id = new QueryId(query);
        @SuppressWarnings("unchecked")
        List<HugeEdge> edges = (List<HugeEdge>) this.edgesCache.get(id);
        if (edges == null) {
            // Iterator can't be cached, caching list instead
            edges = ImmutableList.copyOf(super.queryEdgesFromBackend(query));
            if (edges.size() <= MAX_CACHE_EDGES_PER_QUERY) {
                this.edgesCache.update(id, edges);
            }
        }
        return edges.iterator();
    }


    @Override
    public void commit() throws BackendException {
        // Collect changes before commit
        Collection<HugeVertex> changes = this.verticesInTxUpdated();
        Collection<HugeVertex> deletions = this.verticesInTxRemoved();
        int edgesInTxSize = this.edgesInTxSize();

        try {
            super.commit();
            // Update vertex cache
            for (HugeVertex vertex : changes) {
                vertex = vertex.resetTx();
                this.verticesCache.updateIfPresent(vertex.id(), vertex);
            }
        } finally {
            // Update removed vertex in cache whatever success or fail
            for (HugeVertex vertex : deletions) {
                this.verticesCache.invalidate(vertex.id());
            }

            // Update edge cache if any edges change
            if (edgesInTxSize > 0) {
                // TODO: Use a more precise strategy to update the edge cache
                this.edgesCache.clear();
            }
        }
    }

    @Override
    public void removeIndex(IndexLabel indexLabel) {
        try {
            super.removeIndex(indexLabel);
        } finally {
            // Update edge cache if needed (any edge-index is deleted)
            if (indexLabel.baseType() == HugeType.EDGE_LABEL) {
                // TODO: Use a more precise strategy to update the edge cache
                this.edgesCache.clear();
            }
        }
    }
}
