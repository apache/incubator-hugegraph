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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.cache.CachedBackendStore.QueryId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.event.EventHub;
import com.baidu.hugegraph.event.EventListener;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.iterator.ExtendableIterator;
import com.baidu.hugegraph.iterator.ListIterator;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.Events;
import com.google.common.collect.ImmutableSet;

public final class CachedGraphTransaction extends GraphTransaction {

    private final static int MAX_CACHE_PROPS_PER_VERTEX = 10000;
    private final static int MAX_CACHE_EDGES_PER_QUERY = 100;
    private final static float DEFAULT_LEVEL_RATIO = 0.001f;
    private final static long AVG_VERTEX_ENTRY_SIZE = 40L;
    private final static long AVG_EDGE_ENTRY_SIZE = 100L;

    private final Cache<Id, Object> verticesCache;
    private final Cache<Id, Object> edgesCache;

    private EventListener storeEventListener;
    private EventListener cacheEventListener;

    public CachedGraphTransaction(HugeGraphParams graph, BackendStore store) {
        super(graph, store);

        HugeConfig conf = graph.configuration();

        String type = conf.get(CoreOptions.VERTEX_CACHE_TYPE);
        long capacity = conf.get(CoreOptions.VERTEX_CACHE_CAPACITY);
        int expire = conf.get(CoreOptions.VERTEX_CACHE_EXPIRE);
        this.verticesCache = this.cache("vertex", type, capacity,
                                        AVG_VERTEX_ENTRY_SIZE, expire);

        type = conf.get(CoreOptions.EDGE_CACHE_TYPE);
        capacity = conf.get(CoreOptions.EDGE_CACHE_CAPACITY);
        expire = conf.get(CoreOptions.EDGE_CACHE_EXPIRE);
        this.edgesCache = this.cache("edge", type, capacity,
                                     AVG_EDGE_ENTRY_SIZE, expire);

        this.listenChanges();
    }

    @Override
    public void close() {
        try {
            super.close();
        } finally {
            this.unlistenChanges();
        }
    }

    private Cache<Id, Object> cache(String prefix, String type, long capacity,
                                    long entrySize, long expire) {
        HugeGraph graph = super.graph();
        String name = prefix + "-" + graph.name();
        Cache<Id, Object> cache;
        switch (type) {
            case "l1":
                cache = CacheManager.instance().cache(name, capacity);
                break;
            case "l2":
                long heapCapacity = (long) (DEFAULT_LEVEL_RATIO * capacity);
                cache = CacheManager.instance().levelCache(graph, name,
                                                           heapCapacity,
                                                           capacity, entrySize);
                break;
            default:
                throw new NotSupportException("cache type '%s'", type);
        }
        // Convert the unit from seconds to milliseconds
        cache.expire(expire * 1000L);
        return cache;
    }

    private void listenChanges() {
        // Listen store event: "store.init", "store.clear", ...
        Set<String> storeEvents = ImmutableSet.of(Events.STORE_INIT,
                                                  Events.STORE_CLEAR,
                                                  Events.STORE_TRUNCATE);
        this.storeEventListener = event -> {
            if (storeEvents.contains(event.name())) {
                LOG.debug("Graph {} clear graph cache on event '{}'",
                          this.graph(), event.name());
                this.verticesCache.clear();
                this.edgesCache.clear();
                return true;
            }
            return false;
        };
        this.store().provider().listen(this.storeEventListener);

        // Listen cache event: "cache"(invalid cache item)
        this.cacheEventListener = event -> {
            LOG.debug("Graph {} received graph cache event: {}",
                      this.graph(), event);
            event.checkArgs(String.class, Id.class);
            Object[] args = event.args();
            if ("invalid".equals(args[0])) {
                Id id = (Id) args[1];
                if (this.verticesCache.get(id) != null) {
                    // Invalidate vertex cache
                    this.verticesCache.invalidate(id);
                } else if (this.edgesCache.get(id) != null) {
                    // Invalidate edge cache
                    this.edgesCache.invalidate(id);
                }
                return true;
            } else if ("clear".equals(args[0])) {
                this.verticesCache.clear();
                this.edgesCache.clear();
                return true;
            }
            return false;
        };
        EventHub graphEventHub = this.params().graphEventHub();
        if (!graphEventHub.containsListener(Events.CACHE)) {
            graphEventHub.listen(Events.CACHE, this.cacheEventListener);
        }
    }

    private void unlistenChanges() {
        // Unlisten store event
        this.store().provider().unlisten(this.storeEventListener);

        // Unlisten cache event
        EventHub graphEventHub = this.params().graphEventHub();
        graphEventHub.unlisten(Events.CACHE, this.cacheEventListener);
    }

    @Override
    protected final Iterator<HugeVertex> queryVerticesFromBackend(Query query) {
        if (!query.ids().isEmpty() && query.conditions().isEmpty()) {
            return this.queryVerticesByIds((IdQuery) query);
        } else {
            return super.queryVerticesFromBackend(query);
        }
    }

    private Iterator<HugeVertex> queryVerticesByIds(IdQuery query) {
        IdQuery newQuery = new IdQuery(HugeType.VERTEX, query);
        List<HugeVertex> vertices = new ArrayList<>();
        for (Id vertexId : query.ids()) {
            Object vertex = this.verticesCache.get(vertexId);
            if (vertex != null) {
                vertices.add((HugeVertex) vertex);
            } else {
                newQuery.query(vertexId);
            }
        }

        // Join results from cache and backend
        ExtendableIterator<HugeVertex> results = new ExtendableIterator<>();
        if (!vertices.isEmpty()) {
            results.extend(vertices.iterator());
        } else {
            // Just use the origin query if find none from the cache
            newQuery = query;
        }

        if (!newQuery.empty()) {
            Iterator<HugeVertex> rs = super.queryVerticesFromBackend(newQuery);
            // Generally there are not too much data with id query
            ListIterator<HugeVertex> listIterator = QueryResults.toList(rs);
            for (HugeVertex vertex : listIterator.list()) {
                if (vertex.sizeOfSubProperties() > MAX_CACHE_PROPS_PER_VERTEX) {
                    // Skip large vertex
                    continue;
                }
                this.verticesCache.update(vertex.id(), vertex);
            }
            results.extend(listIterator);
        }

        return results;
    }

    @Override
    protected final Iterator<HugeEdge> queryEdgesFromBackend(Query query) {
        if (query.empty() || query.paging() || query.bigCapacity()) {
            // Query all edges or query edges in paging, don't cache it
            return super.queryEdgesFromBackend(query);
        }

        Id cacheKey = new QueryId(query);
        Object value = this.edgesCache.get(cacheKey);
        if (value != null) {
            @SuppressWarnings("unchecked")
            Collection<HugeEdge> edges = (Collection<HugeEdge>) value;
            return edges.iterator();
        } else {
            Iterator<HugeEdge> rs = super.queryEdgesFromBackend(query);
            /*
             * Iterator can't be cached, caching list instead
             * Generally there are not too much data with id query
             */
            ListIterator<HugeEdge> listIterator = QueryResults.toList(rs);
            Collection<HugeEdge> edges = listIterator.list();
            if (edges.size() == 0) {
                this.edgesCache.update(cacheKey, Collections.emptyList());
            } else if (edges.size() <= MAX_CACHE_EDGES_PER_QUERY) {
                // Skip large query
                this.edgesCache.update(cacheKey, edges);
            }
            return listIterator;
        }
    }

    @Override
    protected final void commitMutation2Backend(BackendMutation... mutations) {
        // Collect changes before commit
        Collection<HugeVertex> changes = this.verticesInTxUpdated();
        Collection<HugeVertex> deletions = this.verticesInTxRemoved();
        int edgesInTxSize = this.edgesInTxSize();

        try {
            super.commitMutation2Backend(mutations);
            // Update vertex cache
            for (HugeVertex vertex : changes) {
                if (vertex.sizeOfSubProperties() > MAX_CACHE_PROPS_PER_VERTEX) {
                    // Skip large vertex
                    this.verticesCache.invalidate(vertex.id());
                    continue;
                }
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
    public final void removeIndex(IndexLabel indexLabel) {
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
