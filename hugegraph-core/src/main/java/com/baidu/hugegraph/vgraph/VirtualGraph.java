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
package com.baidu.hugegraph.vgraph;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.event.EventHub;
import com.baidu.hugegraph.event.EventListener;
import com.baidu.hugegraph.iterator.ExtendableIterator;
import com.baidu.hugegraph.iterator.MapperIterator;
import com.baidu.hugegraph.metrics.MetricManager;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Events;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableSet;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.baidu.hugegraph.backend.id.Id;
import org.slf4j.Logger;

public class VirtualGraph {
    private static final Logger LOG = Log.logger(VirtualGraph.class);

    private Cache<Id, VirtualVertex> vertexCache;
    private Cache<Id, VirtualEdge> edgeCache;

    private EventListener storeEventListener;
    private EventListener cacheEventListener;
    private VirtualGraphLoader loader;

    private final HugeGraphParams graphParams;
    private final String metricsNamePrefix;
    private final Meter hits;
    private final Timer calls;
    private Gauge<Long> vertexCountGauge;
    private Gauge<Long> edgeCountGauge;

    public VirtualGraph(HugeGraphParams graphParams) {
        assert graphParams != null;
        this.graphParams = graphParams;
        this.hits = new Meter();
        this.calls = new Timer();
        this.metricsNamePrefix = MetricRegistry.name(this.getClass(),
                this.graphParams.graph().graphSpace(), this.graphParams.graph().name());
        this.init();
    }

    public void init(){
        this.vertexCache = Caffeine.newBuilder()
                .initialCapacity(this.graphParams.configuration().get(
                                CoreOptions.VIRTUAL_GRAPH_VERTEX_INIT_CAPACITY))
                .expireAfterAccess(this.graphParams.configuration().get(
                        CoreOptions.VIRTUAL_GRAPH_VERTEX_EXPIRE), TimeUnit.SECONDS)
                .maximumSize(this.graphParams.configuration().get(
                        CoreOptions.VIRTUAL_GRAPH_VERTEX_MAX_SIZE))
                .recordStats(() ->
                        new MetricsStatsCounter(MetricManager.INSTANCE.getRegistry(),
                                MetricRegistry.name(this.metricsNamePrefix, "vertex")))
                .build();
        this.edgeCache = Caffeine.newBuilder()
                .initialCapacity(this.graphParams.configuration().get(
                        CoreOptions.VIRTUAL_GRAPH_EDGE_INIT_CAPACITY))
                .expireAfterAccess(this.graphParams.configuration().get(
                        CoreOptions.VIRTUAL_GRAPH_EDGE_EXPIRE), TimeUnit.SECONDS)
                .maximumSize(this.graphParams.configuration().get(
                        CoreOptions.VIRTUAL_GRAPH_EDGE_MAX_SIZE))
                .recordStats(() ->
                        new MetricsStatsCounter(MetricManager.INSTANCE.getRegistry(),
                                MetricRegistry.name(this.metricsNamePrefix, "edge")))
                .build();

        this.loader = new VirtualGraphLoader(this.graphParams, this);

        this.vertexCountGauge = this.vertexCache::estimatedSize;
        this.edgeCountGauge = this.edgeCache::estimatedSize;

        MetricManager.INSTANCE.getRegistry().register(
                MetricRegistry.name(this.metricsNamePrefix, "vertex", "count"), this.vertexCountGauge);
        MetricManager.INSTANCE.getRegistry().register(
                MetricRegistry.name(this.metricsNamePrefix, "edge", "count"), this.edgeCountGauge);
        MetricManager.INSTANCE.getRegistry().register(
                MetricRegistry.name(this.metricsNamePrefix, "hits"), this.hits);
        MetricManager.INSTANCE.getRegistry().register(
                MetricRegistry.name(this.metricsNamePrefix, "calls"), this.calls);

        this.listenChanges();
    }

    public int getVertexSize() {
        return (int) this.vertexCache.estimatedSize();
    }

    public int getEdgeSize() {
        return (int) this.edgeCache.estimatedSize();
    }

    public Iterator<HugeVertex> queryHugeVertexByIds(List<Id> vIds, VirtualVertexStatus status) {
        assert vIds != null;
        assert status != VirtualVertexStatus.None;

        E.checkArgument(status != VirtualVertexStatus.OutEdge && status != VirtualVertexStatus.InEdge,
                "Get edges of vertex is not supported in this method, please use queryVertexWithEdges.");

        ExtendableIterator<HugeVertex> result = new ExtendableIterator<>();
        List<HugeVertex> resultFromCache = new ArrayList<>(vIds.size());
        List<Id> missingIds = new ArrayList<>();
        for (Id vId : vIds) {
            VirtualVertex vertex = queryVertexById(vId, status);
            if (vertex != null) {
                resultFromCache.add(vertex.getVertex(graphParams.graph()));
            } else {
                missingIds.add(vId);
            }
        }

        result.extend(resultFromCache.listIterator());

        if (missingIds.size() > 0) {
            VirtualGraphQueryTask<VirtualVertex> batchTask = new VirtualGraphQueryTask<>(HugeType.VERTEX, missingIds);
            this.loader.add(batchTask);
            try {
                Iterator<VirtualVertex> resultFromBatch = batchTask.getFuture().get();
                result.extend(new MapperIterator<>(resultFromBatch, v -> {
                    HugeVertex vertex = v.getVertex(graphParams.graph());
                    return vertex.expired() ? null : vertex;
                }));
            }
            catch (Exception ex) {
                throw new HugeException("Failed to query vertex in batch", ex);
            }
        }
        return result;
    }

    public Iterator<HugeEdge> queryEdgesByVertexId(Id vId, VirtualVertexStatus status) {
        assert vId != null;
        VirtualVertex v = queryVertexById(vId, status);
        if (v != null) {
            HugeVertex owner = v.getVertex(graphParams.graph());
            return new MapperIterator<>(v.getEdges(graphParams.graph()), e -> e.getEdge(owner));
        }
        return null;
    }

    public boolean updateIfPresentVertex(HugeVertex vertex, Iterator<HugeEdge> inEdges) {
        assert vertex != null;
        VirtualVertex vertexFromCache = this.vertexCache.getIfPresent(vertex.id());
        if (vertexFromCache != null) {
            VirtualVertex vertexMerged = toVirtual(vertexFromCache, vertex, vertex.getEdges(), inEdges);
            if (vertexMerged != null) {
                this.vertexCache.put(vertex.id(), vertexMerged);
                return true;
            }
        }
        return false;
    }

    public VirtualVertex putVertex(HugeVertex vertex, Collection<HugeEdge> outEdges, Iterator<HugeEdge> inEdges) {
        assert vertex != null;
        VirtualVertex vertexFromCache = this.vertexCache.getIfPresent(vertex.id());
        VirtualVertex vertexMerged = toVirtual(vertexFromCache, vertex, outEdges, inEdges);
        if (vertexMerged != null) {
            this.vertexCache.put(vertex.id(), vertexMerged);
        }
        else {
            this.vertexCache.invalidate(vertex.id());
        }
        return vertexMerged;
    }

    public void updateIfPresentEdge(Iterator<HugeEdge> edges) {
        assert edges != null;
        edges.forEachRemaining(edge -> {
            VirtualEdge old = this.edgeCache.getIfPresent(edge.id());
            if (old != null) {
                VirtualEdge edgeMerged = toVirtual(old, edge);
                if (edgeMerged != null) {
                    this.edgeCache.put(edge.id(), edgeMerged);
                }
            }
        });
    }

    public VirtualEdge putEdge(HugeEdge edge) {
        assert edge != null;
        VirtualEdge old = this.edgeCache.getIfPresent(edge.id());
        VirtualEdge edgeMerged = toVirtual(old, edge);
        if (edgeMerged != null) {
            this.edgeCache.put(edge.id(), edgeMerged);
        }
        else {
            this.edgeCache.invalidate(edge.id());
        }
        return edgeMerged;
    }

    public void putEdges(Iterator<HugeEdge> values) {
        assert values != null;
        values.forEachRemaining(this::putEdge);
    }

    public void invalidateVertex(Id vId) {
        assert vId != null;
        this.vertexCache.invalidate(vId);
    }

    public Iterator<HugeEdge> queryEdgeByIds(List<Id> eIds, VirtualEdgeStatus status) {
        assert eIds != null;
        assert status != VirtualEdgeStatus.None;

        ExtendableIterator<HugeEdge> result = new ExtendableIterator<>();
        Map<Id, HugeVertex> vertexMap = new HashMap<>();
        List<HugeEdge> resultFromCache = new ArrayList<>(eIds.size());
        List<Id> missingIds = new ArrayList<>();
        eIds.forEach(eId -> {
            assert eId instanceof EdgeId;
            HugeVertex owner = vertexMap.computeIfAbsent(((EdgeId) eId).ownerVertexId(),
                    vId -> new HugeVertex(graphParams.graph(), vId, VertexLabel.undefined(graphParams.graph())));
            HugeEdge edge = queryEdgeById(eId, status, owner);
            if (edge != null) {
                resultFromCache.add(edge);
            } else {
                missingIds.add(eId);
            }
        });

        result.extend(resultFromCache.listIterator());

        if (missingIds.size() > 0) {
            VirtualGraphQueryTask<VirtualEdge> batchTask = new VirtualGraphQueryTask<>(HugeType.EDGE, missingIds);
            this.loader.add(batchTask);
            try {
                Iterator<VirtualEdge> resultFromBatch = batchTask.getFuture().get();
                result.extend(new MapperIterator<>(resultFromBatch, e -> {
                    HugeVertex owner = vertexMap.computeIfAbsent(e.getOwnerVertexId(),
                            vId -> new HugeVertex(graphParams.graph(), vId,
                                    VertexLabel.undefined(graphParams.graph())));
                    HugeEdge edge = e.getEdge(owner);
                    return edge.expired() ? null : edge;
                }));
            }
            catch (Exception ex) {
                throw new HugeException("Failed to query edge in batch", ex);
            }
        }

        return result;
    }

    public void invalidateEdge(Id eId) {
        assert eId != null;
        this.edgeCache.invalidate(eId);
    }

    public void clear(){
        this.vertexCache.invalidateAll();
        this.edgeCache.invalidateAll();
    }

    public void close() {
        this.loader.close();
        this.unlistenChanges();
        this.clear();
        MetricManager.INSTANCE.getRegistry().removeMatching(
                (name, metric) -> name.startsWith(this.metricsNamePrefix));
    }

    private VirtualVertex queryVertexById(Id vId, VirtualVertexStatus status) {
        assert vId != null;

        try (Timer.Context ignored = this.calls.time()) {
            VirtualVertex vertex = this.vertexCache.getIfPresent(vId);
            if (vertex == null) {
                return null;
            }

            if (vertex.getVertex(graphParams.graph()).expired()) {
                this.invalidateVertex(vId);
                return null;
            }

            if (status.match(vertex.getStatus())) {
                this.hits.mark();
                return vertex;
            } else {
                return null;
            }
        }
    }

    private HugeEdge queryEdgeById(Id eId, VirtualEdgeStatus status, HugeVertex owner) {
        try (Timer.Context ignored = this.calls.time()) {
            HugeEdge edge = toHuge(this.edgeCache.getIfPresent(eId), status, owner);
            if (edge != null) {
                this.hits.mark();
            }
            return edge;
        }
    }

    private HugeEdge toHuge(VirtualEdge edge, VirtualEdgeStatus status, HugeVertex owner) {
        if (edge == null) {
            return null;
        }

        HugeEdge hugeEdge = edge.getEdge(owner);

        if (hugeEdge.expired()) {
            this.invalidateEdge(hugeEdge.id());
            return null;
        }

        if (status.match(edge.getStatus())) {
            return hugeEdge;
        } else {
            return null;
        }
    }

    private VirtualVertex toVirtual(VirtualVertex old, HugeVertex hugeVertex,
                                    Collection<HugeEdge> outEdges,
                                    Iterator<HugeEdge> inEdges) {
        if (outEdges == null && hugeVertex.existsEdges()) {
            throw new IllegalArgumentException(
                    "Argument outEdges is null but hugeVertex exists out-edges.");
        }
        VirtualVertex newVertex = new VirtualVertex(hugeVertex, VirtualVertexStatus.Id.code());
        VirtualVertex result = mergeVVOutEdge(old, newVertex, outEdges);
        result = mergeVVInEdge(old, result, inEdges);
        mergeVVProp(old, result, hugeVertex);
        assert result != null;
        return result;
    }

    private VirtualEdge toVirtual(VirtualEdge old, HugeEdge hugeEdge) {
        VirtualEdge newEdge = new VirtualEdge(hugeEdge, VirtualEdgeStatus.Id.code());
        VirtualEdge result = mergeVEProp(old, newEdge, hugeEdge);
        assert result != null;
        return result;
    }

    private VirtualVertex mergeVVOutEdge(VirtualVertex oldV, VirtualVertex newV,
                                         Collection<HugeEdge> outEdges) {
        if (outEdges != null) {
            List<VirtualEdge> outEdgeList = new ArrayList<>();
            outEdges.forEach(e -> {
                VirtualEdge edge = toVirtual(null, e);  // give up to refresh edge cache due to memory cost
                assert edge != null;
                outEdgeList.add(edge);
            });
            newV.addOutEdges(outEdgeList);
            newV.orStatus(VirtualVertexStatus.OutEdge);
        }
        if (oldV == null) {
            return newV;
        }
        // new vertex has no out-edges
        if (!VirtualVertexStatus.OutEdge.match(newV.getStatus())) {
            return oldV;
        }
        return newV;
    }

    private VirtualVertex mergeVVInEdge(VirtualVertex oldV, VirtualVertex resultV, Iterator<HugeEdge> inEdges) {
        if (inEdges != null) {
            List<VirtualEdge> inEdgeList = new ArrayList<>();
            inEdges.forEachRemaining(e -> {
                VirtualEdge edge = toVirtual(null, e);  // give up to refresh edge cache due to memory cost
                assert edge != null;
                inEdgeList.add(edge);
            });
            resultV.addInEdges(inEdgeList);
            resultV.orStatus(VirtualVertexStatus.InEdge);
        }

        if (oldV == null) {
            return resultV;
        }

        // resultV vertex has no in-edges, copy from old
        if (!VirtualVertexStatus.InEdge.match(resultV.getStatus())) {
            resultV.copyInEdges(oldV);
        }
        return resultV;
    }

    private void mergeVVProp(VirtualVertex oldV, VirtualVertex resultV, HugeVertex hugeVertex) {
        if (hugeVertex.isPropLoaded()) {
            resultV.orStatus(VirtualVertexStatus.Property);
            resultV.setProperties(hugeVertex.getProperties().values());
        } else if (oldV != null && oldV != resultV
                && VirtualVertexStatus.Property.match(oldV.getStatus())) {
            resultV.orStatus(VirtualVertexStatus.Property);
            resultV.copyProperties(oldV);
        }
    }

    private VirtualEdge mergeVEProp(VirtualEdge oldE, VirtualEdge newE, HugeEdge hugeEdge) {
        if (oldE == newE) {
            return oldE;
        }

        if (oldE == null) {
            return newE;
        }

        if (hugeEdge.isPropLoaded()) {
            newE.orStatus(VirtualEdgeStatus.Property);
        }
        else {
            // new edge's properties is not loaded and old edge's properties is loaded,
            // copy properties from old
            if (VirtualVertexStatus.Property.match(oldE.getStatus())) {
                newE.orStatus(VirtualEdgeStatus.Property);
                newE.copyProperties(oldE);
            }
        }

        return newE;
    }


    private void listenChanges() {
        // Listen store event: "store.init", "store.clear", ...
        Set<String> storeEvents = ImmutableSet.of(Events.STORE_INIT,
                Events.STORE_CLEAR,
                Events.STORE_TRUNCATE);
        this.storeEventListener = event -> {
            if (storeEvents.contains(event.name())) {
                LOG.debug("Graph {} clear graph cache on event '{}'",
                        this.graphParams.graph(), event.name());
                this.clearCache(null, true);
                return true;
            }
            return false;
        };
        this.graphParams.loadGraphStore().provider().listen(this.storeEventListener);

        // Listen cache event: "cache"(invalid cache item)
        this.cacheEventListener = event -> {
            LOG.debug("Graph {} received graph cache event: {}",
                    this.graphParams.graph(), event);
            Object[] args = event.args();
            E.checkArgument(args.length > 0 && args[0] instanceof String,
                    "Expect event action argument");
            if (com.baidu.hugegraph.backend.cache.Cache.ACTION_INVALID.equals(args[0])) {
                event.checkArgs(String.class, HugeType.class, Object.class);
                HugeType type = (HugeType) args[1];
                if (type.isVertex()) {
                    // Invalidate vertex cache
                    Object arg2 = args[2];
                    if (arg2 instanceof Id) {
                        Id id = (Id) arg2;
                        this.vertexCache.invalidate(id);
                    } else if (arg2 != null && arg2.getClass().isArray()) {
                        int size = Array.getLength(arg2);
                        for (int i = 0; i < size; i++) {
                            Object id = Array.get(arg2, i);
                            E.checkArgument(id instanceof Id,
                                    "Expect instance of Id in array, " +
                                            "but got '%s'", id.getClass());
                            this.vertexCache.invalidate(id);
                        }
                    } else {
                        E.checkArgument(false,
                                "Expect Id or Id[], but got: %s",
                                arg2);
                    }
                } else if (type.isEdge()) {
                    // Invalidate vertex cache
                    Object arg2 = args[2];
                    if (arg2 instanceof Id) {
                        Id id = (Id) arg2;
                        this.edgeCache.invalidate(id);
                    } else if (arg2 != null && arg2.getClass().isArray()) {
                        int size = Array.getLength(arg2);
                        for (int i = 0; i < size; i++) {
                            Object id = Array.get(arg2, i);
                            E.checkArgument(id instanceof Id,
                                    "Expect instance of Id in array, " +
                                            "but got '%s'", id.getClass());
                            this.edgeCache.invalidate(id);
                        }
                    } else {
                        E.checkArgument(false,
                                "Expect Id or Id[], but got: %s",
                                arg2);
                    }
                }
                return true;
            } else if (com.baidu.hugegraph.backend.cache.Cache.ACTION_CLEAR.equals(args[0])) {
                event.checkArgs(String.class, HugeType.class);
                HugeType type = (HugeType) args[1];
                this.clearCache(type, false);
                return true;
            }
            return false;
        };
        EventHub graphEventHub = this.graphParams.graphEventHub();
        if (!graphEventHub.containsListener(Events.CACHE)) {
            graphEventHub.listen(Events.CACHE, this.cacheEventListener);
        }
    }

    private void unlistenChanges() {
        // Unlisten store event
        this.graphParams.loadGraphStore().provider().unlisten(this.storeEventListener);

        // Unlisten cache event
        EventHub graphEventHub = this.graphParams.graphEventHub();
        graphEventHub.unlisten(Events.CACHE, this.cacheEventListener);
    }

    public void notifyChanges(String action, HugeType type, Id[] ids) {
        EventHub graphEventHub = this.graphParams.graphEventHub();
        graphEventHub.notify(Events.CACHE, action, type, ids);
    }

    private void clearCache(HugeType type, boolean notify) {
        if (type == null || type == HugeType.VERTEX) {
            this.vertexCache.invalidateAll();
        }
        if (type == null || type == HugeType.EDGE) {
            this.edgeCache.invalidateAll();
        }

        if (notify) {
            this.notifyChanges(com.baidu.hugegraph.backend.cache.Cache.ACTION_CLEARED, null, null);
        }
    }
}