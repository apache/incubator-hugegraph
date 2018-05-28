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

package com.baidu.hugegraph.core;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.auth.HugeGraphAuthProxy;
import com.baidu.hugegraph.auth.StandardAuthenticator;
import com.baidu.hugegraph.backend.cache.Cache;
import com.baidu.hugegraph.backend.cache.CacheManager;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.metric.MetricsUtil;
import com.baidu.hugegraph.metric.ServerReporter;
import com.baidu.hugegraph.serializer.JsonSerializer;
import com.baidu.hugegraph.serializer.Serializer;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.MetricRegistry;

public final class GraphManager {

    private static final Logger LOG = Log.logger(RestServer.class);

    private final Map<String, Graph> graphs;
    private final StandardAuthenticator authenticator;

    public GraphManager(HugeConfig conf) {
        this.graphs = new ConcurrentHashMap<>();
        this.authenticator = new StandardAuthenticator(conf);

        this.loadGraphs(conf.getMap(ServerOptions.GRAPHS));

        this.addMetrics(conf);
    }

    public void loadGraphs(final Map<String, String> graphConfs) {
        graphConfs.entrySet().forEach(conf -> {
            try {
                final Graph newGraph = GraphFactory.open(conf.getValue());
                this.graphs.put(conf.getKey(), newGraph);
                LOG.info("Graph '{}' was successfully configured via '{}'",
                         conf.getKey(), conf.getValue());
            } catch (RuntimeException e) {
                LOG.error("Graph '{}': '{}' can't be instantiated",
                          conf.getKey(), conf.getValue(), e);
            }
        });
    }

    public Map<String, Graph> graphs() {
        return this.graphs;
    }

    public HugeGraph graph(String name) {
        Graph graph = this.graphs.get(name);

        if (graph == null) {
            return null;
        } else if (graph instanceof HugeGraphAuthProxy) {
            return ((HugeGraphAuthProxy) graph).graph();
        } else if (graph instanceof HugeGraph) {
            return (HugeGraph) graph;
        }

        throw new NotSupportException("graph instance of %s", graph.getClass());
    }

    public Serializer serializer(Graph g) {
        // TODO: cache Serializer
        return new JsonSerializer(g.io(IoCore.graphson()).writer()
                                   .wrapAdjacencyList(true).create());
    }

    public void rollbackAll() {
        this.graphs.entrySet().forEach(e -> {
            final Graph graph = e.getValue();
            if (graph.features().graph().supportsTransactions() &&
                graph.tx().isOpen()) {
                graph.tx().rollback();
            }
        });
    }

    public void rollback(final Set<String> graphSourceNamesToCloseTxOn) {
        closeTx(graphSourceNamesToCloseTxOn, Transaction.Status.ROLLBACK);
    }

    public void commitAll() {
        this.graphs.entrySet().forEach(e -> {
            final Graph graph = e.getValue();
            if (graph.features().graph().supportsTransactions() &&
                graph.tx().isOpen()) {
                graph.tx().commit();
            }
        });
    }

    public void commit(final Set<String> graphSourceNamesToCloseTxOn) {
        closeTx(graphSourceNamesToCloseTxOn, Transaction.Status.COMMIT);
    }

    private void closeTx(final Set<String> graphSourceNamesToCloseTxOn,
                         final Transaction.Status tx) {
        final Set<Graph> graphsToCloseTxOn = new HashSet<>();

        graphSourceNamesToCloseTxOn.forEach(r -> {
            if (this.graphs.containsKey(r)) {
                graphsToCloseTxOn.add(this.graphs.get(r));
            }
        });

        graphsToCloseTxOn.forEach(graph -> {
            if (graph.features().graph().supportsTransactions() &&
                graph.tx().isOpen()) {
                if (tx == Transaction.Status.COMMIT) {
                    graph.tx().commit();
                } else {
                    graph.tx().rollback();
                }
            }
        });
    }

    public boolean requireAuthentication() {
        return this.authenticator.requireAuthentication();
    }

    public String authenticate(String username, String password) {
        return this.authenticator.authenticate(username, password);
    }

    private void addMetrics(HugeConfig config) {
        final MetricManager metrics = MetricManager.INSTANCE;
        // Force add server reporter
        ServerReporter reporter = ServerReporter.instance(metrics.getRegistry());
        reporter.start(60L, TimeUnit.SECONDS);

        // Add metrics for MAX_WRITE_THREADS
        int maxWriteThreads = config.get(ServerOptions.MAX_WRITE_THREADS);
        MetricsUtil.registerGauge(RestServer.class, "max-write-threads", () -> {
            return maxWriteThreads;
        });

        // Add metrics for cache
        Map<String, Cache> caches = CacheManager.instance().caches();
        final AtomicInteger lastCaches = new AtomicInteger(caches.size());
        MetricsUtil.registerGauge(Cache.class, "instances", () -> {
            int count = caches.size();
            if (count != lastCaches.get()) {
                registerCacheMetrics();
            } else {
                lastCaches.set(count);
            }
            return count;
        });
    }

    private void registerCacheMetrics() {
        Map<String, Cache> caches = CacheManager.instance().caches();
        final MetricRegistry registry = MetricManager.INSTANCE.getRegistry();
        Set<String> names = registry.getNames();
        for (Map.Entry<String, Cache> entry : caches.entrySet()) {
            String key = entry.getKey();
            Cache cache = entry.getValue();

            String hits = String.format("%s.%s", key, "hits");
            String miss = String.format("%s.%s", key, "miss");
            String exp = String.format("%s.%s", key, "expire");
            String size = String.format("%s.%s", key, "size");
            String cap = String.format("%s.%s", key, "capacity");
            // Avoid register multi times

            if (names.stream().anyMatch(name -> name.endsWith(hits))) {
                continue;
            }

            MetricsUtil.registerGauge(Cache.class, hits, () -> cache.hits());
            MetricsUtil.registerGauge(Cache.class, miss, () -> cache.miss());
            MetricsUtil.registerGauge(Cache.class, exp, () -> cache.expire());
            MetricsUtil.registerGauge(Cache.class, size, () -> cache.size());
            MetricsUtil.registerGauge(Cache.class, cap, () -> cache.capacity());
        }
    }
}
