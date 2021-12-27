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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.auth.AuthManager;
import com.baidu.hugegraph.auth.HugeAuthenticator;
import com.baidu.hugegraph.auth.HugeFactoryAuthProxy;
import com.baidu.hugegraph.auth.HugeGraphAuthProxy;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.cache.Cache;
import com.baidu.hugegraph.backend.cache.CacheManager;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.store.BackendStoreSystemInfo;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.config.TypedOption;
import com.baidu.hugegraph.event.EventHub;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.license.LicenseVerifier;
import com.baidu.hugegraph.metrics.MetricsUtil;
import com.baidu.hugegraph.metrics.ServerReporter;
import com.baidu.hugegraph.rpc.RpcClientProvider;
import com.baidu.hugegraph.rpc.RpcConsumerConfig;
import com.baidu.hugegraph.rpc.RpcProviderConfig;
import com.baidu.hugegraph.rpc.RpcServer;
import com.baidu.hugegraph.serializer.JsonSerializer;
import com.baidu.hugegraph.serializer.Serializer;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.task.TaskManager;
import com.baidu.hugegraph.type.define.GraphMode;
import com.baidu.hugegraph.type.define.NodeRole;
import com.baidu.hugegraph.util.ConfigUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Events;
import com.baidu.hugegraph.util.Log;

public final class GraphManager {

    private static final Logger LOG = Log.logger(RestServer.class);

    private final String graphsDir;
    private final Map<String, Graph> graphs;
    private final HugeAuthenticator authenticator;
    private final RpcServer rpcServer;
    private final RpcClientProvider rpcClient;

    private Id server;
    private NodeRole role;

    private final EventHub eventHub;

    public GraphManager(HugeConfig conf, EventHub hub) {
        this.graphsDir = conf.get(ServerOptions.GRAPHS);
        this.graphs = new ConcurrentHashMap<>();
        this.authenticator = HugeAuthenticator.loadAuthenticator(conf);
        this.rpcServer = new RpcServer(conf);
        this.rpcClient = new RpcClientProvider(conf);
        this.eventHub = hub;
        this.listenChanges();
        this.loadGraphs(ConfigUtil.scanGraphsDir(this.graphsDir));
        // this.installLicense(conf, "");
        // Raft will load snapshot firstly then launch election and replay log
        this.waitGraphsStarted();
        this.checkBackendVersionOrExit(conf);
        this.startRpcServer();
        this.serverStarted(conf);
        this.addMetrics(conf);
    }

    public void destroy() {
        this.unlistenChanges();
    }

    private void listenChanges() {
        this.eventHub.listen(Events.GRAPH_CREATE, event -> {
            LOG.debug("RestServer accepts event 'graph.create'");
            event.checkArgs(HugeGraph.class);
            HugeGraph graph = (HugeGraph) event.args()[0];
            this.graphs.put(graph.name(), graph);
            return null;
        });
        this.eventHub.listen(Events.GRAPH_DROP, event -> {
            LOG.debug("RestServer accepts event 'graph.drop'");
            event.checkArgs(String.class);
            String name = (String) event.args()[0];
            this.graphs.remove(name);
            return null;
        });
    }

    private void unlistenChanges() {
        this.eventHub.unlisten(Events.GRAPH_CREATE);
        this.eventHub.unlisten(Events.GRAPH_DROP);
    }

    public void loadGraphs(final Map<String, String> graphConfs) {
        for (Map.Entry<String, String> conf : graphConfs.entrySet()) {
            String name = conf.getKey();
            String path = conf.getValue();
            HugeFactory.checkGraphName(name, "rest-server.properties");
            try {
                this.loadGraph(name, path);
            } catch (RuntimeException e) {
                LOG.error("Graph '{}' can't be loaded: '{}'", name, path, e);
            }
        }
    }

    public void waitGraphsStarted() {
        this.graphs.keySet().forEach(name -> {
            HugeGraph graph = this.graph(name);
            graph.waitStarted();
        });
    }

    public HugeGraph cloneGraph(String name, String newName,
                                String configText) {
        /*
         * 0. check and modify params
         * 1. create graph instance
         * 2. init backend store
         * 3. inject graph and traversal source into gremlin server context
         * 4. inject graph into rest server context
         */
        HugeGraph g = this.graph(name);
        E.checkArgumentNotNull(g, "The origin graph '%s' doesn't exist", name);
        E.checkArgumentNotNull(newName, "The graph name can't be null");
        E.checkArgument(!this.graphs().contains(newName),
                        "The graph '%s' has existed", newName);
        PropertiesConfiguration propConfig = this.buildConfig(configText);

        HugeConfig cloneConfig = g.cloneConfig();
        cloneConfig.setDelimiterParsingDisabled(true);
        // Use the passed config to overwrite the old one
        propConfig.getKeys().forEachRemaining(key -> {
            cloneConfig.setProperty(key, propConfig.getProperty(key));
        });
        this.checkOptions(cloneConfig);
        return this.createGraph(cloneConfig);
    }

    public HugeGraph createGraph(String name, String configText) {
        E.checkArgumentNotNull(name, "The graph name can't be null");
        E.checkArgument(!this.graphs().contains(name),
                        "The graph name '%s' has existed", name);

        PropertiesConfiguration propConfig = this.buildConfig(configText);
        HugeConfig config = new HugeConfig(propConfig);
        this.checkOptions(config);
        return this.createGraph(config);
    }

    private HugeGraph createGraph(HugeConfig config) {
        // open succeed will fill graph instance into HugeFactory graphs(map)
        HugeGraph graph = (HugeGraph) GraphFactory.open(config);
        if (this.requireAuthentication()) {
            /*
             * The main purpose is to call method
             * verifyPermission(HugePermission.WRITE, ResourceType.STATUS)
             * that is private
             */
            graph.mode(GraphMode.NONE);
        }
        try {
            graph.initBackend();
            graph.serverStarted(this.server, this.role);
        } catch (BackendException e) {
            HugeFactory.remove(graph);
            throw e;
        }
        // Let gremlin server and rest server context add graph
        this.eventHub.notify(Events.GRAPH_CREATE, graph);
        // Write config to disk file
        ConfigUtil.writeToFile(this.graphsDir, graph.name(), config);
        return graph;
    }

    private PropertiesConfiguration buildConfig(String configText) {
        E.checkArgument(configText != null && !configText.isEmpty(),
                        "The config text can't be null or empty");
        PropertiesConfiguration propConfig = new PropertiesConfiguration();
        try {
            InputStream in = new ByteArrayInputStream(configText.getBytes(
                                                      API.CHARSET));
            propConfig.setDelimiterParsingDisabled(true);
            propConfig.load(in);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read config options", e);
        }
        return propConfig;
    }

    private void checkOptions(HugeConfig config) {
        // The store cannot be the same as the existing graph
        this.checkOptionsUnique(config, CoreOptions.STORE);
        /*
         * NOTE: rocksdb can't use same data path for different graph,
         *       but it's not easy to check here
         */
        String backend = config.get(CoreOptions.BACKEND);
        if (backend.equalsIgnoreCase("rocksdb")) {
            // TODO: should check data path...
        }
    }

    public void dropGraph(String name) {
        HugeGraph g = this.graph(name);
        E.checkArgumentNotNull(g, "The graph '%s' doesn't exist", name);
        E.checkArgument(this.graphs.size() > 1,
                        "The graph '%s' is the only one, not allowed to delete",
                        name);
        g.drop();
        // Let gremlin server and rest server context remove graph
        this.eventHub.notify(Events.GRAPH_DROP, name);
    }

    public Set<String> graphs() {
        return Collections.unmodifiableSet(this.graphs.keySet());
    }

    public HugeGraph graph(String name) {
        Graph graph = this.graphs.get(name);
        if (graph == null) {
            return null;
        } else if (graph instanceof HugeGraph) {
            return (HugeGraph) graph;
        }
        throw new NotSupportException("graph instance of %s", graph.getClass());
    }

    public Serializer serializer(Graph g) {
        return JsonSerializer.instance();
    }

    public void rollbackAll() {
        this.graphs.values().forEach(graph -> {
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
        this.graphs.values().forEach(graph -> {
            if (graph.features().graph().supportsTransactions() &&
                graph.tx().isOpen()) {
                graph.tx().commit();
            }
        });
    }

    public void commit(final Set<String> graphSourceNamesToCloseTxOn) {
        closeTx(graphSourceNamesToCloseTxOn, Transaction.Status.COMMIT);
    }

    public boolean requireAuthentication() {
        if (this.authenticator == null) {
            return false;
        }
        return this.authenticator.requireAuthentication();
    }

    public HugeAuthenticator.User authenticate(Map<String, String> credentials)
                                               throws AuthenticationException {
        return this.authenticator().authenticate(credentials);
    }

    public AuthManager authManager() {
        return this.authenticator().authManager();
    }

    public void close() {
        this.destroyRpcServer();
    }

    private void startRpcServer() {
        if (!this.rpcServer.enabled()) {
            LOG.info("RpcServer is not enabled, skip starting rpc service");
            return;
        }

        RpcProviderConfig serverConfig = this.rpcServer.config();

        // Start auth rpc service if authenticator enabled
        if (this.authenticator != null) {
            serverConfig.addService(AuthManager.class,
                                    this.authenticator.authManager());
        }

        // Start graph rpc service if RPC_REMOTE_URL enabled
        if (this.rpcClient.enabled()) {
            RpcConsumerConfig clientConfig = this.rpcClient.config();

            for (Graph graph : this.graphs.values()) {
                HugeGraph hugegraph = (HugeGraph) graph;
                hugegraph.registerRpcServices(serverConfig, clientConfig);
            }
        }

        try {
            this.rpcServer.exportAll();
        } catch (Throwable e) {
            this.rpcServer.destroy();
            throw e;
        }
    }

    private void destroyRpcServer() {
        try {
            this.rpcClient.destroy();
        } finally {
            this.rpcServer.destroy();
        }
    }

    private HugeAuthenticator authenticator() {
        E.checkState(this.authenticator != null, "Unconfigured authenticator");
        return this.authenticator;
    }

    @SuppressWarnings("unused")
    private void installLicense(HugeConfig config, String md5) {
        LicenseVerifier.instance().install(config, this, md5);
    }

    private void closeTx(final Set<String> graphSourceNamesToCloseTxOn,
                         final Transaction.Status tx) {
        final Set<Graph> graphsToCloseTxOn = new HashSet<>();

        graphSourceNamesToCloseTxOn.forEach(name -> {
            if (this.graphs.containsKey(name)) {
                graphsToCloseTxOn.add(this.graphs.get(name));
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

    private void loadGraph(String name, String path) {
        final Graph graph = GraphFactory.open(path);
        this.graphs.put(name, graph);
        LOG.info("Graph '{}' was successfully configured via '{}'", name, path);

        if (this.requireAuthentication() &&
            !(graph instanceof HugeGraphAuthProxy)) {
            LOG.warn("You may need to support access control for '{}' with {}",
                     path, HugeFactoryAuthProxy.GRAPH_FACTORY);
        }
    }

    private void checkBackendVersionOrExit(HugeConfig config) {
        for (String graph : this.graphs()) {
            // TODO: close tx from main thread
            HugeGraph hugegraph = this.graph(graph);
            if (!hugegraph.backendStoreFeatures().supportsPersistence()) {
                hugegraph.initBackend();
                if (this.requireAuthentication()) {
                    String token = config.get(ServerOptions.AUTH_ADMIN_TOKEN);
                    try {
                        this.authenticator.initAdminUser(token);
                    } catch (Exception e) {
                        throw new BackendException(
                                  "The backend store of '%s' can't " +
                                  "initialize admin user", hugegraph.name());
                    }
                }
            }
            BackendStoreSystemInfo info = hugegraph.backendStoreSystemInfo();
            if (!info.exists()) {
                throw new BackendException(
                          "The backend store of '%s' has not been initialized",
                          hugegraph.name());
            }
            if (!info.checkVersion()) {
                throw new BackendException(
                          "The backend store version is inconsistent");
            }
        }
    }

    private void serverStarted(HugeConfig config) {
        String server = config.get(ServerOptions.SERVER_ID);
        String role = config.get(ServerOptions.SERVER_ROLE);
        E.checkArgument(server != null && !server.isEmpty(),
                        "The server name can't be null or empty");
        E.checkArgument(role != null && !role.isEmpty(),
                        "The server role can't be null or empty");
        this.server = IdGenerator.of(server);
        this.role = NodeRole.valueOf(role.toUpperCase());
        for (String graph : this.graphs()) {
            HugeGraph hugegraph = this.graph(graph);
            assert hugegraph != null;
            hugegraph.serverStarted(this.server, this.role);
        }
    }

    private void addMetrics(HugeConfig config) {
        final MetricManager metric = MetricManager.INSTANCE;
        // Force to add server reporter
        ServerReporter reporter = ServerReporter.instance(metric.getRegistry());
        reporter.start(60L, TimeUnit.SECONDS);

        // Add metrics for MAX_WRITE_THREADS
        int maxWriteThreads = config.get(ServerOptions.MAX_WRITE_THREADS);
        MetricsUtil.registerGauge(RestServer.class, "max-write-threads", () -> {
            return maxWriteThreads;
        });

        // Add metrics for caches
        @SuppressWarnings({ "rawtypes", "unchecked" })
        Map<String, Cache<?, ?>> caches = (Map) CacheManager.instance()
                                                            .caches();
        registerCacheMetrics(caches);
        final AtomicInteger lastCachesSize = new AtomicInteger(caches.size());
        MetricsUtil.registerGauge(Cache.class, "instances", () -> {
            int count = caches.size();
            if (count != lastCachesSize.get()) {
                // Update if caches changed (effect in the next report period)
                registerCacheMetrics(caches);
                lastCachesSize.set(count);
            }
            return count;
        });

        // Add metrics for task
        MetricsUtil.registerGauge(TaskManager.class, "workers", () -> {
            return TaskManager.instance().workerPoolSize();
        });
        MetricsUtil.registerGauge(TaskManager.class, "pending-tasks", () -> {
            return TaskManager.instance().pendingTasks();
        });
    }

    private void checkOptionsUnique(HugeConfig config,
                                    TypedOption<?, ?> option) {
        Object incomingValue = config.get(option);
        for (String graphName : this.graphs.keySet()) {
            HugeGraph graph = this.graph(graphName);
            Object existedValue = graph.option(option);
            E.checkArgument(!incomingValue.equals(existedValue),
                            "The value '%s' of option '%s' conflicts with " +
                            "existed graph", incomingValue, option.name());
        }
    }

    private static void registerCacheMetrics(Map<String, Cache<?, ?>> caches) {
        Set<String> names = MetricManager.INSTANCE.getRegistry().getNames();
        for (Map.Entry<String, Cache<?, ?>> entry : caches.entrySet()) {
            String key = entry.getKey();
            Cache<?, ?> cache = entry.getValue();

            String hits = String.format("%s.%s", key, "hits");
            String miss = String.format("%s.%s", key, "miss");
            String exp = String.format("%s.%s", key, "expire");
            String size = String.format("%s.%s", key, "size");
            String cap = String.format("%s.%s", key, "capacity");

            // Avoid registering multiple times
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
