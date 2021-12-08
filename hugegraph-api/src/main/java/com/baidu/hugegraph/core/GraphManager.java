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

import static com.baidu.hugegraph.space.GraphSpace.DEFAULT_GRAPH_SPACE_DESCRIPTION;
import static com.baidu.hugegraph.space.GraphSpace.DEFAULT_GRAPH_SPACE_NAME;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.StandardHugeGraph;
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
import com.baidu.hugegraph.k8s.K8sManager;
import com.baidu.hugegraph.license.LicenseVerifier;
import com.baidu.hugegraph.meta.MetaManager;
import com.baidu.hugegraph.metrics.MetricsUtil;
import com.baidu.hugegraph.metrics.ServerReporter;
import com.baidu.hugegraph.serializer.JsonSerializer;
import com.baidu.hugegraph.serializer.Serializer;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.space.GraphSpace;
import com.baidu.hugegraph.space.Service;
import com.baidu.hugegraph.task.TaskManager;
import com.baidu.hugegraph.type.define.CollectionType;
import com.baidu.hugegraph.type.define.GraphMode;
import com.baidu.hugegraph.type.define.NodeRole;
import com.baidu.hugegraph.util.ConfigUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Events;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.baidu.hugegraph.util.collection.CollectionFactory;

import io.fabric8.kubernetes.api.model.Namespace;

public final class GraphManager {

    private static final Logger LOG = Log.logger(RestServer.class);

    public static final String DELIMETER = "-";

    private final String cluster;
    private final String graphsDir;
    private final boolean startIgnoreSingleGraphError;
    private final boolean graphLoadFromLocalConfig;
    private final Map<String, GraphSpace> graphSpaces;
    private final Map<String, Service> services;
    private final Map<String, Graph> graphs;
    private final Set<String> localGraphs;
    private final Set<String> removingGraphs;
    private final Set<String> creatingGraphs;
    private final HugeAuthenticator authenticator;
    private final AuthManager authManager;
    private final MetaManager metaManager = MetaManager.instance();
    private final K8sManager k8sManager = K8sManager.instance();

    private final EventHub eventHub;

    private final Id serverId;
    private final NodeRole serverRole;

    public GraphManager(HugeConfig conf, EventHub hub) {
        String server = conf.get(ServerOptions.SERVER_ID);
        String role = conf.get(ServerOptions.SERVER_ROLE);
        this.startIgnoreSingleGraphError = conf.get(
                ServerOptions.SERVER_START_IGNORE_SINGLE_GRAPH_ERROR);
        E.checkArgument(server != null && !server.isEmpty(),
                        "The server name can't be null or empty");
        E.checkArgument(role != null && !role.isEmpty(),
                        "The server role can't be null or empty");
        this.serverId = IdGenerator.of(server);
        this.serverRole = NodeRole.valueOf(role.toUpperCase());
        this.graphsDir = conf.get(ServerOptions.GRAPHS);
        this.cluster = conf.get(ServerOptions.CLUSTER);
        this.graphSpaces = new ConcurrentHashMap<>();
        this.services = new ConcurrentHashMap<>();
        this.graphs = new ConcurrentHashMap<>();
        this.removingGraphs = ConcurrentHashMap.newKeySet();
        this.creatingGraphs = ConcurrentHashMap.newKeySet();
        this.authenticator = HugeAuthenticator.loadAuthenticator(conf);
        this.eventHub = hub;
        this.listenChanges();

        // Init etcd client
        List<String> endpoints = conf.get(ServerOptions.META_ENDPOINTS);
        boolean useCa = conf.get(ServerOptions.META_USE_CA);
        String ca = null;
        String clientCa = null;
        String clientKey = null;
        if (useCa) {
            ca = conf.get(ServerOptions.META_CA);
            clientCa = conf.get(ServerOptions.META_CLIENT_CA);
            clientKey = conf.get(ServerOptions.META_CLIENT_KEY);
        }
        this.metaManager.connect(this.cluster, MetaManager.MetaDriverType.ETCD,
                                 ca, clientCa, clientKey, endpoints);
        String k8sUrl = conf.get(ServerOptions.SERVER_K8S_URL);
        boolean k8sUseCa = conf.get(ServerOptions.SERVER_K8S_USE_CA);
        String k8sCa = null;
        String k8sClientCa = null;
        String k8sClientKey = null;
        if (k8sUseCa) {
            k8sCa = conf.get(ServerOptions.SERVER_K8S_CA);
            k8sClientCa = conf.get(ServerOptions.SERVER_K8S_CLIENT_CA);
            k8sClientKey = conf.get(ServerOptions.SERVER_K8S_CLIENT_KEY);
        }
        String oltpImage = conf.get(ServerOptions.SERVER_K8S_OLTP_IMAGE);
        String olapImage = conf.get(ServerOptions.SERVER_K8S_OLAP_IMAGE);
        String storageImage = conf.get(ServerOptions.SERVER_K8S_STORAGE_IMAGE);
        this.k8sManager.connect(k8sUrl, k8sCa, k8sClientCa, k8sClientKey,
                                oltpImage, olapImage, storageImage);

        this.createDefaultGraphSpaceIfNeeded(conf);

        this.loadGraphSpaces();

        this.loadServices();

        if (this.authenticator != null) {
            this.authManager = this.authenticator.authManager();
        } else {
            this.authManager = null;
        }

        this.graphLoadFromLocalConfig =
                conf.get(ServerOptions.GRAPH_LOAD_FROM_LOCAL_CONFIG);
        if (this.graphLoadFromLocalConfig) {
            // Load graphs configured in local conf/graphs directory
            Map<String, String> graphConfigs =
                                ConfigUtil.scanGraphsDir(this.graphsDir);
            this.localGraphs = graphConfigs.keySet();
            this.loadGraphs(graphConfigs);
        } else {
            this.localGraphs = ImmutableSet.of();
        }

        // Load graphs configured in etcd
        this.loadGraphsFromMeta(this.graphConfigs());

        // this.installLicense(conf, "");
        // Raft will load snapshot firstly then launch election and replay log
        this.waitGraphsStarted();
        this.checkBackendVersionOrExit(conf);
        this.serverStarted();
        this.addMetrics(conf);
        // listen meta changes, e.g. watch dynamically graph add/remove
        this.listenMetaChanges();
    }

    public void createDefaultGraphSpaceIfNeeded(HugeConfig config) {
        Map<String, GraphSpace> graphSpaceConfigs =
                                this.metaManager.graphSpaceConfigs();
        GraphSpace graphSpace;
        if (graphSpaceConfigs.containsKey(DEFAULT_GRAPH_SPACE_NAME)) {
            return;
        }
        graphSpace = this.createGraphSpace(DEFAULT_GRAPH_SPACE_NAME,
                                           DEFAULT_GRAPH_SPACE_DESCRIPTION,
                                           Integer.MAX_VALUE, Integer.MAX_VALUE,
                                           Integer.MAX_VALUE, Integer.MAX_VALUE,
                                           Integer.MAX_VALUE, ImmutableMap.of());
        boolean useK8s = config.get(ServerOptions.SERVER_USE_K8S);
        if (!useK8s) {
            return;
        }
        String ns = config.get(ServerOptions.SERVER_DEFAULT_K8S_NAMESPACE);
        Namespace namespace = this.k8sManager.namespace(ns);
        if (namespace == null) {
            throw new HugeException(
                      "The config option: %s, value: %s does not exist",
                      ServerOptions.SERVER_DEFAULT_K8S_NAMESPACE.name(), ns);
        }
        graphSpace.oltpNamespace(ns);
        graphSpace.olapNamespace(ns);
        graphSpace.storageNamespace(ns);
        this.updateGraphSpace(graphSpace);
    }

    public void loadGraphSpaces() {
        Map<String, GraphSpace> graphSpaceConfigs =
                                this.metaManager.graphSpaceConfigs();
        this.graphSpaces.putAll(graphSpaceConfigs);
    }

    public void loadServices() {
        for (String graphSpace : this.graphSpaces.keySet()) {
            Map<String, Service> services = this.metaManager
                                                .services(graphSpace);
            for (Map.Entry<String, Service> entry : services.entrySet()) {
                this.services.put(serviceName(graphSpace, entry.getKey()),
                                  entry.getValue());
            }
        }
    }

    public void reload() {
        // Remove graphs from GraphManager
        for (String graph : this.graphs.keySet()) {
            String[] parts = graph.split(DELIMETER);
            this.dropGraph(parts[0], parts[1], false);
        }
        int count = 0;
        while (!this.graphs.isEmpty() && count++ < 10) {
            sleep1s();
        }
        if (!this.graphs.isEmpty()) {
            throw new HugeException("Failed to reload grahps, try later");
        }
        if (this.graphLoadFromLocalConfig) {
            // Load graphs configured in local conf/graphs directory
            this.loadGraphs(ConfigUtil.scanGraphsDir(this.graphsDir));
        }
        // Load graphs configured in etcd
        this.loadGraphsFromMeta(this.graphConfigs());
    }

    public void reload(String graphSpace, String name) {
        if (!this.graphs.containsKey(name)) {
            return;
        }
        // Remove graphs from GraphManager
        this.dropGraph(graphSpace, name, false);
        int count = 0;
        while (this.graphs.containsKey(name) && count++ < 10) {
            sleep1s();
        }
        if (this.graphs.containsKey(name)) {
            throw new HugeException("Failed to reload '%s', try later", name);
        }
        if (this.graphLoadFromLocalConfig) {
            // Load graphs configured in local conf/graphs directory
            Map<String, String> configs =
                    ConfigUtil.scanGraphsDir(this.graphsDir);
            if (configs.containsKey(name)) {
                this.loadGraphs(ImmutableMap.of(name, configs.get(name)));
            }
        }

        // Load graphs configured in etcd
        Map<String, Map<String, Object>> configs = this.graphConfigs();
        String graphName = graphName(graphSpace, name);
        if (configs.containsKey(graphName)) {
            this.loadGraphsFromMeta(
                    ImmutableMap.of(graphName, configs.get(graphName)));
        }
    }

    public synchronized Map<String, Map<String, Object>> graphConfigs() {
        Map<String, Map<String, Object>> configs =
                    CollectionFactory.newMap(CollectionType.EC);
        for (String graphSpace : this.graphSpaces.keySet()) {
            configs.putAll(this.metaManager.graphConfigs(graphSpace));
        }
        return configs;
    }

    public void destroy() {
        this.unlistenChanges();
    }

    private void listenChanges() {
        this.eventHub.listen(Events.GRAPH_CREATE, event -> {
            LOG.info("RestServer accepts event 'graph.create'");
            event.checkArgs(String.class, HugeGraph.class);
            String name = (String) event.args()[0];
            HugeGraph graph = (HugeGraph) event.args()[1];
            this.graphs.putIfAbsent(name, graph);
            return null;
        });
        this.eventHub.listen(Events.GRAPH_DROP, event -> {
            LOG.info("RestServer accepts event 'graph.drop'");
            event.checkArgs(String.class);
            String name = (String) event.args()[0];
            HugeGraph graph = (HugeGraph) this.graphs.remove(name);
            if (graph == null) {
                return null;
            }
            try {
                graph.close();
            } catch (Exception e) {
                LOG.warn("Failed to close graph", e);
            }
            return null;
        });
    }

    private void unlistenChanges() {
        this.eventHub.unlisten(Events.GRAPH_CREATE);
        this.eventHub.unlisten(Events.GRAPH_DROP);
    }

    private void listenMetaChanges() {
        this.metaManager.listenGraphSpaceAdd(this::graphSpaceAddHandler);
        this.metaManager.listenGraphSpaceRemove(this::graphSpaceRemoveHandler);
        this.metaManager.listenGraphSpaceUpdate(this::graphSpaceUpdateHandler);

        this.metaManager.listenServiceAdd(this::serviceAddHandler);
        this.metaManager.listenServiceRemove(this::serviceRemoveHandler);
        this.metaManager.listenServiceUpdate(this::serviceUpdateHandler);

        this.metaManager.listenGraphAdd(this::graphAddHandler);
        this.metaManager.listenGraphRemove(this::graphRemoveHandler);
    }

    public void loadGraphs(final Map<String, String> graphConfs) {
        for (Map.Entry<String, String> conf : graphConfs.entrySet()) {
            String name = conf.getKey();
            String path = conf.getValue();
            HugeFactory.checkGraphName(name, "rest-server.properties");
            try {
                this.loadGraph(name, path);
            } catch (HugeException e) {
                if (!this.startIgnoreSingleGraphError) {
                    throw e;
                }
                LOG.error(String.format("Failed to load graph '%s' from local",
                                        name), e);
            }
        }
    }

    public void loadGraphsFromMeta(Map<String, Map<String, Object>> graphConfs) {
        for (Map.Entry<String, Map<String, Object>> conf :
                graphConfs.entrySet()) {
            String[] parts = conf.getKey().split(DELIMETER);
            Map<String, Object> config = conf.getValue();
            HugeFactory.checkGraphName(parts[1], "meta server");
            try {
                this.createGraph(parts[0], parts[1], config, false);
            } catch (HugeException e) {
                if (!this.startIgnoreSingleGraphError) {
                    throw e;
                }
                LOG.error(String.format("Failed to load graph '%s' from " +
                                        "meta server", parts[1]), e);
            }
        }
    }

    public void waitGraphsStarted() {
        this.graphs.values().forEach(g -> {
            try {
                HugeGraph graph = (HugeGraph) g;
                graph.waitStarted();
            } catch (HugeException e) {
                if (!this.startIgnoreSingleGraphError) {
                    throw e;
                }
                LOG.error("Failed to wait graph started", e);
            }
        });
    }

    public GraphSpace createGraphSpace(String name, String description,
                                       int cpuLimit, int memoryLimit,
                                       int storageLimit,
                                       int maxGraphNumber,
                                       int maxRoleNumber,
                                       Map<String, Object> configs) {
        GraphSpace space = new GraphSpace(name, description, cpuLimit,
                                          memoryLimit, storageLimit,
                                          maxGraphNumber, maxRoleNumber,
                                          configs);
        return this.createGraphSpace(space);
    }

    public GraphSpace updateGraphSpace(GraphSpace space) {
        String name = space.name();
        this.metaManager.addGraphSpaceConfig(name, space);
        this.metaManager.updateGraphSpace(name);
        this.graphSpaces.put(name, space);
        return space;
    }

    public GraphSpace createGraphSpace(GraphSpace space) {
        String name = space.name();
        this.metaManager.addGraphSpaceConfig(name, space);
        this.metaManager.addGraphSpace(name);
        this.graphSpaces.put(name, space);
        return space;
    }

    public void clearGraphSpace(String name) {
        // TODO: clear all roles

        // Clear all graphs
        for (String key : this.graphs.keySet()) {
            if (key.startsWith(name)) {
                String[] parts = key.split(DELIMETER);
                this.dropGraph(parts[0], parts[1], true);
            }
        }

        // Clear all services
        for (String key : this.services.keySet()) {
            if (key.startsWith(name)) {
                String[] parts = key.split(DELIMETER);
                this.dropService(parts[0], parts[1]);
            }
        }
    }

    public void dropGraphSpace(String name) {
        this.clearGraphSpace(name);
        this.metaManager.removeGraphSpaceConfig(name);
        this.metaManager.removeGraphSpace(name);
        this.graphSpaces.remove(name);
    }

    public Service createService(String graphSpace, Service service) {
        String name = service.name();
        GraphSpace gs = this.metaManager.graphSpace(graphSpace);

        // TODO: grab distributed etcd lock
        if (gs.tryOfferResourceFor(service)) {
            this.metaManager.updateGraphSpaceConfig(graphSpace, gs);
            this.metaManager.updateGraphSpace(graphSpace);
        } else {
            throw new HugeException("Not enough resources for service '%s'",
                                    service);
        }

        Set<String> urls = this.k8sManager.startService(gs, service);
        service.urls(urls);
        this.metaManager.addServiceConfig(graphSpace, service);
        this.metaManager.addService(graphSpace, name);
        this.services.put(serviceName(graphSpace, name), service);

        return service;
    }

    public void dropService(String graphSpace, String name) {
        GraphSpace gs = this.graphSpace(graphSpace);
        Service service = this.metaManager.service(graphSpace, name);
        this.k8sManager.stopService(gs, service);
        this.metaManager.removeServiceConfig(graphSpace, name);
        this.metaManager.removeService(graphSpace, name);
        this.services.remove(serviceName(graphSpace, name));

        // TODO: grab distributed etcd lock
        gs.recycleResourceFor(service);
        this.metaManager.updateGraphSpaceConfig(graphSpace, gs);
        this.metaManager.updateGraphSpace(graphSpace);
    }

    public HugeGraph createGraph(String graphSpace, String name,
                                 Map<String, Object> configs, boolean init) {
        GraphSpace gs = this.graphSpace(graphSpace);
        if (!gs.tryOfferGraph()) {
            throw new HugeException("Failed create graph due to Reach graph " +
                                    "limit for graph space '%s'", graphSpace);
        }
        E.checkArgumentNotNull(name, "The graph name can't be null");
        E.checkArgument(!this.graphs(graphSpace).contains(name),
                        "The graph name '%s' has existed", name);

        Configuration propConfig = this.buildConfig(configs);
        String storeName = propConfig.getString(CoreOptions.STORE.name());
        E.checkArgument(name.equals(storeName),
                        "The store name '%s' not match url name '%s'",
                        storeName, name);
        HugeConfig config = new HugeConfig(propConfig);
        this.checkOptions(config);
        HugeGraph graph = this.createGraph(graphSpace, config, init);
        graph.graphSpace(graphSpace);
        String graphName = graphName(graphSpace, name);
        if (init) {
            this.creatingGraphs.add(graphName);
            this.metaManager.addGraphConfig(graphSpace, name, configs);
            this.metaManager.addGraph(graphSpace, name);
        }
        this.graphs.put(graphName, graph);
        this.metaManager.updateGraphSpaceConfig(graphSpace, gs);
        // Let gremlin server and rest server context add graph
        this.eventHub.notify(Events.GRAPH_CREATE, graphName, graph);
        return graph;
    }

    private HugeGraph createGraph(String graphSpace, HugeConfig config,
                                  boolean init) {
        // open succeed will fill graph instance into HugeFactory graphs(map)
        HugeGraph graph;
        try {
            graph = (HugeGraph) GraphFactory.open(config);
        } catch (Throwable e) {
            LOG.error("Exception occur when open graph", e);
            throw e;
        }
        graph.graphSpace(graphSpace);
        if (this.requireAuthentication()) {
            /*
             * The main purpose is to call method
             * verifyPermission(HugePermission.WRITE, ResourceType.STATUS)
             * that is private
             */
            graph.mode(GraphMode.NONE);
        }
        if (init) {
            try {
                graph.initBackend();
                graph.serverStarted(this.serverId, this.serverRole);
            } catch (BackendException e) {
                try {
                    graph.close();
                } catch (Exception e1) {
                    if (graph instanceof StandardHugeGraph) {
                        ((StandardHugeGraph) graph).clearSchedulerAndLock();
                    }
                }
                HugeFactory.remove(graph);
                throw e;
            }
        }
        return graph;
    }

    private MapConfiguration buildConfig(Map<String, Object> configs) {
        return new MapConfiguration(configs);
    }

    private PropertiesConfiguration buildConfig(String configText) {
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
        // NOTE: rocksdb can't use same data path for different graph,
        // but it's not easy to check here
        String backend = config.get(CoreOptions.BACKEND);
        if (backend.equalsIgnoreCase("rocksdb")) {
            // TODO: should check data path...
        }
    }

    public void dropGraph(String graphSpace, String name, boolean clear) {
        HugeGraph g = this.graph(graphSpace, name);
        E.checkArgumentNotNull(g, "The graph '%s' doesn't exist", name);
        if (this.localGraphs.contains(name)) {
            throw new HugeException("Can't delete graph '%s' loaded from " +
                                    "local config. Please delete config file " +
                                    "and restart HugeGraphServer if really " +
                                    "want to delete it.", name);
        }

        String graphName = graphName(graphSpace, name);
        if (clear) {
            this.removingGraphs.add(graphName);
            try {
                this.metaManager.removeGraphConfig(graphSpace, name);
                this.metaManager.removeGraph(graphSpace, name);
            } catch (Exception e) {
                throw new HugeException(
                          "Failed to remove graph config of '%s'", name, e);
            }
            g.clearBackend();
            try {
                g.close();
            } catch (Exception e) {
                LOG.warn("Failed to close graph", e);
            }
        }
        GraphSpace gs = this.graphSpace(graphSpace);
        gs.recycleGraph();
        this.metaManager.updateGraphSpaceConfig(graphSpace, gs);
        // Let gremlin server and rest server context remove graph
        LOG.info("Notify remove graph {} by GRAPH_DROP event", name);
        this.eventHub.notify(Events.GRAPH_DROP, graphName);
    }

    public Set<String> graphSpaces() {
        return Collections.unmodifiableSet(this.graphSpaces.keySet());
    }

    public Set<String> services(String graphSpace) {
        Set<String> services = new HashSet<>();
        for (String key : this.services.keySet()) {
            String[] parts = key.split(DELIMETER);
            if (parts[0].equals(graphSpace)) {
                services.add(parts[1]);
            }
        }
        return services;
    }

    public Service service(String graphSpace, String name) {
        String key = String.join(DELIMETER, graphSpace, name);
        Service service = this.services.get(key);
        if (service == null) {
            service = this.metaManager.service(graphSpace, name);
        }
        return service;
    }

    public Set<HugeGraph> graphs() {
        Set<HugeGraph> graphs = new HashSet<>();
        for (Graph g : this.graphs.values()) {
            graphs.add((HugeGraph) g);
        }
        return graphs;
    }

    public Set<String> graphs(String graphSpace) {
        Set<String> graphs = new HashSet<>();
        for (String key : this.graphs.keySet()) {
            String[] parts = key.split(DELIMETER);
            if (parts[0].equals(graphSpace)) {
                graphs.add(parts[1]);
            }
        }
        return graphs;
    }

    public HugeGraph graph(String graphSpace, String name) {
        String key = String.join(DELIMETER, graphSpace, name);
        Graph graph = this.graphs.get(key);
        if (graph == null) {
            return null;
        } else if (graph instanceof HugeGraph) {
            return (HugeGraph) graph;
        }
        throw new NotSupportException("graph instance of %s", graph.getClass());
    }

    public GraphSpace graphSpace(String name) {
        GraphSpace space = this.graphSpaces.get(name);
        if (space == null) {
            space = this.metaManager.graphSpace(name);
        }
        return space;
    }

    public Serializer serializer() {
        return JsonSerializer.instance();
    }

    public Serializer serializer(Graph g, Map<String, Object> debugMeasure) {
        return JsonSerializer.instance(debugMeasure);
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

    public void close() {}

    private HugeAuthenticator authenticator() {
        E.checkState(this.authenticator != null,
                     "Unconfigured authenticator");
        return this.authenticator;
    }

    public boolean isAuthRequired() {
        return this.authenticator != null;
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
        final HugeGraph graph = (HugeGraph) GraphFactory.open(path);
        String graphName = graphName(DEFAULT_GRAPH_SPACE_NAME, name);
        graph.graphSpace(DEFAULT_GRAPH_SPACE_NAME);
        this.graphs.put(graphName, graph);
        LOG.info("Graph '{}' was successfully configured via '{}'", name, path);

        if (this.requireAuthentication() &&
            !(graph instanceof HugeGraphAuthProxy)) {
            LOG.warn("You may need to support access control for '{}' with {}",
                     path, HugeFactoryAuthProxy.GRAPH_FACTORY);
        }
    }

    private void checkBackendVersionOrExit(HugeConfig config) {
        for (Graph g : this.graphs.values()) {
            try {
                // TODO: close tx from main thread
                HugeGraph hugegraph = (HugeGraph) g;
                if (!hugegraph.backendStoreFeatures().supportsPersistence()) {
                    hugegraph.initBackend();
                    if (this.requireAuthentication()) {
                        String token =
                               config.get(ServerOptions.AUTH_ADMIN_TOKEN);
                        try {
                            // this.authenticator.initAdminUser(token);
                        } catch (Exception e) {
                            throw new BackendException(
                                      "The backend store of '%s' can't " +
                                      "initialize admin user",
                                      hugegraph.name());
                        }
                    }
                }
                BackendStoreSystemInfo info = hugegraph.backendStoreSystemInfo();
                if (!info.exists()) {
                    throw new BackendException(
                              "The backend store of '%s' has not been " +
                              "initialized", hugegraph.name());
                }
                if (!info.checkVersion()) {
                    throw new BackendException(
                              "The backend store version is inconsistent");
                }
            } catch (HugeException e) {
                if (!this.startIgnoreSingleGraphError) {
                    throw e;
                }
                LOG.error(String.format(
                          "Failed to check backend version for graph '%s'",
                          ((HugeGraph) g).name()), e);
            }
        }
    }

    private void serverStarted() {
        for (Graph graph : this.graphs.values()) {
            try {
                HugeGraph hugegraph = (HugeGraph) graph;
                assert hugegraph != null;
                hugegraph.serverStarted(this.serverId, this.serverRole);
            } catch (HugeException e) {
                if (!this.startIgnoreSingleGraphError) {
                    throw e;
                }
                LOG.error(String.format(
                          "Failed to server started for graph '%s'", graph), e);
            }
        }
    }

    private <T> void graphSpaceAddHandler(T response) {
        List<String> names = this.metaManager
                                 .extractGraphSpacesFromResponse(response);
        for (String gs : names) {
            GraphSpace graphSpace = this.metaManager.getGraphSpaceConfig(gs);
            this.graphSpaces.put(gs, graphSpace);
        }
    }

    private <T> void graphSpaceRemoveHandler(T response) {
        List<String> names = this.metaManager
                .extractGraphSpacesFromResponse(response);
        for (String gs : names) {
            this.graphSpaces.remove(gs);
        }
    }

    private <T> void graphSpaceUpdateHandler(T response) {
        List<String> names = this.metaManager
                                 .extractGraphSpacesFromResponse(response);
        for (String gs : names) {
            GraphSpace graphSpace = this.metaManager.getGraphSpaceConfig(gs);
            this.graphSpaces.put(gs, graphSpace);
        }
    }

    private <T> void serviceAddHandler(T response) {
        List<String> names = this.metaManager
                             .extractServicesFromResponse(response);
        Service service;
        for (String s : names) {
            String[] parts = s.split(DELIMETER);
            service = this.metaManager.getServiceConfig(parts[0], parts[1]);
            this.services.put(s, service);
        }
    }

    private <T> void serviceRemoveHandler(T response) {
        List<String> names = this.metaManager
                             .extractServicesFromResponse(response);
        for (String s : names) {
            this.services.remove(s);
        }
    }

    private <T> void serviceUpdateHandler(T response) {
        List<String> names = this.metaManager
                             .extractServicesFromResponse(response);
        Service service;
        for (String s : names) {
            String[] parts = s.split(DELIMETER);
            service = this.metaManager.getServiceConfig(parts[0], parts[1]);
            this.services.put(s, service);
        }
    }

    private <T> void graphAddHandler(T response) {
        List<String> names = this.metaManager
                                 .extractGraphsFromResponse(response);
        for (String graphName : names) {
            if (this.graphs.containsKey(graphName) ||
                this.creatingGraphs.contains(graphName)) {
                this.creatingGraphs.remove(graphName);
                continue;
            }

            String[] parts = graphName.split(DELIMETER);
            Map<String, Object> config =
                    this.metaManager.getGraphConfig(parts[0], parts[1]);

            // Create graph without init
            try {
                HugeGraph graph = this.createGraph(parts[0], parts[1], config, false);
                graph.serverStarted(this.serverId, this.serverRole);
                graph.tx().close();
            } catch (HugeException e) {
                if (!this.startIgnoreSingleGraphError) {
                    throw e;
                }
                LOG.error(String.format(
                          "Failed to create graph '%s'", graphName), e);
            }
        }
    }

    private <T> void graphRemoveHandler(T response) {
        List<String> graphNames = this.metaManager
                                      .extractGraphsFromResponse(response);
        for (String graphName : graphNames) {
            if (!this.graphs.containsKey(graphName) ||
                this.removingGraphs.contains(graphName)) {
                this.removingGraphs.remove(graphName);
                continue;
            }

            // Remove graph without clear
            String[] parts = graphName.split(DELIMETER);
            try {
                this.dropGraph(parts[0], parts[1], false);
            } catch (HugeException e) {
                LOG.error(String.format(
                          "Failed to drop graph '%s'", graphName), e);
            }
        }
    }

    private void addMetrics(HugeConfig config) {
        final MetricManager metric = MetricManager.INSTANCE;
        // Force to add server reporter
        ServerReporter reporter = ServerReporter.instance(metric.getRegistry());
        reporter.start(60L, TimeUnit.SECONDS);

        // Add metrics for MAX_WRITE_THREADS
        int maxWriteThreads = config.get(ServerOptions.MAX_WRITE_THREADS);
        MetricsUtil.registerGauge(RestServer.class, "max-write-threads",
                                  () -> maxWriteThreads);

        // Add metrics for caches
        @SuppressWarnings({"rawtypes", "unchecked"})
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
        MetricsUtil.registerGauge(TaskManager.class, "workers",
                                  () -> TaskManager.instance().workerPoolSize());
        MetricsUtil.registerGauge(TaskManager.class, "pending-tasks",
                                  () -> TaskManager.instance().pendingTasks());
    }

    private void checkOptionsUnique(HugeConfig config,
                                    TypedOption<?, ?> option) {
        Object incomingValue = config.get(option);
        for (String graphName : this.graphs.keySet()) {
            String[] parts = graphName.split(DELIMETER);
            Object existedValue = this.graph(parts[0], parts[1]).option(option);
            E.checkArgument(!incomingValue.equals(existedValue),
                            "The option '%s' conflict with existed",
                            option.name());
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

    public static void sleep1s() {
        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    private static String serviceName(String graphSpace, String service) {
        return String.join(DELIMETER, graphSpace, service);
    }

    private static String graphName(String graphSpace, String graph) {
        return String.join(DELIMETER, graphSpace, graph);
    }
}
