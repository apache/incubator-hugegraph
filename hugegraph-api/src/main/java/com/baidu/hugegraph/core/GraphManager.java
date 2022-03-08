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
import static com.baidu.hugegraph.space.GraphSpace.DEFAULT_GRAPH_SPACE_SERVICE_NAME;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.baidu.hugegraph.k8s.K8sDriver;
import com.baidu.hugegraph.k8s.K8sDriverProxy;
import com.baidu.hugegraph.meta.lock.LockResult;
import com.baidu.hugegraph.pd.client.PDClient;
import com.baidu.hugegraph.pd.client.PDConfig;
import com.baidu.hugegraph.pd.grpc.discovery.NodeInfos;
import com.baidu.hugegraph.registerimpl.PdRegister;
import com.baidu.hugegraph.space.SchemaTemplate;
import com.baidu.hugegraph.traversal.optimize.HugeScriptTraversal;
import com.baidu.hugegraph.type.define.GraphReadMode;
import com.baidu.hugegraph.util.JsonUtil;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.RegisterConfig;
import com.baidu.hugegraph.StandardHugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.auth.AuthManager;
import com.baidu.hugegraph.auth.HugeAuthenticator;
import com.baidu.hugegraph.auth.HugeFactoryAuthProxy;
import com.baidu.hugegraph.auth.HugeGraphAuthProxy;
import com.baidu.hugegraph.auth.HugeAuthenticator.User;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.cache.Cache;
import com.baidu.hugegraph.backend.cache.CacheManager;
import com.baidu.hugegraph.backend.store.BackendStoreSystemInfo;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.config.TypedOption;
import com.baidu.hugegraph.dto.ServiceDTO;
import com.baidu.hugegraph.event.EventHub;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.io.HugeGraphSONModule;
import com.baidu.hugegraph.k8s.K8sManager;
import com.baidu.hugegraph.k8s.K8sRegister;
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
import com.baidu.hugegraph.util.ConfigUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Events;
import com.baidu.hugegraph.util.Log;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.baidu.hugegraph.util.collection.CollectionFactory;

import io.fabric8.kubernetes.api.model.Namespace;

public final class GraphManager {

    private static final Logger LOG = Log.logger(RestServer.class);

    public static final String NAME_REGEX = "^[a-z][a-z0-9_]{0,47}$";
    public static final String DELIMITER = "-";

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
    private final String serviceGraphSpace;
    private final String serviceID;
    private final String pdPeers;

    private final EventHub eventHub;

    private final String url;

    private HugeConfig config;

    private K8sDriver.CA ca;

    private String pdK8sServiceId;

    public GraphManager(HugeConfig conf, EventHub hub) {

        System.out.println("Init graph manager");

        E.checkArgumentNotNull(conf, "The config can't be null");
        this.config = conf;
        this.url = conf.get(ServerOptions.REST_SERVER_URL);
        this.startIgnoreSingleGraphError = conf.get(
                ServerOptions.SERVER_START_IGNORE_SINGLE_GRAPH_ERROR);
        this.graphsDir = conf.get(ServerOptions.GRAPHS);
        this.cluster = conf.get(ServerOptions.CLUSTER);
        this.graphSpaces = new ConcurrentHashMap<>();
        this.services = new ConcurrentHashMap<>();
        this.graphs = new ConcurrentHashMap<>();
        this.removingGraphs = ConcurrentHashMap.newKeySet();
        this.creatingGraphs = ConcurrentHashMap.newKeySet();
        this.authenticator = HugeAuthenticator.loadAuthenticator(conf);
        this.serviceGraphSpace = conf.get(ServerOptions.SERVICE_GRAPH_SPACE);
        this.serviceID = conf.get(ServerOptions.SERVICE_ID);
        this.pdPeers = conf.get(ServerOptions.PD_PEERS);
        this.eventHub = hub;
        this.listenChanges();

        this.initMetaManager(conf);
        this.initK8sManagerIfNeeded(conf);

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

    public void reload() {
        // Remove graphs from GraphManager
        for (String graph : this.graphs.keySet()) {
            String[] parts = graph.split(DELIMITER);
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
            this.loadGraphsFromMeta(ImmutableMap.of(graphName,
                                                    configs.get(graphName)));
        }
    }

    public void destroy() {
        this.unlistenChanges();
    }

    private void initMetaManager(HugeConfig conf) {
        List<String> endpoints = conf.get(ServerOptions.META_ENDPOINTS);
        boolean useCa = conf.get(ServerOptions.META_USE_CA);
        String ca = null;
        String clientCa = null;
        String clientKey = null;
        if (useCa) {
            ca = conf.get(ServerOptions.META_CA);
            clientCa = conf.get(ServerOptions.META_CLIENT_CA);
            clientKey = conf.get(ServerOptions.META_CLIENT_KEY);
            this.ca = new K8sDriver.CA(ca, clientCa, clientKey);
        }
        this.metaManager.connect(this.cluster, MetaManager.MetaDriverType.ETCD,
                                 ca, clientCa, clientKey, endpoints);
    }

    private void initK8sManagerIfNeeded(HugeConfig conf) {
        boolean useK8s = conf.get(ServerOptions.SERVER_USE_K8S);
        if (useK8s) {
            String oltpImage = conf.get(ServerOptions.SERVER_K8S_OLTP_IMAGE);
            String olapImage = conf.get(ServerOptions.SERVER_K8S_OLAP_IMAGE);
            String storageImage =
                   conf.get(ServerOptions.SERVER_K8S_STORAGE_IMAGE);
            this.k8sManager.connect(oltpImage, olapImage, storageImage, this.ca);
        }
    }

    private void createDefaultGraphSpaceIfNeeded(HugeConfig config) {
        Map<String, GraphSpace> graphSpaceConfigs =
                                this.metaManager.graphSpaceConfigs();
        GraphSpace graphSpace;
        if (graphSpaceConfigs.containsKey(DEFAULT_GRAPH_SPACE_SERVICE_NAME)) {
            return;
        }
        graphSpace = this.createGraphSpace(DEFAULT_GRAPH_SPACE_SERVICE_NAME,
                                           DEFAULT_GRAPH_SPACE_DESCRIPTION,
                                           Integer.MAX_VALUE, Integer.MAX_VALUE,
                                           Integer.MAX_VALUE, Integer.MAX_VALUE,
                                           Integer.MAX_VALUE, false,
                                           User.ADMIN.getName(),
                                           ImmutableMap.of());
        boolean useK8s = config.get(ServerOptions.SERVER_USE_K8S);
        if (!useK8s) {
            return;
        }
        String oltp = config.get(ServerOptions.SERVER_DEFAULT_OLTP_K8S_NAMESPACE);
        // oltp namespace
        Namespace oltpNamespace = this.k8sManager.namespace(oltp);
        if (oltpNamespace == null) {
            throw new HugeException(
                      "The config option: %s, value: %s does not exist",
                      ServerOptions.SERVER_DEFAULT_OLTP_K8S_NAMESPACE.name(), oltp);
        }
        graphSpace.oltpNamespace(oltp);
        // olap namespace
        String olap = config.get(ServerOptions.SERVER_DEFAULT_OLAP_K8S_NAMESPACE);
        Namespace olapNamespace = this.k8sManager.namespace(olap);
        if (olapNamespace == null) {
            throw new HugeException(
                "The config option: %s, value: %s does not exist",
                ServerOptions.SERVER_DEFAULT_OLAP_K8S_NAMESPACE.name(), olap);
        }
        graphSpace.olapNamespace(olap);
        // storage is same as oltp
        graphSpace.storageNamespace(oltp);
        this.updateGraphSpace(graphSpace);
    }

    private void loadGraphSpaces() {
        Map<String, GraphSpace> graphSpaceConfigs =
                                this.metaManager.graphSpaceConfigs();
        this.graphSpaces.putAll(graphSpaceConfigs);
    }

    private void loadServices() {
        for (String graphSpace : this.graphSpaces.keySet()) {
            Map<String, Service> services = this.metaManager
                                                .serviceConfigs(graphSpace);
            for (Map.Entry<String, Service> entry : services.entrySet()) {
                this.services.put(serviceName(graphSpace, entry.getKey()),
                                  entry.getValue());
            }
        }
        if (!this.services.containsKey(this.serviceID)) {
            Service service = new Service(this.serviceID, User.ADMIN.getName(),
                                          Service.ServiceType.OLTP,
                                          Service.DeploymentType.MANUAL);
            service.description(service.name());
            service.url(this.url);

            // register self to pd, should prior to etcd due to pdServiceId info
            this.registerServiceToPd(service);

            // register to etcd
            this.metaManager.addServiceConfig(this.serviceGraphSpace, service);
            this.metaManager.notifyServiceAdd(this.serviceGraphSpace,
                                              this.serviceID);
            // add to local cache since even-handler has not been registered now
            this.services.put(serviceName(this.serviceGraphSpace, service.name()), service);
        }
    }

    public boolean isAuth() {
        return this.graphSpace(this.serviceGraphSpace).auth();
    }

    private synchronized Map<String, Map<String, Object>> graphConfigs() {
        Map<String, Map<String, Object>> configs =
                    CollectionFactory.newMap(CollectionType.EC);
        for (String graphSpace : this.graphSpaces.keySet()) {
            configs.putAll(this.metaManager.graphConfigs(graphSpace));
        }
        return configs;
    }

    private void listenChanges() {
        this.eventHub.listen(Events.GRAPH_CREATE, event -> {
            LOG.info("RestServer accepts event 'graph.create'");
            event.checkArgs(String.class, HugeGraph.class);
            String name = (String) event.args()[0];
            HugeGraph graph = (HugeGraph) event.args()[1];
            graph.switchAuthManager(this.authManager);
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
        this.metaManager.listenGraphUpdate(this::graphUpdateHandler);

        this.metaManager.listenRestPropertiesUpdate(
                         this.serviceGraphSpace, this.serviceID,
                         this::restPropertiesHandler);
        this.metaManager.listenGremlinYamlUpdate(
                         this.serviceGraphSpace, this.serviceID,
                         this::gremlinYamlHandler);
        this.metaManager.listenAuthEvent(this::authHandler);
    }

    private void loadGraphs(final Map<String, String> graphConfigs) {
        for (Map.Entry<String, String> conf : graphConfigs.entrySet()) {
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

    private Date parseDate(Object o) {
        if (null == o) {
            return null;
        }
        String timeStr = String.valueOf(o);
        try {
            return HugeGraphSONModule.DATE_FORMAT.parse(timeStr);
        } catch (ParseException exc) {
            return null;
        }
    }

    private void loadGraphsFromMeta(
                 Map<String, Map<String, Object>> graphConfigs) {
        for (Map.Entry<String, Map<String, Object>> conf :
                                                    graphConfigs.entrySet()) {
            String[] parts = conf.getKey().split(DELIMITER);
            Map<String, Object> config = conf.getValue();

            String creator = String.valueOf(config.get("creator"));
            Date createTime = parseDate(config.get("create_time"));
            Date updateTime = parseDate(config.get("update_time"));


            HugeFactory.checkGraphName(parts[1], "meta server");
            try {
                HugeGraph graph = this.createGraph(parts[0], parts[1], creator, config, false);
                graph.createTime(createTime);
                graph.updateTime(updateTime);
            } catch (HugeException e) {
                if (!this.startIgnoreSingleGraphError) {
                    throw e;
                }
                LOG.error(String.format("Failed to load graph '%s' from " +
                                        "meta server", parts[1]), e);
            }
        }
    }

    private void waitGraphsStarted() {
        this.graphs.values().forEach(g -> {
            try {
                HugeGraph graph = (HugeGraph) g;
                graph.switchAuthManager(this.authManager);
                graph.waitStarted();
            } catch (HugeException e) {
                if (!this.startIgnoreSingleGraphError) {
                    throw e;
                }
                LOG.error("Failed to wait graph started", e);
            }
        });
    }

    private GraphSpace createGraphSpace(String name, String description,
                                        int cpuLimit, int memoryLimit,
                                        int storageLimit,
                                        int maxGraphNumber,
                                        int maxRoleNumber,
                                        boolean auth, String creator,
                                        Map<String, Object> configs) {
        checkGraphSpaceName(name);
        GraphSpace space = new GraphSpace(name, description, cpuLimit,
                                          memoryLimit, storageLimit,
                                          maxGraphNumber, maxRoleNumber,
                                          auth, creator, configs);
        return this.createGraphSpace(space);
    }

    private GraphSpace updateGraphSpace(GraphSpace space) {
        String name = space.name();
        this.metaManager.addGraphSpaceConfig(name, space);
        this.metaManager.notifyGraphSpaceUpdate(name);
        this.graphSpaces.put(name, space);
        return space;
    }

    private void attachK8sNamespace(String namespace) {
        if (!Strings.isNullOrEmpty(namespace)) {
            Namespace current = k8sManager.namespace(namespace);
            if (null == current) {
                current = k8sManager.createNamespace(namespace,
                    ImmutableMap.of());
                if (null == current) {
                    throw new HugeException("Cannot attach k8s namespace {}", namespace);
                }
                // start operator pod
                // read from computer-system or default ?
                // read from "hugegraph-computer-system" 
                // String containerName = "hugegraph-operator";
                // String imageName = "";

                k8sManager.createOperatorPod(namespace);
            }

        }
    }

    public GraphSpace createGraphSpace(GraphSpace space) {
        String name = space.name();
        checkGraphSpaceName(name);
        this.limitStorage(space, space.storageLimit);
        this.metaManager.addGraphSpaceConfig(name, space);
        this.metaManager.appendGraphSpaceList(name);

        boolean useK8s = config.get(ServerOptions.SERVER_USE_K8S);
        if (useK8s) {
            attachK8sNamespace(space.oltpNamespace());
            attachK8sNamespace(space.olapNamespace());
        }

        this.metaManager.notifyGraphSpaceAdd(name);
        this.graphSpaces.put(name, space);
        return space;
    }

    private void limitStorage(GraphSpace space, int storageLimit) {
        PDClient pdClient = PDClient.create(PDConfig.of(this.pdPeers)
                                    .setEnablePDNotify(true));
        try {
            pdClient.setGraphSpace(space.name(), storageLimit);
        } catch (Exception e) {
            LOG.error("Exception occur when set storage limit!", e);
        }
    }

    public void clearGraphSpace(String name) {
        // Clear all roles
        this.metaManager.clearGraphAuth(name);

        // Clear all schemaTemplate
        this.metaManager.clearSchemaTemplate(name);

        // Clear all graphs
        for (String key : this.graphs.keySet()) {
            if (key.startsWith(name)) {
                String[] parts = key.split(DELIMITER);
                this.dropGraph(parts[0], parts[1], true);
            }
        }

        // Clear all services
        for (String key : this.services.keySet()) {
            if (key.startsWith(name)) {
                String[] parts = key.split(DELIMITER);
                this.dropService(parts[0], parts[1]);
            }
        }
    }

    public void dropGraphSpace(String name) {
        this.clearGraphSpace(name);
        this.metaManager.removeGraphSpaceConfig(name);
        this.metaManager.clearGraphSpaceList(name);
        this.metaManager.notifyGraphSpaceRemove(name);
        this.graphSpaces.remove(name);
    }

    private void registerServiceToPd(Service service) {
        try {
            PdRegister register = PdRegister.getInstance();
            RegisterConfig config = new RegisterConfig()
                                    .setAppName(this.cluster)
                                    .setGrpcAddress(this.pdPeers)
                                    .setUrls(service.urls())
                                    .setLabelMap(ImmutableMap.of(
                                            PdRegisterLabel.REGISTER_TYPE.name(),   PdRegisterType.DDS.name(),
                                            PdRegisterLabel.GRAPHSPACE.name(),      this.serviceGraphSpace,
                                            PdRegisterLabel.SERVICE_NAME.name(),           service.name()
                                    ));
            String pdServiceId = register.registerService(config);
            service.pdServiceId(pdServiceId);
            LOG.debug("pd registered, serviceId is {}, going to validate", pdServiceId);
            Map<String, NodeInfos> infos = register.getServiceInfo(pdServiceId);
            
            for(Map.Entry<String, NodeInfos> entry : infos.entrySet()) {
                NodeInfos info = entry.getValue();
                info.getInfoList().forEach(node -> {
                    LOG.debug("Registered Info serviceId {}: appName: {} , id: {} , address: {}",
                       entry.getKey(), node.getAppName(), node.getId(), node.getAddress());
                });
            }
        } catch (Exception e) {
            LOG.error("Failed to register service to pd", e);
        }
    }

    public void registerK8StoPd() throws Exception {
        try {
            PdRegister pdRegister = PdRegister.getInstance();
            K8sRegister k8sRegister = K8sRegister.instance();

            k8sRegister.initHttpClient();
            String rawConfig = k8sRegister.loadConfigStr();

            Gson gson = new Gson();
            ServiceDTO serviceDTO = gson.fromJson(rawConfig, ServiceDTO.class);
            RegisterConfig config = new RegisterConfig();

            config
                .setNodePort(serviceDTO.getSpec().getPorts().get(0).getNodePort().toString())
                .setNodeName(serviceDTO.getSpec().getClusterIP())
                .setAppName(this.cluster)
                .setGrpcAddress(this.pdPeers)
                .setVersion(serviceDTO.getMetadata().getResourceVersion())
                .setLabelMap(ImmutableMap.of(
                        PdRegisterLabel.REGISTER_TYPE.name(),   PdRegisterType.NODE_PORT.name(),
                        PdRegisterLabel.GRAPHSPACE.name(),      this.serviceGraphSpace,
                        PdRegisterLabel.SERVICE_NAME.name(),    serviceDTO.getMetadata().getName()
                ));

            this.pdK8sServiceId = pdRegister.registerService(config);
        } catch (Exception e) {
            LOG.error("Register service k8s external info to pd failed!", e);
            throw e;
        }
    }

    public Service createService(String graphSpace, Service service) {
        String name = service.name();
        checkServiceName(name);

        if (null != service.urls() && service.urls().contains(this.url)) {
            throw new HugeException("url cannot be same as current url %s", this.url);
        }


        GraphSpace gs = this.metaManager.graphSpace(graphSpace);

        LockResult lock = this.metaManager.lock(this.cluster, graphSpace);
        try {
            if (gs.tryOfferResourceFor(service)) {
                this.metaManager.updateGraphSpaceConfig(graphSpace, gs);
                this.metaManager.notifyGraphSpaceUpdate(graphSpace);
            } else {
                throw new HugeException("Not enough resources for service '%s'",
                                        service);
            }
        } finally {
            this.metaManager.unlock(lock, this.cluster, graphSpace);
        }

        lock = this.metaManager.lock(this.cluster, graphSpace, name);
        try {
            if (service.k8s()) {
                List<String> endpoints = this.config.get(
                                         ServerOptions.META_ENDPOINTS);
                Set<String> urls = this.k8sManager.startService(
                                   gs, service, endpoints, this.cluster);
                if (!urls.isEmpty()) {
                    String url = urls.iterator().next();
                    String[] parts = url.split(":");
                    service.port(Integer.valueOf(parts[parts.length - 1]));
                }
                service.urls(urls);
            }
            // Register to pd. The order here is important since pdServiceId will be stored in etcd
            this.registerServiceToPd(service);
            // Persist to etcd
            this.metaManager.addServiceConfig(graphSpace, service);
            this.metaManager.notifyServiceAdd(graphSpace, name);
            this.services.put(serviceName(graphSpace, name), service);
        } finally {
            this.metaManager.unlock(lock, this.cluster, graphSpace, name);
        }

        return service;
    }

    public void dropService(String graphSpace, String name) {
        GraphSpace gs = this.graphSpace(graphSpace);
        Service service = this.metaManager.service(graphSpace, name);
        if (null == service) {
            return;
        }
        if (service.k8s()) {
            this.k8sManager.stopService(gs, service);
        }
        LockResult lock = this.metaManager.lock(this.cluster, graphSpace, name);
        this.metaManager.removeServiceConfig(graphSpace, name);
        this.metaManager.notifyServiceRemove(graphSpace, name);
        this.services.remove(serviceName(graphSpace, name));
        this.metaManager.unlock(lock, this.cluster, graphSpace, name);

        lock = this.metaManager.lock(this.cluster, graphSpace);
        gs.recycleResourceFor(service);
        this.metaManager.updateGraphSpaceConfig(graphSpace, gs);
        this.metaManager.notifyGraphSpaceUpdate(graphSpace);
        this.metaManager.unlock(lock, this.cluster, graphSpace);

        String pdServiceId = service.pdServiceId();
        LOG.debug("Going to unregister service {} from Pd", pdServiceId);
        if (StringUtils.isNotEmpty(pdServiceId)) {
            PdRegister register = PdRegister.getInstance();
            register.unregister(service.pdServiceId());
            LOG.debug("Service {} has been withdrew from Pd", pdServiceId);
        }
    }

    public HugeGraph createGraph(String graphSpace, String name, String creator,
                                 Map<String, Object> configs, boolean init) {
        checkGraphName(name);
        GraphSpace gs = this.graphSpace(graphSpace);
        if (!gs.tryOfferGraph()) {
            throw new HugeException("Failed create graph due to Reach graph " +
                                    "limit for graph space '%s'", graphSpace);
        }
        E.checkArgumentNotNull(name, "The graph name can't be null");
        E.checkArgument(!this.graphs(graphSpace).contains(name),
                        "The graph name '%s' has existed", name);

        configs.put(ServerOptions.PD_PEERS.name(), this.pdPeers);
        configs.put(CoreOptions.GRAPH_SPACE.name(), graphSpace);
        boolean auth = this.metaManager.graphSpace(graphSpace).auth();
        if (DEFAULT_GRAPH_SPACE_SERVICE_NAME.equals(graphSpace) || !auth) {
            configs.put("gremlin.graph", "com.baidu.hugegraph.HugeFactory");
        } else {
            configs.put("gremlin.graph", "com.baidu.hugegraph.auth.HugeFactoryAuthProxy");
        }

        configs.put("graphSpace", graphSpace);

        Configuration propConfig = this.buildConfig(configs);
        String storeName = propConfig.getString(CoreOptions.STORE.name());
        E.checkArgument(name.equals(storeName),
                        "The store name '%s' not match url name '%s'",
                        storeName, name);

        HugeConfig config = new HugeConfig(propConfig);
        this.checkOptions(graphSpace, config);
        HugeGraph graph = this.createGraph(graphSpace, config, this.authManager, init);
        graph.graphSpace(graphSpace);

        graph.creator(creator);
        graph.createTime(new Date());
        graph.refreshUpdateTime();

        String graphName = graphName(graphSpace, name);
        if (init) {
            this.creatingGraphs.add(graphName);
            configs.put("creator", graph.creator());
            configs.put("create_time", graph.createTime());
            configs.put("update_time", graph.updateTime());
            this.metaManager.addGraphConfig(graphSpace, name, configs);
            this.metaManager.notifyGraphAdd(graphSpace, name);
        }
        this.graphs.put(graphName, graph);
        this.metaManager.updateGraphSpaceConfig(graphSpace, gs);
        // Let gremlin server and rest server context add graph
        this.eventHub.notify(Events.GRAPH_CREATE, graphName, graph);

        String schema = propConfig.getString(
                        CoreOptions.SCHEMA_INIT_TEMPLATE.name());
        if (schema == null || schema.isEmpty()) {
            return graph;
        }
        String schemas = this.schemaTemplate(graphSpace, schema).schema();
        prepareSchema(graph, schemas);
        return graph;
    }

    public static void prepareSchema(HugeGraph graph, String gremlin) {
        Map<String, Object> bindings = ImmutableMap.of(
                                       "graph", graph,
                                       "schema", graph.schema());
        HugeScriptTraversal<?, ?> traversal = new HugeScriptTraversal<>(
                                              graph.traversal(),
                                              "gremlin-groovy", gremlin,
                                              bindings, ImmutableMap.of());
        while (traversal.hasNext()) {
            traversal.next();
        }
        try {
            traversal.close();
        } catch (Exception e) {
            throw new HugeException("Failed to init schema", e);
        }
    }

    private HugeGraph createGraph(String graphSpace, HugeConfig config,
                                  AuthManager authManager, boolean init) {
        // open succeed will fill graph instance into HugeFactory graphs(map)
        HugeGraph graph;
        try {
            graph = (HugeGraph) GraphFactory.open(config);
        } catch (Throwable e) {
            LOG.error("Exception occur when open graph", e);
            throw e;
        }
        graph.switchAuthManager(authManager);
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
                graph.serverStarted();
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

    private void checkOptions(String graphSpace, HugeConfig config) {
        // The store cannot be the same as the existing graph
        this.checkOptionsUnique(graphSpace, config, CoreOptions.STORE);
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
                this.metaManager.notifyGraphRemove(graphSpace, name);
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
        Set<String> result = new HashSet<>();
        for (String key : this.services.keySet()) {
            String[] parts = key.split(DELIMITER);
            if (parts[0].equals(graphSpace)) {
                result.add(parts[1]);
            }
        }
        return result;
    }

    public Service service(String graphSpace, String name) {
        String key = String.join(DELIMITER, graphSpace, name);
        Service service = this.services.get(key);
        if (service == null) {
            service = this.metaManager.service(graphSpace, name);
        }
        if (service.manual()) {
            return service;
        }
        GraphSpace gs = this.graphSpace(graphSpace);
        int running = this.k8sManager.podsRunning(gs, service);
        if (service.running() != running) {
            service.running(running);
            this.metaManager.updateServiceConfig(graphSpace, service);
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
            String[] parts = key.split(DELIMITER);
            if (parts[0].equals(graphSpace)) {
                graphs.add(parts[1]);
            }
        }
        return graphs;
    }

    public HugeGraph graph(String graphSpace, String name) {
        String key = String.join(DELIMITER, graphSpace, name);
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
        String graphName = graphName(DEFAULT_GRAPH_SPACE_SERVICE_NAME, name);
        graph.graphSpace(DEFAULT_GRAPH_SPACE_SERVICE_NAME);
        graph.switchAuthManager(this.authManager);
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
                hugegraph.serverStarted();
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
            String[] parts = s.split(DELIMITER);
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
            String[] parts = s.split(DELIMITER);
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

            String[] parts = graphName.split(DELIMITER);
            Map<String, Object> config =
                    this.metaManager.getGraphConfig(parts[0], parts[1]);
            Object objc = config.get("creator");
            String creator = null == objc ? GraphSpace.DEFAULT_CREATOR_NAME : String.valueOf(objc);


            // Create graph without init
            try {
                HugeGraph graph = this.createGraph(parts[0], parts[1], creator, config, false);
                graph.serverStarted();
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
            String[] parts = graphName.split(DELIMITER);
            try {
                this.dropGraph(parts[0], parts[1], false);
            } catch (HugeException e) {
                LOG.error(String.format(
                          "Failed to drop graph '%s'", graphName), e);
            }
        }
    }

    private <T> void graphUpdateHandler(T response) {
        List<String> graphNames = this.metaManager
                                      .extractGraphsFromResponse(response);
        for (String graphName : graphNames) {
            if (this.graphs.containsKey(graphName)) {
                Graph graph = this.graphs.get(graphName);
                if (graph instanceof HugeGraph) {
                    HugeGraph hugeGraph = (HugeGraph) graph;
                    String[] values =
                             graphName.split(MetaManager.META_PATH_JOIN);
                    Map<String, Object> configs =
                                this.metaManager.getGraphConfig(values[0],
                                                                values[1]);
                    String readMode = configs.get(
                           CoreOptions.GRAPH_READ_MODE.name()).toString();
                    hugeGraph.readMode(GraphReadMode.valueOf(readMode));
                }
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

    private void checkOptionsUnique(String graphSpace,
                                    HugeConfig config,
                                    TypedOption<?, ?> option) {
        Object incomingValue = config.get(option);
        for (String graphName : this.graphs.keySet()) {
            String[] parts = graphName.split(DELIMITER);
            HugeGraph hugeGraph = this.graph(graphSpace, parts[1]);
            if (hugeGraph == null) {
                continue;
            }
            Object existedValue = hugeGraph.option(option);
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

    private static void sleep1s() {
        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    private static String serviceName(String graphSpace, String service) {
        return String.join(DELIMITER, graphSpace, service);
    }

    private static String graphName(String graphSpace, String graph) {
        return String.join(DELIMITER, graphSpace, graph);
    }

    private static void checkGraphSpaceName(String name) {
        if (DEFAULT_GRAPH_SPACE_SERVICE_NAME.equals(name)) {
            return;
        }
        checkName(name, "graph space");
    }

    private static void checkServiceName(String name) {
        if (DEFAULT_GRAPH_SPACE_SERVICE_NAME.equals(name)) {
            return;
        }
        checkName(name, "service");
    }

    private static void checkGraphName(String name) {
        checkName(name, "graph");
    }

    private static void checkSchemaTemplateName(String name) {
        checkName(name, "schema template");
    }

    private static void checkName(String name, String type) {
        E.checkArgument(name.matches(NAME_REGEX),
                        "Invalid name '%s' for %s, valid name is up to 128 " +
                        "alpha-numeric characters and underscores and only " +
                        "letters are supported as first letter. " +
                        "Note: letter is lower case", name, type);
    }

    @SuppressWarnings("unchecked")
    private <T> void restPropertiesHandler(T response) {
        List<String> events = this.metaManager
                                  .extractGraphsFromResponse(response);
        try {
            for (String event : events) {
                if (StringUtils.isNotEmpty(event)) {
                    Map<String, Object> properties = JsonUtil.fromJson(event, Map.class);
                    HugeConfig conf = new HugeConfig(properties);
                    Boolean k8sApiEnable = conf.get(ServerOptions.K8S_API_ENABLE);
                    if (k8sApiEnable) {
                        GraphSpace gs = this.metaManager.graphSpace(this.serviceGraphSpace);
                        String namespace = gs.olapNamespace();
                        // conf.get(
                        //ServerOptions.K8S_KUBE_CONFIG);
                        String enableInternalAlgorithm = conf.get(
                               ServerOptions.K8S_ENABLE_INTERNAL_ALGORITHM);
                        String internalAlgorithmImageUrl = conf.get(
                               ServerOptions.K8S_INTERNAL_ALGORITHM_IMAGE_URL);
                        String internalAlgorithm = conf.get(
                               ServerOptions.K8S_INTERNAL_ALGORITHM);
                        Map<String, String> algorithms = conf.getMap(
                                    ServerOptions.K8S_ALGORITHMS);
                        K8sDriverProxy.setConfig(namespace,
                                                 enableInternalAlgorithm,
                                                 internalAlgorithmImageUrl,
                                                 internalAlgorithm,
                                                 algorithms);
                    } else {
                        K8sDriverProxy.disable();
                    }
                }
            }
        } catch (IOException e) {
            LOG.warn(e.toString());
        }
    }

    private <T> void gremlinYamlHandler(T response) {
        List<String> events = this.metaManager
                                  .extractGraphsFromResponse(response);
        for (String event : events) {
            // TODO: Restart gremlin server
        }
    }

    private <T> void authHandler(T response) {
        List<String> events = this.metaManager
                                  .extractGraphsFromResponse(response);
        for (String event : events) {
            Map<String, Object> properties =
                        JsonUtil.fromJson(event, Map.class);
            MetaManager.AuthEvent authEvent = new MetaManager.AuthEvent(properties);
            if (this.authManager != null) {
                this.authManager.processEvent(authEvent);
            }
        }
    }

    public Map<String, Object> restProperties(String graphSpace) {
        Map<String, Object> map;
        map = this.metaManager.restProperties(graphSpace, this.serviceID);
        return map == null ? new HashMap<>() : map;
    }

    public Map<String, Object> restProperties(String graphSpace,
                                              Map<String, Object> properties) {
        return this.metaManager.restProperties(graphSpace,
                                               this.serviceID,
                                               properties);
    }

    public Map<String, Object> deleteRestProperties(String graphSpace,
                                                    String key) {
        Map<String, Object> map;
        map = this.metaManager.deleteRestProperties(graphSpace,
                                                    this.serviceID,
                                                    key);
        return map == null ? new HashMap<>() : map;
    }

    public String gremlinYaml(String graphSpace) {
        return this.metaManager.gremlinYaml(graphSpace, this.serviceID);
    }

    public String gremlinYaml(String graphSpace, String yaml) {
        return this.metaManager.gremlinYaml(graphSpace, this.serviceID, yaml);
    }

    public Set<String> schemaTemplates(String graphSpace) {
        return this.metaManager.schemaTemplates(graphSpace);
    }

    public SchemaTemplate schemaTemplate(String graphSpace,
                                         String schemaTemplate) {

        return this.metaManager.schemaTemplate(graphSpace, schemaTemplate);
    }

    public void createSchemaTemplate(String graphSpace,
                                     SchemaTemplate schemaTemplate) {
        checkSchemaTemplateName(schemaTemplate.name());
        this.metaManager.addSchemaTemplate(graphSpace, schemaTemplate);
    }

    public void updateSchemaTemplate(String graphSpace, SchemaTemplate schemaTemplate) {
        this.metaManager.updateSchemaTemplate(graphSpace, schemaTemplate);
    }

    public void dropSchemaTemplate(String graphSpace, String name) {
        this.metaManager.removeSchemaTemplate(graphSpace, name);
    }

    public void graphReadMode(String graphSpace, String graphName,
                              GraphReadMode readMode) {
        try {
            Map<String, Object> configs =
                        this.metaManager.getGraphConfig(graphSpace, graphName);
            configs.put(CoreOptions.GRAPH_READ_MODE.name(), readMode);
            this.metaManager.updateGraphConfig(graphSpace, graphName, configs);
            this.metaManager.notifyGraphUpdate(graphSpace, graphName);
        } catch (Exception e) {
            LOG.warn("The graph not exist or local graph");
        }
    }

    public Map<String, Object> graphConfig(String graphSpace,
                                           String graphName) {
        return this.metaManager.getGraphConfig(graphSpace, graphName);
    }

    public String pdPeers() {
        return this.pdPeers;
    }


    private static enum PdRegisterType {

        NODE_PORT,
        DDS,
        ;
    }
    
    private static enum PdRegisterLabel {
        REGISTER_TYPE,
        GRAPHSPACE,
        SERVICE_NAME,
        ;
    }
}
