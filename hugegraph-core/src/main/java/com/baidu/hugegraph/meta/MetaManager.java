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

package com.baidu.hugegraph.meta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.auth.HugeAccess;
import com.baidu.hugegraph.auth.HugeBelong;
import com.baidu.hugegraph.auth.HugeGroup;
import com.baidu.hugegraph.auth.HugePermission;
import com.baidu.hugegraph.auth.HugeTarget;
import com.baidu.hugegraph.auth.HugeUser;
import com.baidu.hugegraph.auth.SchemaDefine;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.meta.lock.LockResult;
import com.baidu.hugegraph.space.GraphSpace;
import com.baidu.hugegraph.space.SchemaTemplate;
import com.baidu.hugegraph.space.Service;
import com.baidu.hugegraph.task.HugeTask;
import com.baidu.hugegraph.task.TaskPriority;
import com.baidu.hugegraph.task.TaskSerializer;
import com.baidu.hugegraph.task.TaskStatus;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;

import com.baidu.hugegraph.type.define.CollectionType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.collection.CollectionFactory;

import io.fabric8.kubernetes.api.model.Namespace;

public class MetaManager {

    private static final Logger LOG = Log.logger(MetaManager.class);

    public static final String META_PATH_DELIMITER = "/";
    public static final String META_PATH_JOIN = "-";

    public static final String META_PATH_HUGEGRAPH = "HUGEGRAPH";
    public static final String META_PATH_GRAPHSPACE = "GRAPHSPACE";
    public static final String META_PATH_GRAPHSPACE_LIST = "GRAPHSPACE_LIST";
    public static final String META_PATH_SERVICE = "SERVICE";
    public static final String META_PATH_SERVICE_CONF = "SERVICE_CONF";
    public static final String META_PATH_GRAPH_CONF = "GRAPH_CONF";
    public static final String META_PATH_CONF = "CONF";
    public static final String META_PATH_GRAPH = "GRAPH";
    public static final String META_PATH_AUTH = "AUTH";
    public static final String META_PATH_USER = "USER";
    public static final String META_PATH_GROUP = "GROUP";
    public static final String META_PATH_TARGET = "TARGET";
    public static final String META_PATH_BELONG = "BELONG";
    public static final String META_PATH_ACCESS = "ACCESS";
    public static final String META_PATH_K8S_BINDINGS = "BINDING";
    public static final String META_PATH_REST_PROPERTIES = "REST_PROPERTIES";
    public static final String META_PATH_GREMLIN_YAML = "GREMLIN_YAML";
    private static final String META_PATH_URLS = "URLS";
    public static final String META_PATH_SCHEMA_TEMPLATE = "SCHEMA_TEMPLATE";
    private static final String META_PATH_PD_PEERS = "HSTORE_PD_PEERS";
    public static final String META_PATH_TASK = "TASK";
    public static final String META_PATH_TASK_LOCK = "TASK_LOCK";

    public static final String META_PATH_AUTH_EVENT = "AUTH_EVENT";
    public static final String META_PATH_EVENT = "EVENT";
    public static final String META_PATH_ADD = "ADD";
    public static final String META_PATH_REMOVE = "REMOVE";
    public static final String META_PATH_UPDATE = "UPDATE";

    public static final String META_PATH_DDS = "DDS_HOST";
    public static final String META_PATH_KAFKA = "KAFKA";
    public static final String META_PATH_HOST = "BROKER_HOST";
    public static final String META_PATH_PORT = "BROKER_PORT";
    public static final String META_PATH_DATA_SYNC_ROLE = "DATA_SYNC_ROLE";
    public static final String META_PATH_SLAVE_SERVER_HOST = "SLAVE_SERVER_HOST";
    public static final String META_PATH_SLAVE_SERVER_PORT = "SLAVE_SERVER_PORT";
    public static final String META_PATH_SYNC_BROKER = "SYNC_BROKER";
    public static final String META_PATH_SYNC_STORAGE = "SYNC_STORAGE";

    private static final String TASK_STATUS_POSTFIX = "Status";
    private static final String TASK_PROGRESS_POSTFIX = "Progress";
    private static final String TASK_CONTEXT_POSTFIX = "Context";
    private static final String TASK_RETRY_POSTFIX = "Retry";

    public static final long LOCK_DEFAULT_LEASE = 30L;

    private MetaDriver metaDriver;
    private String cluster;

    private static final MetaManager INSTANCE = new MetaManager();

    public static MetaManager instance() {
        return INSTANCE;
    }

    private MetaManager() {
    }

    public synchronized void connect(String cluster, MetaDriverType type,
                                     String trustFile, String clientCertFile,
                                     String clientKeyFile, Object... args) {
        E.checkArgument(cluster != null && !cluster.isEmpty(),
                        "The cluster can't be null or empty");
        if (this.metaDriver == null) {
            this.cluster = cluster;

            switch (type) {
                case ETCD:
                    this.metaDriver = trustFile == null || trustFile.isEmpty() ?
                                      new EtcdMetaDriver(args) :
                                      new EtcdMetaDriver(trustFile,
                                                         clientCertFile,
                                                         clientKeyFile, args);
                    break;
                case PD:
                    // TODO: uncomment after implement PdMetaDriver
                    // this.metaDriver = new PdMetaDriver(args);
                    break;
                default:
                    throw new AssertionError(String.format(
                              "Invalid meta driver type: %s", type));
            }
        }
    }

    public <T> void listenGraphSpaceAdd(Consumer<T> consumer) {
        this.listen(this.graphSpaceAddKey(), consumer);
    }

    public <T> void listenGraphSpaceRemove(Consumer<T> consumer) {
        this.listen(this.graphSpaceRemoveKey(), consumer);
    }

    public <T> void listenGraphSpaceUpdate(Consumer<T> consumer) {
        this.listen(this.graphSpaceUpdateKey(), consumer);
    }

    public <T> void listenServiceAdd(Consumer<T> consumer) {
        this.listen(this.serviceAddKey(), consumer);
    }

    public <T> void listenServiceRemove(Consumer<T> consumer) {
        this.listen(this.serviceRemoveKey(), consumer);
    }

    public <T> void listenServiceUpdate(Consumer<T> consumer) {
        this.listen(this.serviceUpdateKey(), consumer);
    }

    public <T> void listenGraphAdd(Consumer<T> consumer) {
        this.listen(this.graphAddKey(), consumer);
    }

    public <T> void listenGraphUpdate(Consumer<T> consumer) {
        this.listen(this.graphUpdateKey(), consumer);
    }

    public <T> void listenGraphRemove(Consumer<T> consumer) {
        this.listen(this.graphRemoveKey(), consumer);
    }

    public <T> void listenRestPropertiesUpdate(String graphSpace,
                                               String serviceId,
                                               Consumer<T> consumer) {
        this.listen(this.restPropertiesKey(graphSpace, serviceId), consumer);
    }

    public <T> void listenGremlinYamlUpdate(String graphSpace,
                                            String serviceId,
                                            Consumer<T> consumer) {
        this.listen(this.gremlinYamlKey(graphSpace, serviceId), consumer);
    }

    public <T> void listenAuthEvent(Consumer<T> consumer) {
        this.listen(this.authEventKey(), consumer);
    }

    public <T> void listenTaskAdded(String graphSpace, String graphName, TaskPriority priority,  Consumer<T> consumer) {
        String prefix = this.taskEventKey(graphSpace, graphName, priority.toString());
        this.listenPrefix(prefix, consumer);
    }

    public <T> void listenKafkaConfig(Consumer<T> consumer) {
        String prefix = this.kafkaPrefixKey();
        this.listenPrefix(prefix, consumer);
    }

    private <T> void listen(String key, Consumer<T> consumer) {
        this.metaDriver.listen(key, consumer);
    }

    private <T> void listenPrefix(String prefix, Consumer<T> consumer) {
        this.metaDriver.listenPrefix(prefix, consumer);
    }



    public void bindOltpNamespace(GraphSpace graphSpace, Namespace namespace) {
        this.bindNamespace(graphSpace, namespace, BindingType.OLTP);
    }

    public void bindOlapNamespace(GraphSpace graphSpace, Namespace namespace) {
        this.bindNamespace(graphSpace, namespace, BindingType.OLAP);
    }

    public void bindStorageNamespace(GraphSpace graphSpace,
                                     Namespace namespace) {
        this.bindNamespace(graphSpace, namespace, BindingType.STORAGE);
    }

    private void bindNamespace(GraphSpace graphSpace, Namespace namespace,
                               BindingType type) {
        this.metaDriver.put(this.graphSpaceBindingsKey(graphSpace.name(), type),
                            JsonUtil.toJson(namespace.getMetadata().getName()));
    }

    public Map<String, GraphSpace> graphSpaceConfigs() {
        Map<String, String> keyValues = this.metaDriver.scanWithPrefix(
                                        this.graphSpaceConfPrefix());
        Map<String, GraphSpace> configs =
                    CollectionFactory.newMap(CollectionType.EC);
        for (Map.Entry<String, String> entry : keyValues.entrySet()) {
            String key = entry.getKey();
            String[] parts = key.split(META_PATH_DELIMITER);
            configs.put(parts[parts.length - 1],
                        JsonUtil.fromJson(entry.getValue(), GraphSpace.class));
        }
        return configs;
    }

    public Map<String, Service> serviceConfigs(String graphSpace) {
        Map<String, Service> serviceMap =  new HashMap<>();
        Map<String, String> keyValues = this.metaDriver.scanWithPrefix(
                            this.serviceConfPrefix(graphSpace));
        for (Map.Entry<String, String> entry : keyValues.entrySet()) {
            String key = entry.getKey();
            String[] parts = key.split(META_PATH_DELIMITER);
            serviceMap.put(parts[parts.length - 1],
                           JsonUtil.fromJson(entry.getValue(), Service.class));
        }
        return serviceMap;
    }

    public Map<String, Map<String, Object>> graphConfigs(String graphSpace) {
        Map<String, Map<String, Object>> configs =
                            CollectionFactory.newMap(CollectionType.EC);
        Map<String, String> keyValues = this.metaDriver.scanWithPrefix(
                                        this.graphConfPrefix(graphSpace));
        for (Map.Entry<String, String> entry : keyValues.entrySet()) {
            String key = entry.getKey();
            String[] parts = key.split(META_PATH_DELIMITER);
            String name = parts[parts.length - 1];
            String graphName = String.join("-", graphSpace, name);
            configs.put(graphName, configMap(entry.getValue()));
        }
        return configs;
    }

    public Set<String> schemaTemplates(String graphSpace) {
        Set<String> result = new HashSet<>();
        Map<String, String> keyValues = this.metaDriver.scanWithPrefix(
                this.schemaTemplatePrefix(graphSpace));
        for (String key : keyValues.keySet()) {
            String[] parts = key.split(META_PATH_DELIMITER);
            result.add(parts[parts.length - 1]);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public SchemaTemplate schemaTemplate(String graphSpace,
                                         String schemaTemplate) {
        String s = this.metaDriver.get(this.schemaTemplateKey(graphSpace,
                                                              schemaTemplate));
        if (s == null) {
            return null;
        }
        return SchemaTemplate.fromMap(JsonUtil.fromJson(s, Map.class));
    }

    public void addSchemaTemplate(String graphSpace,
                                  SchemaTemplate template) {
        String key = this.schemaTemplateKey(graphSpace, template.name());

        String data = this.metaDriver.get(key);
        if (null != data) {
            throw new HugeException("Cannot create schema template since it has been created");
        }

        this.metaDriver.put(this.schemaTemplateKey(graphSpace, template.name()),
                            JsonUtil.toJson(template.asMap()));
    }

    public void updateSchemaTemplate(String graphSpace, SchemaTemplate template) {
        String key = this.schemaTemplateKey(graphSpace, template.name());
        this.metaDriver.put(this.schemaTemplateKey(graphSpace, template.name()),
                            JsonUtil.toJson(template.asMap()));
    }

    public void removeSchemaTemplate(String graphSpace, String name) {
        this.metaDriver.delete(this.schemaTemplateKey(graphSpace, name));
    }

    public void clearSchemaTemplate(String graphSpace) {
        String prefix = this.schemaTemplatePrefix(graphSpace);
        this.metaDriver.deleteWithPrefix(prefix);
    }

    public <T> List<String> extractGraphSpacesFromResponse(T response) {
        return this.metaDriver.extractValuesFromResponse(response);
    }

    public <T> List<String> extractServicesFromResponse(T response) {
        return this.metaDriver.extractValuesFromResponse(response);
    }

    public <T> List<String> extractGraphsFromResponse(T response) {
        return this.metaDriver.extractValuesFromResponse(response);
    }

    public <T> Map<String, String> extractKVFromResponse(T response) {
        return this.metaDriver.extractKVFromResponse(response);
    }

    public GraphSpace getGraphSpaceConfig(String graphSpace) {
        String gs = this.metaDriver.get(this.graphSpaceConfKey(graphSpace));
        if (gs == null) {
            return null;
        }
        return JsonUtil.fromJson(gs, GraphSpace.class);
    }

    public String getServiceRawConfig(String graphSpace, String service) {
        return this.metaDriver.get(this.serviceConfKey(graphSpace, service));
    }

    public Service parseServiceRawConfig(String serviceRawConf) {
        return JsonUtil.fromJson(serviceRawConf, Service.class);
    }

    public Service getServiceConfig(String graphSpace, String service) {
        String s = this.getServiceRawConfig(graphSpace, service);
        return this.parseServiceRawConfig(s);
    }

    public Map<String, Object> getGraphConfig(String graphSpace, String graph) {
        return configMap(this.metaDriver.get(this.graphConfKey(graphSpace,
                                                               graph)));
    }

    public void addGraphConfig(String graphSpace, String graph,
                               Map<String, Object> configs) {
        this.metaDriver.put(this.graphConfKey(graphSpace, graph),
                            JsonUtil.toJson(configs));
    }

    public void updateGraphConfig(String graphSpace, String graph,
                                  Map<String, Object> configs) {
        this.metaDriver.put(this.graphConfKey(graphSpace, graph),
                            JsonUtil.toJson(configs));
    }

    public GraphSpace graphSpace(String name) {
        String space = this.metaDriver.get(this.graphSpaceConfKey(name));
        if (space == null) {
            return null;
        }
        return JsonUtil.fromJson(space, GraphSpace.class);
    }

    public void addGraphSpaceConfig(String name, GraphSpace space) {
        this.metaDriver.put(this.graphSpaceConfKey(name),
                            JsonUtil.toJson(space));
    }

    public void removeGraphSpaceConfig(String name) {
        this.metaDriver.delete(this.graphSpaceConfKey(name));
    }

    public void updateGraphSpaceConfig(String name, GraphSpace space) {
        this.metaDriver.put(this.graphSpaceConfKey(name),
                            JsonUtil.toJson(space));
    }

    public void notifyGraphSpaceAdd(String graphSpace) {
        this.metaDriver.put(this.graphSpaceAddKey(), graphSpace);
    }

    public void appendGraphSpaceList(String name) {
        String key = this.graphSpaceListKey(name);
        this.metaDriver.put(key, name);
    }

    public void clearGraphSpaceList(String name) {
        String key = this.graphSpaceListKey(name);
        this.metaDriver.delete(key);
    }

    public void notifyServiceAdd(String graphSpace, String name) {
        this.metaDriver.put(this.serviceAddKey(),
                            this.serviceName(graphSpace, name));
    }

    public void notifyServiceRemove(String graphSpace, String name) {
        this.metaDriver.put(this.serviceRemoveKey(),
                            this.serviceName(graphSpace, name));
    }

    public void notifyServiceUpdate(String graphSpace, String name) {
        this.metaDriver.put(this.serviceUpdateKey(),
                            this.serviceName(graphSpace, name));
    }

    public void notifyGraphSpaceRemove(String graphSpace) {
        this.metaDriver.put(this.graphSpaceRemoveKey(), graphSpace);
    }

    public void notifyGraphSpaceUpdate(String graphSpace) {
        this.metaDriver.put(this.graphSpaceUpdateKey(), graphSpace);
    }

    public Service service(String graphSpace, String name) {
        String service = this.metaDriver.get(this.serviceConfKey(graphSpace,
                                                                 name));
        if (service == null) {
            return null;
        }
        return JsonUtil.fromJson(service, Service.class);
    }

    public void addServiceConfig(String graphSpace, Service service) {
        this.metaDriver.put(this.serviceConfKey(graphSpace, service.name()),
                            JsonUtil.toJson(service));
    }

    public void removeServiceConfig(String graphSpace, String service) {
        this.metaDriver.delete(this.serviceConfKey(graphSpace, service));
    }

    public void updateServiceConfig(String graphSpace, Service service) {
        this.addServiceConfig(graphSpace, service);
    }

    public void removeGraphConfig(String graphSpace, String graph) {
        this.metaDriver.delete(this.graphConfKey(graphSpace, graph));
    }

    public void notifyGraphAdd(String graphSpace, String graph) {
        this.metaDriver.put(this.graphAddKey(),
                            this.graphName(graphSpace, graph));
    }

    public void notifyGraphRemove(String graphSpace, String graph) {
        this.metaDriver.put(this.graphRemoveKey(),
                            this.graphName(graphSpace, graph));
    }

    public void notifyGraphUpdate(String graphSpace, String graph) {
        this.metaDriver.put(this.graphUpdateKey(),
                            this.graphName(graphSpace, graph));
    }

    public LockResult lock(String... keys) {
        return this.lock(LOCK_DEFAULT_LEASE, keys);
    }

    public LockResult lock(long ttl, String... keys) {
        String key = String.join(META_PATH_DELIMITER, keys);
        return this.lock(key, ttl);
    }

    public LockResult lock(String key, long ttl) {
        LockResult lockResult = this.metaDriver.lock(key, ttl);
        if (!lockResult.lockSuccess()) {
            throw new HugeException("Failed to lock '%s'", key);
        }
        return lockResult;
    }

    public LockResult lock(String key) {
        return this.metaDriver.lock(key, LOCK_DEFAULT_LEASE);
    }

    /**
     * keep alive of given key & lease
     * @param key
     * @param lease
     * @return
     */
    public long keepAlive(String key, long lease) {
        return this.metaDriver.keepAlive(key, lease);
    }

    public void unlock(LockResult lockResult, String... keys) {
        String key = String.join(META_PATH_DELIMITER, keys);
        this.unlock(key, lockResult);
    }

    public void unlock(String key, LockResult lockResult) {
        this.metaDriver.unlock(key, lockResult);
    }

    private String graphSpaceAddKey() {
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_EVENT,
                           META_PATH_GRAPHSPACE, META_PATH_ADD);
    }

    private String graphSpaceRemoveKey() {
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_EVENT,
                           META_PATH_GRAPHSPACE, META_PATH_REMOVE);
    }

    private String graphSpaceUpdateKey() {
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_EVENT,
                           META_PATH_GRAPHSPACE, META_PATH_UPDATE);
    }

    private String serviceAddKey() {
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_EVENT,
                           META_PATH_SERVICE, META_PATH_ADD);
    }

    private String serviceRemoveKey() {
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_EVENT,
                           META_PATH_SERVICE, META_PATH_REMOVE);
    }

    private String serviceUpdateKey() {
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_EVENT,
                           META_PATH_SERVICE, META_PATH_UPDATE);
    }

    private String graphAddKey() {
        // HUGEGRAPH/{cluster}/EVENT/GRAPH/ADD
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_EVENT,
                           META_PATH_GRAPH, META_PATH_ADD);
    }

    private String graphRemoveKey() {
        // HUGEGRAPH/{cluster}/EVENT/GRAPH/REMOVE
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_EVENT,
                           META_PATH_GRAPH, META_PATH_REMOVE);
    }

    private String graphUpdateKey() {
        // HUGEGRAPH/{cluster}/EVENT/GRAPH/UPDATE
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_EVENT,
                           META_PATH_GRAPH, META_PATH_UPDATE);
    }

    private String graphSpaceConfKey(String name) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/CONF/{graphspace}
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE,
                           META_PATH_CONF, name);
    }

    private String graphSpaceConfPrefix() {
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE,
                           META_PATH_CONF);
    }

    private String serviceConfPrefix(String graphSpace) {
        return this.serviceConfKey(graphSpace, Strings.EMPTY);
    }

    private String graphConfPrefix(String graphSpace) {
        return this.graphConfKey(graphSpace, Strings.EMPTY);
    }

    private String schemaTemplatePrefix(String graphSpace) {
        return this.schemaTemplateKey(graphSpace, Strings.EMPTY);
    }

    private String graphSpaceBindingsKey(String name, BindingType type) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/CONF/{graphspace}
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, name,
                           META_PATH_K8S_BINDINGS, type.name());
    }

    private String graphSpaceBindingsServer(String name, BindingType type) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/CONF/{graphspace}
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, name,
                           META_PATH_K8S_BINDINGS, type.name(), META_PATH_URLS);
    }

    private String serviceConfKey(String graphSpace, String name) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/SERVICE_CONF
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE,
                           graphSpace, META_PATH_SERVICE_CONF, name);
    }

    private String graphConfKey(String graphSpace, String graph) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/GRAPH_CONF
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE,
                           graphSpace, META_PATH_GRAPH_CONF, graph);
    }

    private String schemaTemplateKey(String graphSpace, String schemaTemplate) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/SCHEMA_TEMPLATE
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE,
                           graphSpace, META_PATH_SCHEMA_TEMPLATE,
                           schemaTemplate);
    }

    private String restPropertiesKey(String graphSpace, String serviceId) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/SERVICE/
        // {serviceId}/REST_PROPERTIES
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE,
                           graphSpace, META_PATH_SERVICE, serviceId,
                           META_PATH_REST_PROPERTIES);
    }

    private String gremlinYamlKey(String graphSpace, String serviceId) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/SERVICE/
        // {serviceId}/GREMLIN_YAML
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE,
                           graphSpace, META_PATH_SERVICE, serviceId,
                           META_PATH_GREMLIN_YAML);
    }

    private String userKey(String name) {
        // HUGEGRAPH/{cluster}/AUTH/USER/{user}
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_AUTH, META_PATH_USER, name);
    }

    private String userListKey() {
        // HUGEGRAPH/{cluster}/AUTH/USER
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_AUTH, META_PATH_USER);
    }

    private String authPrefix(String graphSpace) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                            this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                            META_PATH_AUTH);
    }

    private String groupKey(String graphSpace, String group) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/GROUP/{group}
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                           META_PATH_AUTH, META_PATH_GROUP, group);
    }

    private String groupListKey(String graphSpace) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/GROUP
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                           META_PATH_AUTH, META_PATH_GROUP);
    }

    private String targetKey(String graphSpace, String target) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/TARGET/{target}
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                           META_PATH_AUTH, META_PATH_TARGET, target);
    }

    private String targetListKey(String graphSpace) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/TARGET
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                           META_PATH_AUTH, META_PATH_TARGET);
    }

    private String belongKey(String graphSpace, String belong) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/BELONG/{belong}
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                           META_PATH_AUTH, META_PATH_BELONG, belong);
    }

    private String belongListKey(String graphSpace) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/BELONG
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                           META_PATH_AUTH, META_PATH_BELONG);
    }

    private String belongListKeyByUser(String graphSpace, String userName) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/BELONG/{userName}
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                           META_PATH_AUTH, META_PATH_BELONG, userName + "->");
    }

    private String accessKey(String graphSpace, String access) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/ACCESS/{group->op->target}
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                           META_PATH_AUTH, META_PATH_ACCESS, access);
    }

    private String accessListKey(String graphSpace) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/ACCESS
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                           META_PATH_AUTH, META_PATH_ACCESS);
    }

    private String taskPriorityKey(String graphSpace, String graphName, String taskPriority, String taskId) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/{graphName}/TASK/{priority}/{id}
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH, this.cluster, META_PATH_GRAPHSPACE,
        graphSpace, graphName, META_PATH_TASK, taskPriority, taskId);
    }

    /**
     * All tasks
     * @return
     */
    private String taskBaseKey(String graphSpace, String graphName) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/{graphName}/TASK
        return  String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH, this.cluster, META_PATH_GRAPHSPACE,
        graphSpace, graphName, META_PATH_TASK);
    }

    /**
     * Task base key, include subdir of TASK_LOCK, {property}
     * @param graphSpace
     * @param taskId
     * @return
     */
    private String taskKey(String graphSpace, String graphName, String taskId) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/{graphName}/TASK/{id}
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH, this.cluster, META_PATH_GRAPHSPACE,
        graphSpace, graphName, META_PATH_TASK, taskId);
    }

    private String taskLockKey(String graphSpace, String graphName, String taskId) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/{graphName}/TASK/{id}/TASK_LOCK
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH, this.cluster, META_PATH_GRAPHSPACE,
        graphSpace, graphName, META_PATH_TASK, taskId, META_PATH_TASK_LOCK);
    }

    private String taskPropertyKey(String graphSpace, String graphName, String taskId, String property) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/{graphName}/TASK/{id}/{property}
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH, this.cluster, META_PATH_GRAPHSPACE,
        graphSpace, graphName, META_PATH_TASK, taskId, property);
    }

    private String taskListKey(String graphSpace, String graphName, String taskPriority) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/TASK/{priority}
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH, this.cluster, META_PATH_GRAPHSPACE,
        graphSpace, graphName, META_PATH_TASK, taskPriority);
    }

    private String taskStatusListKey(String graphSpace, String graphName, String taskId, TaskStatus status) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/TASK/{statusType}/{id}
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH, this.cluster, META_PATH_GRAPHSPACE,
        graphSpace, graphName, META_PATH_TASK, status.name(), taskId);
    }

    private String taskStatusListKey(String graphSpace, String graphName, TaskStatus status) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/TASK/{statusType}
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH, this.cluster, META_PATH_GRAPHSPACE,
        graphSpace, graphName, META_PATH_TASK, status.name());
    }

    private String taskEventKey(String graphSpace, String graphName, String taskPriority) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/TASK/{priority}
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH, this.cluster, META_PATH_GRAPHSPACE,
        graphSpace, graphName, META_PATH_TASK, taskPriority);
    }

    /**
     * Get DDS (eureka) host, format should be "ip:port", with no /
     * @return
     */
    private String ddsHostKey() {
        // HUGEGRAPH/{cluster}/DDS_HOST
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH, this.cluster, META_PATH_DDS);
    }

    private String hugeClusterRoleKey() {
        // HUGEGRAPH/{clusterRole}/KAFKA/DATA_SYNC_ROLE
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH, this.cluster, META_PATH_KAFKA, META_PATH_DATA_SYNC_ROLE);
    }

    private String kafkaPrefixKey() {
        // HUGEGRAPH/{cluster}/KAFKA
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH, this.cluster, META_PATH_KAFKA);
    }

    private String kafkaHostKey() {
        // HUGEGRAPH/{cluster}/KAFKA/BROKER_HOST
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH, this.cluster, META_PATH_KAFKA, META_PATH_HOST);
    }

    private String kafkaPortKey() {
        // HUGEGRAPH/{cluster}/KAFKA/BROKER_PORT
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH, this.cluster, META_PATH_KAFKA, META_PATH_PORT);
    }

    private String kafkaSlaveHostKey() {
        // HUGEGRAPH/{cluster}/KAFKA/SLAVE_SERVER_HOST
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH, this.cluster, META_PATH_KAFKA, META_PATH_SLAVE_SERVER_HOST);
    }

    private String kafkaSlavePortKey() {
        // HUGEGRAPH/{cluster}/KAFKA/SLAVE_SERVER_PORT
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH, this.cluster, META_PATH_KAFKA, META_PATH_SLAVE_SERVER_PORT);
    }

    public String kafkaSyncBrokerKey() {
        // HUGEGRAPH/{cluster}/KAFKA/SYNC_BROKER
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH, this.cluster, META_PATH_KAFKA, META_PATH_SYNC_BROKER);
    }

    public String kafkaSyncStorageKey() {
        // HUGEGRAPH/{cluster}/KAFKA/SYNC_STORAGE
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH, this.cluster, META_PATH_KAFKA, META_PATH_SYNC_STORAGE);
    }

    /**
     * Use to split task key and fill them back to task
     * @param taskKey
     * @return
     */
    private <V> void attachTaskKeyInfo(HugeTask<V> task, String taskKey) {
        String[] parts = taskKey.split(META_PATH_DELIMITER);
        if (parts.length < 8) {
            return;
        }
        String priorityStr = parts[6];
        String idStr = parts[7];

        /**
         * Only proceeding when id's are matching
         */
        if (task.id().asString().equals(idStr)) {
            task.priority(TaskPriority.valueOf(priorityStr));
        }
    }

    public <V> void attachTaskInfo(HugeTask<V> task, String taskKey) {
        this.attachTaskKeyInfo(task, taskKey);
    }

    private String accessListKeyByGroup(String graphSpace, String groupName) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/ACCESS/{groupName}
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                           META_PATH_AUTH, META_PATH_ACCESS, groupName + "->");
    }

    public String belongId(String userName, String groupName) {
        E.checkArgument(StringUtils.isNotEmpty(userName) &&
                        StringUtils.isNotEmpty(groupName),
                        "The user name '%s' or group name '%s' is empty",
                        userName, groupName);
        return String.join("->", userName, groupName);
    }

    public String accessId(String groupName, String targetName, HugePermission permission) {
        E.checkArgument(StringUtils.isNotEmpty(groupName) &&
                        StringUtils.isNotEmpty(targetName),
                        "The group name '%s' or target name '%s' is empty",
                        groupName, targetName);
        String code = String.valueOf(permission.code());
        return String.join("->", groupName, code, targetName);
    }

    public String authEventKey() {
        // HUGEGRAPH/{cluster}/AUTH_EVENT
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_AUTH_EVENT);
    }

    private String graphSpaceListKey() {
        // HUGEGRAPH/{cluster}/GRAPHSPACE_LIST
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE_LIST);
    }

    private String graphSpaceListKey(String name) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE_LIST/{graphspace}
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                            this.cluster, META_PATH_GRAPHSPACE_LIST, name);
    }

    private String hstorePDPeersKey() {
        // HUGEGRAPH/{cluster}/META_PATH_PD_PEERS
        return String.join(META_PATH_DELIMITER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_PD_PEERS);
    }

    private String graphName(String graphSpace, String name) {
        return String.join(META_PATH_JOIN, graphSpace, name);
    }

    private String serviceName(String graphSpace, String name) {
        return String.join(META_PATH_JOIN, graphSpace, name);
    }

    private String serialize(SchemaDefine.AuthElement element) {
        Map<String, Object> objectMap = element.asMap();
        return JsonUtil.toJson(objectMap);
    }

    private <V> String serialize(HugeTask<V> task) {
        String json = TaskSerializer.toJson(task);
        return json;
    }

    public static class AuthEvent {
        private String op; // ALLOW: CREATE | DELETE | UPDATE
        private String type; // ALLOW: USER | GROUP | TARGET | ACCESS | BELONG
        private String id;

        public AuthEvent(String op, String type, String id) {
            this.op = op;
            this.type = type;
            this.id = id;
        }

        public AuthEvent(Map<String, Object> properties) {
            this.op = properties.get("op").toString();
            this.type = properties.get("type").toString();
            this.id = properties.get("id").toString();
        }

        public String op() {
            return this.op;
        }

        public void op(String op) {
            this.op = op;
        }

        public String type() {
            return this.type;
        }

        public void type(String type) {
            this.type = type;
        }

        public String id() {
            return this.id;
        }

        public void id(String id) {
            this.id = id;
        }

        public Map<String, Object> asMap() {
            return ImmutableMap.of("op", this.op,
                                   "type", this.type,
                                   "id", this.id);
        }
    }

    private void putAuthEvent(AuthEvent event) {
        this.metaDriver.put(authEventKey(), JsonUtil.toJson(event.asMap()));
    }

    public void createUser(HugeUser user) throws IOException {
        String result = this.metaDriver.get(userKey(user.name()));
        E.checkArgument(StringUtils.isEmpty(result),
                        "The user name '%s' has existed", user.name());
        this.metaDriver.put(userKey(user.name()), serialize(user));

    }

    public HugeUser updateUser(HugeUser user) throws IOException {
        String result = this.metaDriver.get(userKey(user.name()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The user name '%s' does not existed", user.name());

        HugeUser ori = HugeUser.fromMap(JsonUtil.fromJson(result, Map.class));
        ori.update(new Date());
        ori.password(user.password());
        ori.phone(user.phone());
        ori.email(user.email());
        ori.avatar(user.avatar());
        ori.description(user.description());
        this.metaDriver.put(userKey(user.name()), serialize(ori));
        return ori;
    }

    public HugeUser deleteUser(Id id) throws IOException,
                               ClassNotFoundException {
        String result = this.metaDriver.get(userKey(id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The user name '%s' does not existed", id.asString());
        this.metaDriver.delete(userKey(id.asString()));
        this.putAuthEvent(new AuthEvent("DELETE", "USER", id.asString()));
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        HugeUser user = HugeUser.fromMap(map);
        return user;
    }

    public HugeUser findUser(String name)
                             throws IOException, ClassNotFoundException {
        String result = this.metaDriver.get(userKey(name));
        if (StringUtils.isEmpty(result)) {
            return null;
        }

        return HugeUser.fromMap(JsonUtil.fromJson(result, Map.class));
    }

    public List<HugeUser> listUsers(List<Id> ids) throws IOException,
                                    ClassNotFoundException {
        List<HugeUser> result = new ArrayList<>();
        Map<String, String> userMap =
                            this.metaDriver.scanWithPrefix(userListKey());
        for (Id id : ids) {
            if (userMap.containsKey(userKey(id.asString()))) {
                String value = userMap.get(userKey(id.asString()));
                Map<String, Object> map = JsonUtil.fromJson(value, Map.class);
                HugeUser user = HugeUser.fromMap(map);
                result.add(user);
            }
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    public List<HugeUser> listAllUsers(long limit)
                                       throws IOException,
                                       ClassNotFoundException {
        List<HugeUser> result = new ArrayList<>();
        Map<String, String> userMap =
                            this.metaDriver.scanWithPrefix(userListKey());
        for (Map.Entry<String, String> item : userMap.entrySet()) {
            if (limit >=0 && result.size() >= limit) {
                break;
            }
            Map<String, Object> map = JsonUtil.fromJson(item.getValue(),
                                                        Map.class);
            HugeUser user = HugeUser.fromMap(map);
            result.add(user);
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    public Id createGroup(String graphSpace, HugeGroup group)
                          throws IOException {
        String result = this.metaDriver.get(groupKey(graphSpace, group.name()));
        E.checkArgument(StringUtils.isEmpty(result),
                        "The group name '%s' has existed", group.name());
        this.metaDriver.put(groupKey(graphSpace, group.name()),
                            serialize(group));
        return IdGenerator.of(group.name());
    }

    @SuppressWarnings("unchecked")
    public HugeGroup updateGroup(String graphSpace, HugeGroup group)
                          throws IOException {
        String result = this.metaDriver.get(groupKey(graphSpace, group.name()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The group name '%s' is not existed", group.name());

        // only description and update-time could be updated
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        HugeGroup ori = HugeGroup.fromMap(map);
        ori.update(new Date());
        ori.description(group.description());
        this.metaDriver.put(groupKey(graphSpace, ori.name()),
                            serialize(ori));
        return ori;
    }

    @SuppressWarnings("unchecked")
    public HugeGroup deleteGroup(String graphSpace, Id id)
                                 throws IOException,
                                 ClassNotFoundException {
        String result = this.metaDriver.get(groupKey(graphSpace,
                                                     id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The group name '%s' is not existed", id.asString());
        this.metaDriver.delete(groupKey(graphSpace, id.asString()));
        this.putAuthEvent(new AuthEvent("DELETE", "GROUP", id.asString()));
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        return HugeGroup.fromMap(map);
    }

    @SuppressWarnings("unchecked")
    public HugeGroup findGroup(String graphSpace, Id id) {
        String result = this.metaDriver.get(groupKey(graphSpace,
                                                     id.asString()));
        if (StringUtils.isEmpty(result)) {
            return null;
        }
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        return HugeGroup.fromMap(map);
    }

    @SuppressWarnings("unchecked")
    public HugeGroup getGroup(String graphSpace, Id id)
                              throws IOException,
                              ClassNotFoundException {
        String result = this.metaDriver.get(groupKey(graphSpace,
                                                     id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The group name '%s' is not existed", id.asString());
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        return HugeGroup.fromMap(map);
    }

    @SuppressWarnings("unchecked")
    public List<HugeGroup> listGroups(String graphSpace, List<Id> ids)
                                      throws IOException,
                                      ClassNotFoundException {
        List<HugeGroup> result = new ArrayList<>();
        Map<String, String> groupMap =
                    this.metaDriver.scanWithPrefix(groupListKey(graphSpace));
        for (Id id : ids) {
            if (groupMap.containsKey(groupKey(graphSpace, id.asString()))) {
                String groupString = groupMap.get(groupKey(graphSpace,
                                                           id.asString()));
                Map<String, Object> map = JsonUtil.fromJson(groupString,
                                                            Map.class);
                HugeGroup group = HugeGroup.fromMap(map);
                result.add(group);
            }
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    public List<HugeGroup> listAllGroups(String graphSpace, long limit)
                                         throws IOException,
                                         ClassNotFoundException {
        List<HugeGroup> result = new ArrayList<>();
        Map<String, String> groupMap =
                            this.metaDriver.scanWithPrefix(groupListKey(graphSpace));
        for (Map.Entry<String, String> item : groupMap.entrySet()) {
            if (limit >=0 && result.size() >= limit) {
                break;
            }
            Map<String, Object> map = JsonUtil.fromJson(item.getValue(),
                                                        Map.class);
            HugeGroup group = HugeGroup.fromMap(map);
            result.add(group);
        }

        return result;
    }

    public Id createTarget(String graphSpace, HugeTarget target)
                           throws IOException {
        String result = this.metaDriver.get(targetKey(graphSpace,
                                                      target.name()));
        E.checkArgument(StringUtils.isEmpty(result),
                        "The target name '%s' has existed", target.name());
        this.metaDriver.put(targetKey(graphSpace, target.name()),
                            serialize(target));
        return target.id();
    }

    @SuppressWarnings("unchecked")
    public HugeTarget updateTarget(String graphSpace, HugeTarget target)
                           throws IOException {
        String result = this.metaDriver.get(targetKey(graphSpace,
                                                      target.name()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The target name '%s' is not existed", target.name());

        // only resources and update-time could be updated
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        HugeTarget ori = HugeTarget.fromMap(map);
        ori.update(new Date());
        ori.resources(target.resources());
        this.metaDriver.put(targetKey(graphSpace, target.name()),
                            serialize(ori));
        this.putAuthEvent(new AuthEvent("UPDATE", "TARGET",
                                        ori.id().asString()));
        return ori;
    }

    @SuppressWarnings("unchecked")
    public HugeTarget deleteTarget(String graphSpace, Id id)
                                   throws IOException,
                                   ClassNotFoundException {
        String result = this.metaDriver.get(targetKey(graphSpace,
                                                      id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The target name '%s' is not existed", id.asString());
        this.metaDriver.delete(targetKey(graphSpace, id.asString()));
        this.putAuthEvent(new AuthEvent("DELETE", "TARGET", id.asString()));
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        return HugeTarget.fromMap(map);
    }

    @SuppressWarnings("unchecked")
    public HugeTarget findTarget(String graphSpace, Id id) {
        String result = this.metaDriver.get(targetKey(graphSpace,
                                                      id.asString()));
        if (StringUtils.isEmpty(result)) {
            return null;
        }
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        return HugeTarget.fromMap(map);
    }

    @SuppressWarnings("unchecked")
    public HugeTarget getTarget(String graphSpace, Id id)
                                throws IOException,
                                ClassNotFoundException {
        String result = this.metaDriver.get(targetKey(graphSpace,
                                                      id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The target name '%s' is not existed", id.asString());
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        return HugeTarget.fromMap(map);
    }

    @SuppressWarnings("unchecked")
    public List<HugeTarget> listTargets(String graphSpace, List<Id> ids)
                                        throws IOException,
                                        ClassNotFoundException {
        List<HugeTarget> result = new ArrayList<>();
        Map<String, String> targetMap =
                    this.metaDriver.scanWithPrefix(targetListKey(graphSpace));
        for (Id id : ids) {
            if (targetMap.containsKey(targetKey(graphSpace, id.asString()))) {
                String targetString = targetMap.get(targetKey(graphSpace,
                                                              id.asString()));
                Map<String, Object> map = JsonUtil.fromJson(targetString,
                                                            Map.class);
                HugeTarget target = HugeTarget.fromMap(map);
                result.add(target);
            }
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    public List<HugeTarget> listAllTargets(String graphSpace, long limit)
                                           throws IOException,
                                           ClassNotFoundException {
        List<HugeTarget> result = new ArrayList<>();
        Map<String, String> targetMap =
                    this.metaDriver.scanWithPrefix(targetListKey(graphSpace));
        for (Map.Entry<String, String> item : targetMap.entrySet()) {
            if (limit >=0 && result.size() >= limit) {
                break;
            }
            Map<String, Object> map = JsonUtil.fromJson(item.getValue(),
                                                        Map.class);
            HugeTarget target = HugeTarget.fromMap(map);
            result.add(target);
        }

        return result;
    }

    public Id createBelong(String graphSpace, HugeBelong belong)
                           throws IOException, ClassNotFoundException {
        HugeUser user = this.findUser(belong.source().asString());
        E.checkArgument(user != null,
                        "The user name '%s' is not existed",
                        belong.source().asString());
        HugeGroup group = this.getGroup(graphSpace, belong.target());
        E.checkArgument(group != null,
                        "The group name '%s' is not existed",
                        belong.target().asString());

        String belongId = belongId(user.name(), group.name());
        String result = this.metaDriver.get(belongKey(graphSpace, belongId));
        E.checkArgument(StringUtils.isEmpty(result),
                        "The belong name '%s' has existed", belongId);
        this.metaDriver.put(belongKey(graphSpace, belongId), serialize(belong));
        this.putAuthEvent(new AuthEvent("CREATE", "BELONG", belongId));
        return IdGenerator.of(belongId);
    }

    @SuppressWarnings("unchecked")
    public HugeBelong updateBelong(String graphSpace, HugeBelong belong)
                           throws IOException, ClassNotFoundException {
        HugeUser user = this.findUser(belong.source().asString());
        E.checkArgument(user != null,
                        "The user name '%s' is not existed",
                        belong.source().asString());
        HugeGroup group = this.getGroup(graphSpace, belong.target());
        E.checkArgument(group != null,
                        "The group name '%s' is not existed",
                        belong.target().asString());

        String belongId = belongId(user.name(), group.name());
        String result = this.metaDriver.get(belongKey(graphSpace, belongId));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The belong name '%s' is not existed", belongId);

        // only description and update-time could be updated
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        HugeBelong ori = HugeBelong.fromMap(map);
        ori.update(new Date());
        ori.description(belong.description());
        this.metaDriver.put(belongKey(graphSpace, belongId), serialize(ori));
        return ori;
    }

    @SuppressWarnings("unchecked")
    public HugeBelong deleteBelong(String graphSpace, Id id)
                                   throws IOException,
                                   ClassNotFoundException {
        String result = this.metaDriver.get(belongKey(graphSpace,
                                                      id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The belong name '%s' is not existed", id.asString());
        this.metaDriver.delete(belongKey(graphSpace, id.asString()));
        this.putAuthEvent(new AuthEvent("DELETE", "BELONG", id.asString()));
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        return HugeBelong.fromMap(map);
    }

    @SuppressWarnings("unchecked")
    public HugeBelong getBelong(String graphSpace, Id id)
                                throws IOException,
                                ClassNotFoundException {
        String result = this.metaDriver.get(belongKey(graphSpace,
                                                      id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The belong name '%s' is not existed", id.asString());

        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        return HugeBelong.fromMap(map);
    }

    public boolean existBelong(String graphSpace, Id id) {
        String result = this.metaDriver.get(belongKey(graphSpace,
                                                      id.asString()));

        if (result == null) {
            return false;
        }

        return true;
    }

    @SuppressWarnings("unchecked")
    public List<HugeBelong> listBelong(String graphSpace, List<Id> ids)
                                       throws IOException,
                                       ClassNotFoundException {
        List<HugeBelong> result = new ArrayList<>();
        Map<String, String> belongMap =
                    this.metaDriver.scanWithPrefix(belongListKey(graphSpace));
        for (Id id : ids) {
            if (belongMap.containsKey(belongKey(graphSpace, id.asString()))) {
                String belongString = belongMap.get(belongKey(graphSpace,
                                                              id.asString()));
                Map<String, Object> map = JsonUtil.fromJson(belongString,
                                                            Map.class);
                HugeBelong belong = HugeBelong.fromMap(map);
                result.add(belong);
            }
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    public List<HugeBelong> listAllBelong(String graphSpace, long limit)
                                          throws IOException,
                                          ClassNotFoundException {
        List<HugeBelong> result = new ArrayList<>();
        Map<String, String> belongMap =
                    this.metaDriver.scanWithPrefix(belongListKey(graphSpace));
        for (Map.Entry<String, String> item : belongMap.entrySet()) {
            if (limit >=0 && result.size() >= limit) {
                break;
            }
            Map<String, Object> map = JsonUtil.fromJson(item.getValue(),
                                                        Map.class);
            HugeBelong belong = HugeBelong.fromMap(map);
            result.add(belong);
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    public List<HugeBelong> listBelongByUser(String graphSpace,
                                             Id user, long limit)
                                             throws IOException,
                                             ClassNotFoundException {
        List<HugeBelong> result = new ArrayList<>();

        String key = belongListKeyByUser(graphSpace, user.asString());

        Map<String, String> belongMap = this.metaDriver.scanWithPrefix(key);
        for (Map.Entry<String, String> item : belongMap.entrySet()) {
            if (limit >=0 && result.size() >= limit) {
                break;
            }
            Map<String, Object> map = JsonUtil.fromJson(item.getValue(),
                                                        Map.class);
            HugeBelong belong = HugeBelong.fromMap(map);
            result.add(belong);
        }

        return result;
    }

    public String groupFromBelong(String belongKey) {
        E.checkArgument(StringUtils.isNotEmpty(belongKey),
                        "The belong name '%s' is empty", belongKey);
        E.checkArgument(belongKey.contains("->"),
                        "The belong name '%s' is invalid", belongKey);
        String[] items = belongKey.split("->");
        E.checkArgument(items.length == 2,
                        "The belong name '%s' is invalid", belongKey);
        return items[1];
    }

    @SuppressWarnings("unchecked")
    public List<HugeBelong> listBelongByGroup(String graphSpace,
                                              Id group, long limit)
                                              throws IOException,
                                              ClassNotFoundException {
        List<HugeBelong> result = new ArrayList<>();
        Map<String, String> belongMap = this.metaDriver.scanWithPrefix(
                                        belongListKey(graphSpace));
        for (Map.Entry<String, String> item : belongMap.entrySet()) {
            if (limit >=0 && result.size() >= limit) {
                break;
            }
            String groupName = groupFromBelong(item.getKey());
            if (groupName.equals(group.asString())) {
                Map<String, Object> map = JsonUtil.fromJson(item.getValue(),
                                                            Map.class);
                HugeBelong belong = HugeBelong.fromMap(map);
                result.add(belong);
            }
        }

        return result;
    }

    public Id createAccess(String graphSpace, HugeAccess access)
                           throws IOException, ClassNotFoundException {
        HugeGroup group = this.getGroup(graphSpace, access.source());
        E.checkArgument(group != null,
                        "The group name '%s' is not existed",
                        access.source().asString());

        HugeTarget target = this.getTarget(graphSpace, access.target());
        E.checkArgument(target != null,
                        "The target name '%s' is not existed",
                        access.target().asString());

        String accessId = accessId(group.name(), target.name(),
                                   access.permission());
        String result = this.metaDriver.get(accessKey(graphSpace, accessId));
        E.checkArgument(StringUtils.isEmpty(result),
                        "The access name '%s' has existed", accessId);
        this.metaDriver.put(accessKey(graphSpace, accessId), serialize(access));
        this.putAuthEvent(new AuthEvent("CREATE", "ACCESS", accessId));
        return IdGenerator.of(accessId);
    }

    @SuppressWarnings("unchecked")
    public HugeAccess updateAccess(String graphSpace, HugeAccess access)
                                   throws IOException, ClassNotFoundException {
        HugeGroup group = this.getGroup(graphSpace, access.source());
        E.checkArgument(group != null,
                        "The group name '%s' is not existed",
                        access.source().asString());

        HugeTarget target = this.getTarget(graphSpace, access.target());
        E.checkArgument(target != null,
                        "The target name '%s' is not existed",
                        access.target().asString());

        String accessId = accessId(group.name(), target.name(),
                                   access.permission());
        String result = this.metaDriver.get(accessKey(graphSpace, accessId));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The access name '%s' is not existed", accessId);
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        HugeAccess existed = HugeAccess.fromMap(map);
        E.checkArgument(existed.permission().code() ==
                        access.permission().code(),
                        "The access name '%s' has existed", accessId);

        // only description and update-time could be updated
        Map<String, Object> oriMap = JsonUtil.fromJson(result, Map.class);
        HugeAccess ori = HugeAccess.fromMap(oriMap);
        ori.update(new Date());
        ori.description(access.description());
        this.metaDriver.put(accessKey(graphSpace, accessId), serialize(ori));
        return ori;
    }

    @SuppressWarnings("unchecked")
    public HugeAccess deleteAccess(String graphSpace, Id id)
                                   throws IOException, ClassNotFoundException {
        String result = this.metaDriver.get(accessKey(graphSpace,
                                                      id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The access name '%s' is not existed", id.asString());
        this.metaDriver.delete(accessKey(graphSpace, id.asString()));
        this.putAuthEvent(new AuthEvent("DELETE", "ACCESS", id.asString()));
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        return HugeAccess.fromMap(map);
    }

    @SuppressWarnings("unchecked")
    public HugeAccess findAccess(String graphSpace, Id id) {
        String result = this.metaDriver.get(accessKey(graphSpace,
                                                      id.asString()));
        if (StringUtils.isEmpty(result)) {
            return null;
        }
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        return HugeAccess.fromMap(map);
    }

    @SuppressWarnings("unchecked")
    public HugeAccess getAccess(String graphSpace, Id id)
                                throws IOException, ClassNotFoundException {
        String result = this.metaDriver.get(accessKey(graphSpace,
                                                      id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The access name '%s' is not existed", id.asString());
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        return HugeAccess.fromMap(map);
    }

    @SuppressWarnings("unchecked")
    public List<HugeAccess> listAccess(String graphSpace, List<Id> ids)
                                       throws IOException,
                                       ClassNotFoundException {
        List<HugeAccess> result = new ArrayList<>();
        Map<String, String> accessMap =
                    this.metaDriver.scanWithPrefix(accessListKey(graphSpace));
        for (Id id : ids) {
            if (accessMap.containsKey(accessKey(graphSpace, id.asString()))) {
                String accessString = accessMap.get(accessKey(graphSpace,
                                                              id.asString()));
                Map<String, Object> map = JsonUtil.fromJson(accessString,
                                                            Map.class);
                HugeAccess access = HugeAccess.fromMap(map);
                result.add(access);
            }
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    public List<HugeAccess> listAllAccess(String graphSpace, long limit)
                                          throws IOException,
                                          ClassNotFoundException {
        List<HugeAccess> result = new ArrayList<>();
        Map<String, String> accessMap =
                    this.metaDriver.scanWithPrefix(accessListKey(graphSpace));
        for (Map.Entry<String, String> item : accessMap.entrySet()) {
            if (limit >=0 && result.size() >= limit) {
                break;
            }
            Map<String, Object> map = JsonUtil.fromJson(item.getValue(),
                                                        Map.class);
            HugeAccess access = HugeAccess.fromMap(map);
            result.add(access);
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    public List<HugeAccess> listAccessByGroup(String graphSpace,
                                              Id group, long limit)
                                              throws IOException,
                                              ClassNotFoundException {
        List<HugeAccess> result = new ArrayList<>();
        Map<String, String> accessMap = this.metaDriver.scanWithPrefix(
                    accessListKeyByGroup(graphSpace, group.asString()));
        for (Map.Entry<String, String> item : accessMap.entrySet()) {
            if (limit >=0 && result.size() >= limit) {
                break;
            }
            Map<String, Object> map = JsonUtil.fromJson(item.getValue(),
                                                        Map.class);
            HugeAccess access = HugeAccess.fromMap(map);
            result.add(access);
        }

        return result;
    }

    public String targetFromAccess(String accessKey) {
        E.checkArgument(StringUtils.isNotEmpty(accessKey),
                        "The access name '%s' is empty", accessKey);
        E.checkArgument(accessKey.contains("->"),
                        "The access name '%s' is invalid", accessKey);
        String[] items = accessKey.split("->");
        E.checkArgument(items.length == 3,
                        "The access name '%s' is invalid", accessKey);
        return items[2];
    }

    public void clearGraphAuth(String graphSpace) {
        E.checkArgument(StringUtils.isNotEmpty(graphSpace),
                        "The graphSpace is empty");
        String prefix = this.authPrefix(graphSpace);
        this.metaDriver.deleteWithPrefix(prefix);
    }

    @SuppressWarnings("unchecked")
    public List<HugeAccess> listAccessByTarget(String graphSpace,
                                               Id target, long limit)
                                               throws IOException,
                                               ClassNotFoundException {
        List<HugeAccess> result = new ArrayList<>();
        Map<String, String> accessMap = this.metaDriver.scanWithPrefix(
                                        accessListKey(graphSpace));
        for (Map.Entry<String, String> item : accessMap.entrySet()) {
            if (limit >=0 && result.size() >= limit) {
                break;
            }
            String targetName = targetFromAccess(item.getKey());
            if (targetName.equals(target.asString())) {
                Map<String, Object> map = JsonUtil.fromJson(item.getValue(),
                                                            Map.class);
                HugeAccess access = HugeAccess.fromMap(map);
                result.add(access);
            }
        }

        return result;
    }

    public List<String> listGraphSpace() {
        List<String> result = new ArrayList<>();
        Map<String, String> graphSpaceMap = this.metaDriver.scanWithPrefix(
                                            graphSpaceListKey());
        for (Map.Entry<String, String> item : graphSpaceMap.entrySet()) {
            result.add(item.getValue());
        }

        return result;
    }

    public void initDefaultGraphSpace() {
        String defaultGraphSpace = "DEFAULT";
        this.appendGraphSpaceList(defaultGraphSpace);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> configMap(String config) {
        return JsonUtil.fromJson(config, Map.class);
    }

    public void registerStorageServerUrls(GraphSpace graphSpace,
                                          Set<String> serverUrls) {
        this.registerServerUrls(graphSpace, BindingType.STORAGE, serverUrls);
    }

    public void registerOlapServerUrls(GraphSpace graphSpace,
                                       Set<String> serverUrls) {
        this.registerServerUrls(graphSpace, BindingType.OLAP, serverUrls);
    }

    public void registerOltpServerUrls(GraphSpace graphSpace,
                                       Set<String> serverUrls) {
        this.registerServerUrls(graphSpace, BindingType.OLTP, serverUrls);
    }

    @SuppressWarnings("unchecked")
    private void registerServerUrls(GraphSpace graphSpace, BindingType type,
                                    Set<String> serverUrls) {
        String key = this.graphSpaceBindingsServer(graphSpace.name(), type);
        Set<String> exists = JsonUtil.fromJson(this.metaDriver.get(key),
                                                Set.class);
        exists.addAll(serverUrls);
        this.metaDriver.put(key, JsonUtil.toJson(serverUrls));
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> restProperties(String graphSpace,
                                              String serviceId) {
        Map<String, Object> map = null;
        String result = this.metaDriver.get(restPropertiesKey(graphSpace,
                                                              serviceId));
        if (StringUtils.isNotEmpty(result)) {
            map = JsonUtil.fromJson(result, Map.class);
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> restProperties(String graphSpace,
                                              String serviceId,
                                              Map<String, Object> properties) {
        Map<String, Object> map;
        String result = this.metaDriver.get(restPropertiesKey(graphSpace,
                                                              serviceId));
        if (StringUtils.isNotEmpty(result)) {
            map = JsonUtil.fromJson(result, Map.class);
            for (Map.Entry<String, Object> item : properties.entrySet()) {
                map.put(item.getKey(), item.getValue());
            }
        } else {
            map = properties;
        }
        this.metaDriver.put(restPropertiesKey(graphSpace, serviceId),
                            JsonUtil.toJson(map));
        return map;
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> deleteRestProperties(String graphSpace,
                                                    String serviceId,
                                                    String key) {
        Map<String, Object> map = null;
        String result = this.metaDriver.get(restPropertiesKey(graphSpace,
                                                              serviceId));
        if (StringUtils.isNotEmpty(result)) {
            map = JsonUtil.fromJson(result, Map.class);
            if (map.containsKey(key)) {
                map.remove(key);
            }
            this.metaDriver.put(restPropertiesKey(graphSpace, serviceId),
                                JsonUtil.toJson(map));
        }
        return map;
    }

    private <V> HugeTask<V> parseTask(String jsonStr, String graphSpace, String graphName) {
        if (Strings.isBlank(jsonStr)) {
            return null;
        }
        try {
            HugeTask<V> task = TaskSerializer.fromJson(jsonStr);
            task.progress(this.getTaskProgress(graphSpace, graphName, task));
            task.retries(this.getTaskRetry(graphSpace, graphName, task));
            task.overwriteContext(this.getTaskContext(graphSpace, graphName, task));
            return task;
        } catch (Throwable e) {
            // get task error
            return null;
        }
    }

    public <V> List<HugeTask<V>> listTasks(String graphSpace, String graphName) {
        List<HugeTask<V>> tasks = new ArrayList<>();
        for (TaskPriority priority : TaskPriority.values()) {
            List<HugeTask<V>> subTasks = this.listTasks(graphSpace, graphName, priority);
            tasks.addAll(subTasks);
        }
        return tasks;
    }

    public <V> List<HugeTask<V>> listTasks(String graphSpace, String graphName, TaskPriority priority) {
        String key = taskListKey(graphSpace, graphName, priority.toString());

        Map<String, String> taskMap = 
            this.metaDriver.scanWithPrefix(key);

        List<HugeTask<V>> taskList = taskMap.values().stream()
        .map((jsonStr) -> {
            HugeTask<V> task = this.parseTask(jsonStr, graphSpace, graphName);
            return task;
        })
        .filter(v -> v != null).collect(Collectors.toList());

        return taskList;
    }

    public <V> List<HugeTask<V>> listTasks(String graphSpace, String graphName, List<Id> idList) {
        List<HugeTask<V>> taskList = new ArrayList<>();
        for(Id id : idList) {
            HugeTask<V> task = this.getTask(graphSpace, graphName, id);
            if (null != task) {
                taskList.add(task);
            }
        }

        return taskList;
    }

    public <V> HugeTask<V> getTask(String graphSpace, String graphName, Id id) {
        String taskKey = this.taskPropertyKey(graphSpace, graphName, id.asString(), TASK_STATUS_POSTFIX);
        TaskStatus status = this.getTaskStatus(taskKey);
        if (status == TaskStatus.UNKNOWN) {
            return null;
        }
        String statusListKey = this.taskStatusListKey(graphSpace, graphName, id.asString(), status);
        String jsonStr = this.metaDriver.get(statusListKey);
        return parseTask(jsonStr, graphSpace, graphName);
    }
    
    /**
     * 
     * @param <V>
     * @param graphSpace
     * @param priority
     * @param id
     * @return
     */
    public <V> HugeTask<V> getTask(String graphSpace, String graphName, TaskPriority priority, Id id) {
        String key = taskPriorityKey(graphSpace, graphName, priority.toString(), id.asString());
        String jsonStr = this.metaDriver.get(key);
        return parseTask(jsonStr, graphSpace, graphName);
    }

    /**
     * Persist meta info of task to metaDriver
     * @param <V>
     * @param graphSpace
     * @param task
     * @return
     */
    public <V> Id createTask(String graphSpace, String graphName, HugeTask<V> task) {
        Id id = IdGenerator.of(task.id().asString());
        String key = taskPriorityKey(graphSpace, graphName, task.priority().toString(), id.asString());
        this.metaDriver.put(key, serialize(task));

        return id;
    }

    /**
     * Clear the task, for automatic scenarios, this method should only be called
     * when task is finished with a result of success
     * @param <V>
     * @param graphSpace
     * @param task
     */
    public <V> void clearTask(String graphSpace, String graphName, HugeTask<V> task) {
        Id id = IdGenerator.of(task.id().asString());
        String key = taskPriorityKey(graphSpace, graphName, task.priority().toString(), id.asString());
        this.metaDriver.delete(key);
    }

    /**
     * Try to lock a task for further update, this should be called by consumer only
     * @param <V>
     * @param graphSpace
     * @param task
     * @return
     */
    public <V> LockResult lockTask(String graphSpace, String graphName, HugeTask<V> task) {
        // task has been locked by current node
        if (task.lockResult() != null) {
            return task.lockResult();
        }
        String key = taskLockKey(graphSpace, graphName, task.id().asString());
        String lease = this.metaDriver.get(key);
        if (Strings.isBlank(lease)) {
            return this.lock(key);
        }
        return new LockResult();
    }

    public <V> void unlockTask(String graphSpace, String graphName, HugeTask<V> task) {
        // task is not locked by current node
        if (null == task.lockResult()) {
            return;
        }
        String key = taskLockKey(graphSpace, graphName, task.id().asString());
        this.unlock(key, task.lockResult());
    }
    /**
     * Get task progress
     * @param <V>
     * @param taskKey
     * @return
     */
    public <V> int getTaskProgress(String taskKey) {
        String value = this.metaDriver.get(taskKey);
        if (null == value) {
            return 0;
        }
        return Integer.valueOf(value);
    }

    /**
     * Get task retry
     * @param <V>
     * @param taskKey
     * @return
     */
    public <V> int getTaskRetry(String taskKey) {
        String value = this.metaDriver.get(taskKey);
        if (null == value) {
            return 0;
        }
        return Integer.valueOf(value);
    }

    /**
     * Get task context
     * @param <V>
     * @param taskKey
     * @return
     */
    public <V> String getTaskContext(String taskKey) {
        String value = this.metaDriver.get(taskKey);
        return value;
    }

    /**
     * Get task progress
     * @param <V>
     * @param graphSpace
     * @param graphName
     * @param task
     * @return
     */
    public <V> int getTaskProgress(String graphSpace, String graphName, HugeTask<V> task) {
        String key = this.taskPropertyKey(graphSpace, graphName, task.id().asString(), TASK_PROGRESS_POSTFIX);
        return this.getTaskProgress(key);
    }

    /**
     * Get task retry
     * @param <V>
     * @param graphSpace
     * @param graphName
     * @param task
     * @return
     */
    public <V> int getTaskRetry(String graphSpace, String graphName, HugeTask<V> task) {
        String key = this.taskPropertyKey(graphSpace, graphName, task.id().asString(), TASK_RETRY_POSTFIX);
        return this.getTaskRetry(key);
    }

    /**
     * Get task context
     * @param <V>
     * @param graphSpace
     * @param graphName
     * @param task
     * @return
     */
    public <V> String getTaskContext(String graphSpace, String graphName, HugeTask<V> task) {
        String key = this.taskPropertyKey(graphSpace, graphName, task.id().asString(), TASK_CONTEXT_POSTFIX);
        return this.getTaskContext(key);
    }

    /**
     * Get task status, be aware of the name of enum
     * @param <V>
     * @param taskKey
     * @return
     */
    public <V> TaskStatus getTaskStatus(String taskKey) {
        String value = this.metaDriver.get(taskKey);
        if (null == value) {
            return TaskStatus.UNKNOWN;
        }
        return TaskStatus.fromName(value);
    }

    /**
     * Get task status
     * @param <V>
     * @param graphSpace
     * @param task
     * @return
     */
    public <V> TaskStatus getTaskStatus(String graphSpace, String graphName, HugeTask<V> task) {
        String key = this.taskPropertyKey(graphSpace, graphName, task.id().asString(), TASK_STATUS_POSTFIX);
        return this.getTaskStatus(key);
    }

    /**
     * Get specified task status
     * @param graphSpace
     * @param taskId
     * @return
     */
    public TaskStatus getTaskStatus(String graphSpace, String graphName, Id taskId) {
        String key = this.taskPropertyKey(graphSpace, graphName, taskId.asString(), TASK_STATUS_POSTFIX);
        return this.getTaskStatus(key);
    }

    /**
     * Update task progress, only if having lease
     * @param <V>
     * @param graphSpace
     * @param task
     */
    public <V> void updateTaskProgress(String graphSpace, String graphName, HugeTask<V> task) {
        synchronized(task) {
            String key = taskPropertyKey(graphSpace, graphName, task.id().asString(), TASK_PROGRESS_POSTFIX);
            this.metaDriver.put(key, String.valueOf(task.progress()));
        }
    }

    public <V> void updateTaskRetry(String graphSpace, String graphName, HugeTask<V> task) {
        synchronized(task) {
            String key = taskPropertyKey(graphSpace, graphName, task.id().asString(), TASK_RETRY_POSTFIX);
            this.metaDriver.put(key ,String.valueOf(task.retries()));
        }
    }

    public <V> void updateTaskContext(String graphSpace, String graphName, HugeTask<V> task) {
        synchronized(task) {
            String key = taskPropertyKey(graphSpace, graphName, task.id().asString(), TASK_CONTEXT_POSTFIX);
            this.metaDriver.put(key, task.context());
        }
    }

    /**
     * Update task status, only if having lease, use string()!!!
     * Be aware, the name() and string() of TaskStatus refers to different value!
     * When used as key, use name
     * but for value cases, use string
     * @param <V>
     * @param graphSpace
     * @param task
     */
    private <V> void updateTaskStatus(String graphSpace, String graphName, HugeTask<V> task) {
        String key = taskPropertyKey(graphSpace, graphName, task.id().asString(), TASK_STATUS_POSTFIX);
        this.metaDriver.put(key, task.status().string());
    }

    private <V> void removeTaskFromStatusList(String graphSpace, String graphName, String taskId, TaskStatus status) {
        String key = taskStatusListKey(graphSpace, graphName, taskId, status);
        this.metaDriver.delete(key);
    }

    private <V> void addTaskToStatusList(String graphSpace, String graphName, String taskId, String jsonTask, TaskStatus status) {
        String key = taskStatusListKey(graphSpace, graphName, taskId, status);
        this.metaDriver.put(key, jsonTask);
    }

    public int countTaskByStatus(String graphSpace, String graphName, TaskStatus status) {
        String key = taskStatusListKey(graphSpace, graphName, status);
        Map<String, String> taskMap = 
            this.metaDriver.scanWithPrefix(key);
        return taskMap.size();
    }

    public <V> List<HugeTask<V>> listTasksByStatus(String graphSpace, String graphName, TaskStatus status) {
        String key = taskStatusListKey(graphSpace, graphName, status);
        Map<String, String> taskMap = 
            this.metaDriver.scanWithPrefix(key);

        List<String> taskJsonList = taskMap.values().stream().collect(Collectors.toList());
        if (taskJsonList.size() == 0) {
            return ImmutableList.of();
        }
        List<HugeTask<V>> taskList = new ArrayList<>();
        for (String taskJson : taskJsonList) {
            HugeTask<V> task = this.parseTask(taskJson, graphSpace, graphName);
            if (null != task) {
                taskList.add(task);
            }
        }
        return taskList;

    }

    /**
     * Used for task collection examine, when task status changed 
     * @param <V>
     * @param graphSpace
     * @param taskId
     * @param prevStatus
     * @param currentStatus
     */
    public <V> void migrateTaskStatus(String graphSpace, String graphName, HugeTask<V> task, TaskStatus prevStatus) {
        synchronized(task) {
            String taskId = task.id().asString();
            if (prevStatus != TaskStatus.UNKNOWN && prevStatus != TaskStatus.NEW) {
                this.removeTaskFromStatusList(graphSpace, graphName, taskId, prevStatus);
            }
            if (task.status() != TaskStatus.UNKNOWN && task.status() != TaskStatus.NEW) {
                this.addTaskToStatusList(graphSpace, graphName, taskId, TaskSerializer.toJson(task), task.status());
            }
            this.updateTaskStatus(graphSpace, graphName, task);
        }
    }

    public <V> HugeTask<V> deleteTask(String graphSpace, String graphName, HugeTask<V> task) {
        /**
         * So where we should remove?
         * 1. task priority relate
         * 2. task status relate
         * 3. task lock and task properties ( by removing taskKey dir)
         */
        String taskId = task.id().asString();

        String taskPriorityKey = taskPriorityKey(graphSpace, graphName, task.priority().toString(), taskId);
        String statusListKey = taskStatusListKey(graphSpace, graphName, taskId, task.status());
        String taskKey = taskKey(graphSpace, graphName, taskId);

        this.metaDriver.delete(taskPriorityKey);
        this.metaDriver.delete(statusListKey);
        this.metaDriver.delete(taskKey);

        return null;
    }

    public void flushAllTasks(String graphSpace, String graphName) {
        String key = taskBaseKey(graphSpace, graphName);
        this.metaDriver.deleteWithPrefix(key);
    }

    public String gremlinYaml(String graphSpace, String serviceId) {
        return this.metaDriver.get(gremlinYamlKey(graphSpace, serviceId));
    }

    public String gremlinYaml(String graphSpace, String serviceId,
                              String yaml) {
        this.metaDriver.put(gremlinYamlKey(graphSpace, serviceId), yaml);
        return yaml;
    }

    public String hstorePDPeers() {
        return this.metaDriver.get(hstorePDPeersKey());
    }

    public String getDDSHost() {
        String key = this.ddsHostKey();
        String host = this.metaDriver.get(key);
        return host;
    }

    public String getHugeGraphClusterRole() {
        String key = this.hugeClusterRoleKey();
        String role = this.metaDriver.get(key);
        return role;
    }

    public String getKafkaBrokerHost() {
        String key = this.kafkaHostKey();
        return this.metaDriver.get(key);
    }

    public String getKafkaBrokerPort() {
        String key = this.kafkaPortKey();
        return this.metaDriver.get(key);
    }

    public String getKafkaSlaveServerHost() {
        String key = this.kafkaSlaveHostKey();
        return this.metaDriver.get(key);
    }

    public Integer getKafkaSlaveServerPort() {
        String key = this.kafkaSlavePortKey();
        String portStr = this.metaDriver.get(key);
        int port = Integer.parseInt(portStr);
        return port;
    }

    public enum MetaDriverType {
        ETCD,
        PD
    }

    public enum BindingType {
        OLTP,
        OLAP,
        STORAGE
    }
}
