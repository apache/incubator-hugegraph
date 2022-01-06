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

import com.alipay.sofa.jraft.entity.Task;
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
import com.baidu.hugegraph.util.JsonUtil;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;

import com.baidu.hugegraph.type.define.CollectionType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.collection.CollectionFactory;

import io.fabric8.kubernetes.api.model.Namespace;

public class MetaManager {

    public static final String META_PATH_DELIMETER = "/";
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

    public static final String META_PATH_AUTH_EVENT = "AUTH_EVENT";
    public static final String META_PATH_EVENT = "EVENT";
    public static final String META_PATH_ADD = "ADD";
    public static final String META_PATH_REMOVE = "REMOVE";
    public static final String META_PATH_UPDATE = "UPDATE";

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
        this.listen(this.graphSpaceAddKey(), consumer);
    }

    public <T> void listenServiceRemove(Consumer<T> consumer) {
        this.listen(this.graphSpaceRemoveKey(), consumer);
    }

    public <T> void listenServiceUpdate(Consumer<T> consumer) {
        this.listen(this.graphSpaceUpdateKey(), consumer);
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

    public <T> void listenTaskAdded(String graphSpace, TaskPriority priority,  Consumer<T> consumer) {
        this.listenPrefix(this.taskEventKey(graphSpace, priority.toString()), consumer);
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
            String[] parts = key.split(META_PATH_DELIMETER);
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
            String[] parts = key.split(META_PATH_DELIMETER);
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
            String[] parts = key.split(META_PATH_DELIMETER);
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
            String[] parts = key.split(META_PATH_DELIMETER);
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
        this.metaDriver.put(this.schemaTemplateKey(graphSpace, template.name()),
                            JsonUtil.toJson(template.asMap()));
    }

    public void removeSchemaTemplate(String graphSpace, String name) {
        this.metaDriver.delete(this.schemaTemplateKey(graphSpace, name));
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

    public GraphSpace getGraphSpaceConfig(String graphSpace) {
        String gs = this.metaDriver.get(this.graphSpaceConfKey(graphSpace));
        if (gs == null) {
            return null;
        }
        return JsonUtil.fromJson(gs, GraphSpace.class);
    }

    public Service getServiceConfig(String graphSpace, String service) {
        String s = this.metaDriver.get(this.serviceConfKey(graphSpace, service));
        return JsonUtil.fromJson(s, Service.class);
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
        String key = String.join(META_PATH_DELIMETER, keys);
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

    public void unlock(LockResult lockResult, String... keys) {
        String key = String.join(META_PATH_DELIMETER, keys);
        this.unlock(key, lockResult);
    }

    public void unlock(String key, LockResult lockResult) {
        this.metaDriver.unlock(key, lockResult);
    }

    private String graphSpaceAddKey() {
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_EVENT,
                           META_PATH_GRAPHSPACE, META_PATH_ADD);
    }

    private String graphSpaceRemoveKey() {
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_EVENT,
                           META_PATH_GRAPHSPACE, META_PATH_REMOVE);
    }

    private String graphSpaceUpdateKey() {
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_EVENT,
                           META_PATH_GRAPHSPACE, META_PATH_UPDATE);
    }

    private String serviceAddKey() {
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_EVENT,
                           META_PATH_SERVICE, META_PATH_ADD);
    }

    private String serviceRemoveKey() {
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_EVENT,
                           META_PATH_SERVICE, META_PATH_REMOVE);
    }

    private String serviceUpdateKey() {
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_EVENT,
                           META_PATH_SERVICE, META_PATH_UPDATE);
    }

    private String graphAddKey() {
        // HUGEGRAPH/{cluster}/EVENT/GRAPH/ADD
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_EVENT,
                           META_PATH_GRAPH, META_PATH_ADD);
    }

    private String graphRemoveKey() {
        // HUGEGRAPH/{cluster}/EVENT/GRAPH/REMOVE
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_EVENT,
                           META_PATH_GRAPH, META_PATH_REMOVE);
    }

    private String graphUpdateKey() {
        // HUGEGRAPH/{cluster}/EVENT/GRAPH/UPDATE
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_EVENT,
                           META_PATH_GRAPH, META_PATH_UPDATE);
    }

    private String graphSpaceConfKey(String name) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/CONF/{graphspace}
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE,
                           META_PATH_CONF, name);
    }

    private String graphSpaceConfPrefix() {
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
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
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, name,
                           META_PATH_K8S_BINDINGS, type.name());
    }

    private String graphSpaceBindingsServer(String name, BindingType type) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/CONF/{graphspace}
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, name,
                           META_PATH_K8S_BINDINGS, type.name(), META_PATH_URLS);
    }

    private String serviceConfKey(String graphSpace, String name) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/SERVICE_CONF
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE,
                           graphSpace, META_PATH_SERVICE_CONF, name);
    }

    private String graphConfKey(String graphSpace, String graph) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/GRAPH_CONF
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE,
                           graphSpace, META_PATH_GRAPH_CONF, graph);
    }

    private String schemaTemplateKey(String graphSpace, String schemaTemplate) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/SCHEMA_TEMPLATE
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE,
                           graphSpace, META_PATH_SCHEMA_TEMPLATE,
                           schemaTemplate);
    }

    private String restPropertiesKey(String graphSpace, String serviceId) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/SERVICE/
        // {serviceId}/REST_PROPERTIES
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE,
                           graphSpace, META_PATH_SERVICE, serviceId,
                           META_PATH_REST_PROPERTIES);
    }

    private String gremlinYamlKey(String graphSpace, String serviceId) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/SERVICE/
        // {serviceId}/GREMLIN_YAML
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE,
                           graphSpace, META_PATH_SERVICE, serviceId,
                           META_PATH_GREMLIN_YAML);
    }

    private String userKey(String name) {
        // HUGEGRAPH/{cluster}/AUTH/USER/{user}
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_AUTH, META_PATH_USER, name);
    }

    private String userListKey() {
        // HUGEGRAPH/{cluster}/AUTH/USER
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_AUTH, META_PATH_USER);
    }

    private String groupKey(String graphSpace, String group) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/GROUP/{group}
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                           META_PATH_AUTH, META_PATH_GROUP, group);
    }

    private String groupListKey(String graphSpace) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/GROUP
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                           META_PATH_AUTH, META_PATH_GROUP);
    }

    private String targetKey(String graphSpace, String target) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/TARGET/{target}
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                           META_PATH_AUTH, META_PATH_TARGET, target);
    }

    private String targetListKey(String graphSpace) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/TARGET
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                           META_PATH_AUTH, META_PATH_TARGET);
    }

    private String belongKey(String graphSpace, String belong) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/BELONG/{belong}
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                           META_PATH_AUTH, META_PATH_BELONG, belong);
    }

    private String belongListKey(String graphSpace) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/BELONG
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                           META_PATH_AUTH, META_PATH_BELONG);
    }

    private String belongListKeyByUser(String graphSpace, String userName) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/BELONG/{userName}
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                           META_PATH_AUTH, META_PATH_BELONG, userName + "->");
    }

    private String accessKey(String graphSpace, String access) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/ACCESS/{group->op->target}
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                           META_PATH_AUTH, META_PATH_ACCESS, access);
    }

    private String accessListKey(String graphSpace) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/ACCESS
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE, graphSpace,
                           META_PATH_AUTH, META_PATH_ACCESS);
    }

    private String taskKey(String graphSpace, String taskPriority, String taskId) {
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH, this.cluster, META_PATH_GRAPHSPACE,
        graphSpace, META_PATH_TASK, taskPriority, taskId);
    }

    private String taskListKey(String graphSpace, String taskPriority) {
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH, this.cluster, META_PATH_GRAPHSPACE,
        graphSpace, META_PATH_TASK, taskPriority);
    }

    private String taskEventKey(String graphSpace, String taskPriority) {
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH, this.cluster, META_PATH_GRAPHSPACE,
        graphSpace, META_PATH_TASK, taskPriority);
    }

    private String accessListKeyByGroup(String graphSpace, String groupName) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphSpace}/AUTH/ACCESS/{groupName}
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
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
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_AUTH_EVENT);
    }

    private String graphSpaceListKey() {
        // HUGEGRAPH/{cluster}/GRAPHSPACE_LIST
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE_LIST);
    }

    private String hstorePDPeersKey() {
        // HUGEGRAPH/{cluster}/META_PATH_PD_PEERS
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
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

    public Id createGroup(String graphSpace, HugeGroup group)
                          throws IOException {
        String result = this.metaDriver.get(groupKey(graphSpace, group.name()));
        E.checkArgument(StringUtils.isEmpty(result),
                        "The group name '%s' has existed", group.name());
        this.metaDriver.put(groupKey(graphSpace, group.name()),
                            serialize(group));
        return IdGenerator.of(group.name());
    }

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

    public HugeGroup findGroup(String graphSpace, Id id) {
        String result = this.metaDriver.get(groupKey(graphSpace,
                                                     id.asString()));
        if (StringUtils.isEmpty(result)) {
            return null;
        }
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        return HugeGroup.fromMap(map);
    }

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

    public HugeTarget findTarget(String graphSpace, Id id) {
        String result = this.metaDriver.get(targetKey(graphSpace,
                                                      id.asString()));
        if (StringUtils.isEmpty(result)) {
            return null;
        }
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        return HugeTarget.fromMap(map);
    }

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

    public List<HugeBelong> listBelongByUser(String graphSpace,
                                             Id user, long limit)
                                             throws IOException,
                                             ClassNotFoundException {
        List<HugeBelong> result = new ArrayList<>();
        Map<String, String> belongMap = this.metaDriver.scanWithPrefix(
                            belongListKeyByUser(graphSpace, user.asString()));
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

    public HugeAccess findAccess(String graphSpace, Id id) {
        String result = this.metaDriver.get(accessKey(graphSpace,
                                                      id.asString()));
        if (StringUtils.isEmpty(result)) {
            return null;
        }
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        return HugeAccess.fromMap(map);
    }

    public HugeAccess getAccess(String graphSpace, Id id)
                                throws IOException, ClassNotFoundException {
        String result = this.metaDriver.get(accessKey(graphSpace,
                                                      id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The access name '%s' is not existed", id.asString());
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        return HugeAccess.fromMap(map);
    }

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
        String defaultGS = String.join(META_PATH_DELIMETER,
                                       META_PATH_HUGEGRAPH,
                                       this.cluster,
                                       META_PATH_GRAPHSPACE_LIST,
                                       "DEFAULT");
        this.metaDriver.put(defaultGS, "DEFAULT");
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

    public List<String> listTasks(String graphSpace, TaskPriority priority) {
        String key = taskListKey(graphSpace, priority.toString());

        Map<String, String> taskMap = 
            this.metaDriver.scanWithPrefix(key);

        return taskMap.values().stream().collect(Collectors.toList());
    }
    
    public <V> HugeTask<V> getTask(String graphSpace, TaskPriority priority, Id id) {
        String key = taskKey(graphSpace, priority.toString(), id.asString());
        String jsonStr = this.metaDriver.get(key);
        HugeTask<V> task = TaskSerializer.fromJson(jsonStr);
        return task;
    }

    public <V> Id createTask(String graphSpace, HugeTask<V> task) {
        Id id = IdGenerator.of(task.id().asString());
        String key = taskKey(graphSpace, task.priority().toString(), id.asString());
        this.metaDriver.put(key, serialize(task));

        return id;
    }

    public <V> HugeTask<V> updateTask() {

        return null;
    }

    public <V> HugeTask<V> deleteTask() {
        return null;
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
