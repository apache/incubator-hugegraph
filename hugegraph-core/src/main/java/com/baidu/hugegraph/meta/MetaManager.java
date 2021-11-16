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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import com.baidu.hugegraph.auth.HugeAccess;
import com.baidu.hugegraph.auth.HugeBelong;
import com.baidu.hugegraph.auth.HugeGroup;
import com.baidu.hugegraph.auth.HugeTarget;
import com.baidu.hugegraph.auth.HugeUser;
import com.baidu.hugegraph.auth.SchemaDefine;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.space.GraphSpace;
import com.baidu.hugegraph.util.JsonUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;

import com.baidu.hugegraph.type.define.CollectionType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.collection.CollectionFactory;

public class MetaManager {

    public static final String META_PATH_DELIMETER = "/";
    public static final String META_PATH_JOIN = "-";

    public static final String META_PATH_HUGEGRAPH = "HUGEGRAPH";
    public static final String META_PATH_GRAPHSPACE = "GRAPHSPACE";
    public static final String META_PATH_GRAPHSPACE_LIST = "GRAPHSPACE_LIST";
    public static final String META_PATH_GRAPH_CONF = "GRAPH_CONF";
    public static final String META_PATH_CONF = "CONF";
    public static final String META_PATH_GRAPH = "GRAPH";
    public static final String META_PATH_AUTH = "AUTH";
    public static final String META_PATH_USER = "USER";
    public static final String META_PATH_GROUP = "GROUP";
    public static final String META_PATH_TARGET = "TARGET";
    public static final String META_PATH_BELONG = "BELONG";
    public static final String META_PATH_ACCESS = "ACCESS";

    public static final String META_PATH_EVENT = "EVENT";
    public static final String META_PATH_ADD = "ADD";
    public static final String META_PATH_REMOVE = "REMOVE";
    public static final String META_PATH_CHANGE = "CHANGE";

    public static final String DEFAULT_NAMESPACE = "default_ns";

    private MetaDriver metaDriver;
    private String cluster;

    private static final MetaManager INSTANCE = new MetaManager();

    public static MetaManager instance() {
        return INSTANCE;
    }

    private MetaManager() {
    }

    public void connect(String cluster, MetaDriverType type, Object... args) {
        E.checkArgument(cluster != null && !cluster.isEmpty(),
                        "The cluster can't be null or empty");
        if (this.metaDriver == null) {
            this.cluster = cluster;

            switch (type) {
                case ETCD:
                    this.metaDriver = new EtcdMetaDriver(args);
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

    public <T> void listenGraphSpaceChange(Consumer<T> consumer) {
        this.listen(this.graphSpaceUpdateKey(), consumer);
    }

    public <T> void listenGraphAdd(Consumer<T> consumer) {
        this.listen(this.graphAddKey(), consumer);
    }

    public <T> void listenGraphRemove(Consumer<T> consumer) {
        this.listen(this.graphRemoveKey(), consumer);
    }

    private <T> void listen(String key, Consumer<T> consumer) {
        this.metaDriver.listen(key, consumer);
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

    public <T> List<String> extractGraphSpacesFromResponse(T response) {
        return this.metaDriver.extractValuesFromResponse(response);
    }

    public <T> List<String> extractGraphsFromResponse(T response) {
        return this.metaDriver.extractValuesFromResponse(response);
    }

    public GraphSpace getGraphSpaceConfig(String graphSpace) {
        String gs = this.metaDriver.get(this.graphSpaceConfKey(graphSpace));
        return JsonUtil.fromJson(gs, GraphSpace.class);
    }

    public Map<String, Object> getGraphConfig(String graphSpace, String graph) {
        return configMap(this.metaDriver.get(this.graphConfKey(graphSpace,
                                                               graph)));
    }

    public String getGraphConfig(String graph) {
        return this.metaDriver.get(this.graphConfKey(graph));
    }

    public void addGraphConfig(String graphSpace, String graph, String config) {
        this.metaDriver.put(this.graphConfKey(graphSpace, graph), config);
    }

    public void addGraphConfig(String graphSpace, String graph,
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

    public void addGraphSpace(String graphSpace) {
        this.metaDriver.put(this.graphSpaceAddKey(), graphSpace);
    }

    public void removeGraphSpace(String graphSpace) {
        this.metaDriver.put(this.graphSpaceRemoveKey(), graphSpace);
    }

    public void updateGraphSpace(String graphSpace) {
        this.metaDriver.put(this.graphSpaceUpdateKey(), graphSpace);
    }

    public void removeGraphConfig(String graphSpace, String graph) {
        this.metaDriver.delete(this.graphConfKey(graphSpace, graph));
    }

    public void addGraph(String graphSpace, String graph) {
        this.metaDriver.put(this.graphAddKey(),
                            this.nsGraphName(graphSpace, graph));
    }

    public void removeGraph(String graphSpace, String graph) {
        this.metaDriver.put(this.graphRemoveKey(),
                            this.nsGraphName(graphSpace, graph));
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
                           META_PATH_GRAPHSPACE, META_PATH_CHANGE);
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

    private String graphConfPrefix(String graphSpace) {
        return this.graphConfKey(graphSpace, Strings.EMPTY);
    }

    private String graphSpaceConfPrefix() {
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE,
                           META_PATH_CONF);
    }

    private String graphSpaceConfKey(String name) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/CONF/{graphspace}
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE,
                           META_PATH_CONF, name);
    }

    // TODO: remove after support namespace
    private String graphConfKey(String graph) {
        return this.graphConfKey(DEFAULT_NAMESPACE, graph);
    }

    private String graphConfKey(String graphSpace, String graph) {
        // HUGEGRAPH/{cluster}/GRAPHSPACE/{graphspace}/GRAPH_CONF
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE,
                           graphSpace, META_PATH_GRAPH_CONF, graph);
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

    public String accessId(String groupName, String targetName, String code) {
        E.checkArgument(StringUtils.isNotEmpty(groupName) &&
                        StringUtils.isNotEmpty(targetName),
                        "The group name '%s' or target name '%s' is empty",
                        groupName, targetName);
        return String.join("->", groupName, code, targetName);
    }

    private String graphSpaceListKey() {
        // HUGEGRAPH/{cluster}/GRAPHSPACE_LIST
        return String.join(META_PATH_DELIMETER, META_PATH_HUGEGRAPH,
                           this.cluster, META_PATH_GRAPHSPACE_LIST);
    }

    private String nsGraphName(String namespace, String name) {
        return String.join(META_PATH_JOIN, namespace, name);
    }

    private String serialize(SchemaDefine.AuthElement element) throws IOException {
        Map<String, Object> objectMap = element.asMap();
        return JsonUtil.toJson(objectMap);
    }

    private <T> T deserialize(String content)
            throws IOException, ClassNotFoundException {
        byte[] source = content.getBytes();
        ByteArrayInputStream bis = new ByteArrayInputStream(source);
        ObjectInputStream ois = new ObjectInputStream(bis);
        return (T) ois.readObject();
        //Map<String, Object> map = JsonUtil.fromJson(content, Map.class);
    }

    public void createUser(HugeUser user) throws IOException {
        String result = this.metaDriver.get(userKey(user.name()));
        E.checkArgument(StringUtils.isEmpty(result),
                        "The user name '%s' has existed", user.name());
        this.metaDriver.put(userKey(user.name()), serialize(user));

    }

    public void updateUser(HugeUser user) throws IOException {
        String result = this.metaDriver.get(userKey(user.name()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The user name '%s' does not existed", user.name());

        this.metaDriver.put(userKey(user.name()), serialize(user));
    }

    public HugeUser deleteUser(Id id) throws IOException,
                               ClassNotFoundException {
        String result = this.metaDriver.get(userKey(id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The user name '%s' does not existed", id.asString());
        this.metaDriver.delete(userKey(id.asString()));
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

    public Id updateGroup(String graphSpace, HugeGroup group)
                          throws IOException {
        String result = this.metaDriver.get(groupKey(graphSpace, group.name()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The group name '%s' is not existed", group.name());
        this.metaDriver.put(groupKey(graphSpace, group.name()),
                            serialize(group));
        return group.id();
    }

    public HugeGroup deleteGroup(String graphSpace, Id id)
                                 throws IOException,
                                 ClassNotFoundException {
        String result = this.metaDriver.get(groupKey(graphSpace,
                                                     id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The group name '%s' is not existed", id.asString());
        this.metaDriver.delete(groupKey(graphSpace, id.asString()));
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

    public Id updateTarget(String graphSpace, HugeTarget target)
                           throws IOException {
        String result = this.metaDriver.get(targetKey(graphSpace,
                                                      target.name()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The target name '%s' is not existed", target.name());
        this.metaDriver.put(targetKey(graphSpace, target.name()),
                            serialize(target));
        return target.id();
    }

    public HugeTarget deleteTarget(String graphSpace, Id id)
                                   throws IOException,
                                   ClassNotFoundException {
        String result = this.metaDriver.get(targetKey(graphSpace,
                                                      id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The target name '%s' is not existed", id.asString());
        this.metaDriver.delete(targetKey(graphSpace, id.asString()));
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
        return IdGenerator.of(belongId);
    }

    public Id updateBelong(String graphSpace, HugeBelong belong)
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
        this.metaDriver.put(belongKey(graphSpace, belongId), serialize(belong));
        return belong.id();
    }

    public HugeBelong deleteBelong(String graphSpace, Id id)
                                   throws IOException,
                                   ClassNotFoundException {
        String result = this.metaDriver.get(belongKey(graphSpace,
                                                      id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The belong name '%s' is not existed", id.asString());
        this.metaDriver.delete(belongKey(graphSpace, id.asString()));
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

        String opCode = String.valueOf(access.permission().code());
        String accessId = accessId(group.name(), target.name(), opCode);
        String result = this.metaDriver.get(accessKey(graphSpace, accessId));
        E.checkArgument(StringUtils.isEmpty(result),
                        "The access name '%s' has existed", accessId);
        this.metaDriver.put(accessKey(graphSpace, accessId), serialize(access));
        return IdGenerator.of(accessId);
    }

    public Id updateAccess(String graphSpace, HugeAccess access)
                           throws IOException, ClassNotFoundException {
        HugeGroup group = this.getGroup(graphSpace, access.source());
        E.checkArgument(group != null,
                        "The group name '%s' is not existed",
                        access.source().asString());

        HugeTarget target = this.getTarget(graphSpace, access.target());
        E.checkArgument(target != null,
                        "The target name '%s' is not existed",
                        access.target().asString());

        String opCode = String.valueOf(access.permission().code());
        String accessId = accessId(group.name(), target.name(), opCode);
        String result = this.metaDriver.get(accessKey(graphSpace, accessId));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The access name '%s' is not existed", accessId);
        Map<String, Object> map = JsonUtil.fromJson(result, Map.class);
        HugeAccess existed = HugeAccess.fromMap(map);
        E.checkArgument(existed.permission().code() ==
                        access.permission().code(),
                        "The access name '%s' has existed", accessId);
        this.metaDriver.put(accessKey(graphSpace, accessId), serialize(access));
        return access.id();
    }

    public HugeAccess deleteAccess(String graphSpace, Id id)
                                   throws IOException, ClassNotFoundException {
        String result = this.metaDriver.get(accessKey(graphSpace,
                                                      id.asString()));
        E.checkArgument(StringUtils.isNotEmpty(result),
                        "The access name '%s' is not existed", id.asString());
        this.metaDriver.delete(accessKey(graphSpace, id.asString()));
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

    public enum MetaDriverType {
        ETCD,
        PD
    }
}
