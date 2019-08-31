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

package com.baidu.hugegraph.auth;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.auth.HugeUser.P;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.event.EventListener;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Events;
import com.baidu.hugegraph.util.JsonUtil;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class UserManager {

    private final HugeGraph graph;
    private final EventListener eventListener;

    private final EntityManager<HugeUser> users;
    private final EntityManager<HugeGroup> groups;
    private final EntityManager<HugeTarget> targets;

    private final RelationshipManager<HugeBelong> belong;
    private final RelationshipManager<HugeAccess> access;

    public UserManager(HugeGraph graph) {
        E.checkNotNull(graph, "graph");

        this.graph = graph;
        this.eventListener = this.listenChanges();

        this.users = new EntityManager<>(this.graph, HugeUser.P.USER,
                                         HugeUser::fromVertex);
        this.groups = new EntityManager<>(this.graph, HugeGroup.P.GROUP,
                                          HugeGroup::fromVertex);
        this.targets = new EntityManager<>(this.graph, HugeTarget.P.TARGET,
                                           HugeTarget::fromVertex);

        this.belong = new RelationshipManager<>(this.graph, HugeBelong.P.BELONG,
                                                HugeBelong::fromEdge);
        this.access = new RelationshipManager<>(this.graph, HugeAccess.P.ACCESS,
                                                HugeAccess::fromEdge);
    }

    public HugeGraph graph() {
        return this.graph;
    }

    private EventListener listenChanges() {
        // Listen store event: "store.inited"
        Set<String> storeEvents = ImmutableSet.of(Events.STORE_INITED);
        EventListener eventListener = event -> {
            // Ensure user schema create after system info initialized
            if (storeEvents.contains(event.name())) {
                try {
                    this.initSchemaIfNeeded();
                } finally {
                    this.graph().closeTx();
                }
                return true;
            }
            return false;
        };
        this.graph.loadSystemStore().provider().listen(eventListener);
        return eventListener;
    }

    private void unlistenChanges() {
        this.graph.loadSystemStore().provider().unlisten(this.eventListener);
    }

    public boolean close() {
        this.unlistenChanges();
        return true;
    }

    public Id createUser(HugeUser user) {
        return this.users.add(user);
    }

    public void updateUser(HugeUser user) {
        this.users.update(user);
    }

    public HugeUser deleteUser(Id id) {
        return this.users.delete(id);
    }

    public HugeUser matchUser(String name, String password) {
        E.checkArgumentNotNull(name, "User name can't be null");
        E.checkArgumentNotNull(password, "User password can't be null");
        HugeUser user = null;
        List<HugeUser> users = this.users.query(P.NAME, name, 2L);
        if (users.size() > 0) {
            assert users.size() == 1;
            user = users.get(0);
        }
        if (user != null && user.password().equals(password)) {
            return user;
        }
        return null;
    }

    public HugeUser getUser(Id id) {
        return this.users.get(id);
    }

    public List<HugeUser> listUsers(List<Id> ids) {
        return this.users.list(ids);
    }

    public List<HugeUser> listAllUsers(long limit) {
        return this.users.list(limit);
    }

    public Id createGroup(HugeGroup group) {
        return this.groups.add(group);
    }

    public void updateGroup(HugeGroup group) {
        this.groups.update(group);
    }

    public HugeGroup deleteGroup(Id id) {
        return this.groups.delete(id);
    }

    public HugeGroup getGroup(Id id) {
        return this.groups.get(id);
    }

    public List<HugeGroup> listGroups(List<Id> ids) {
        return this.groups.list(ids);
    }

    public List<HugeGroup> listAllGroups(long limit) {
        return this.groups.list(limit);
    }

    public Id createTarget(HugeTarget target) {
        return this.targets.add(target);
    }

    public void updateTarget(HugeTarget target) {
        this.targets.update(target);
    }

    public HugeTarget deleteTarget(Id id) {
        return this.targets.delete(id);
    }

    public HugeTarget getTarget(Id id) {
        return this.targets.get(id);
    }

    public List<HugeTarget> listTargets(List<Id> ids) {
        return this.targets.list(ids);
    }

    public List<HugeTarget> listAllTargets(long limit) {
        return this.targets.list(limit);
    }

    public Id createBelong(Id user, Id group) {
        return this.createBelong(new HugeBelong(user, group));
    }

    public Id createBelong(HugeBelong belong) {
        return this.belong.add(belong);
    }

    public void updateBelong(HugeBelong belong) {
        this.belong.update(belong);
    }

    public HugeBelong deleteBelong(Id id) {
        return this.belong.delete(id);
    }

    public HugeBelong getBelong(Id id) {
        return this.belong.get(id);
    }

    public List<HugeBelong> listBelongs(List<Id> ids) {
        return this.belong.list(ids);
    }

    public List<HugeBelong> listAllBelongs(long limit) {
        return this.belong.list(limit);
    }

    public List<HugeBelong> listBelongsByUser(Id user, long limit) {
        return this.belong.list(user, Directions.OUT,
                                HugeBelong.P.BELONG, limit);
    }

    public List<HugeBelong> listBelongsByGroup(Id group, long limit) {
        return this.belong.list(group, Directions.IN,
                                HugeBelong.P.BELONG, limit);
    }

    public Id createAccess(Id group, Id target, HugePermission permission) {
        return this.createAccess(new HugeAccess(group, target, permission));
    }

    public Id createAccess(HugeAccess access) {
        return this.access.add(access);
    }

    public void updateAccess(HugeAccess access) {
        this.access.update(access);
    }

    public HugeAccess deleteAccess(Id id) {
        return this.access.delete(id);
    }

    public HugeAccess getAccess(Id id) {
        return this.access.get(id);
    }

    public List<HugeAccess> listAccesss(List<Id> ids) {
        return this.access.list(ids);
    }

    public List<HugeAccess> listAllAccesss(long limit) {
        return this.access.list(limit);
    }

    public List<HugeAccess> listAccesssByGroup(Id group, long limit) {
        return this.access.list(group, Directions.OUT,
                                HugeAccess.P.ACCESS, limit);
    }

    public List<HugeAccess> listAccesssByTarget(Id target, long limit) {
        return this.access.list(target, Directions.IN,
                                HugeAccess.P.ACCESS, limit);
    }

    public String roleAction(HugeUser user) {
        List<HugeAccess> accesses = new ArrayList<>();;
        List<HugeBelong> belongs = this.listBelongsByUser(user.id(), -1);
        for (HugeBelong belong : belongs) {
            accesses.addAll(this.listAccesssByGroup(belong.target(), -1));
        }

        Map<String, Set<String>> role = new HashMap<>();
        for (HugeAccess access : accesses) {
            Id graph = access.target();
            String[] parts = SplicingIdGenerator.parse(graph);
            E.checkState(parts.length == 2,
                         "Invalid primary key vertex id '%s'", graph);
            String key = parts[1];
            Set<String> value = role.get(key);
            if (value == null) {
                value = new HashSet<>();
                role.put(key, value);
            }
            value.add(access.permission().string());
        }
        return JsonUtil.toJson(ImmutableMap.of("owners", role));
    }

    public void initSchemaIfNeeded() {
        HugeGraph graph = this.graph();
        HugeUser.schema(graph).initSchemaIfNeeded();
        HugeGroup.schema(graph).initSchemaIfNeeded();
        HugeTarget.schema(graph).initSchemaIfNeeded();
        HugeBelong.schema(graph).initSchemaIfNeeded();
        HugeAccess.schema(graph).initSchemaIfNeeded();
    }
}
