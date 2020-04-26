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

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.auth.HugeTarget.HugeResource;
import com.baidu.hugegraph.auth.HugeUser.P;
import com.baidu.hugegraph.backend.cache.Cache;
import com.baidu.hugegraph.backend.cache.CacheManager;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.event.EventListener;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Events;
import com.baidu.hugegraph.util.StringEncoding;
import com.google.common.collect.ImmutableSet;

public class UserManager {

    private static final long CACHE_EXPIRE = Duration.ofDays(1L).toMillis();

    private final HugeGraphParams graph;
    private final EventListener eventListener;
    private final Cache<Id, HugeUser> usersCache;

    private final EntityManager<HugeUser> users;
    private final EntityManager<HugeGroup> groups;
    private final EntityManager<HugeTarget> targets;

    private final RelationshipManager<HugeBelong> belong;
    private final RelationshipManager<HugeAccess> access;

    public UserManager(HugeGraphParams graph) {
        E.checkNotNull(graph, "graph");

        this.graph = graph;
        this.eventListener = this.listenChanges();
        this.usersCache = this.cache("users");

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

    private Cache<Id, HugeUser> cache(String prefix) {
        String name = prefix + "-" + this.graph.name();
        Cache<Id, HugeUser> cache = CacheManager.instance().cache(name);
        cache.expire(CACHE_EXPIRE);
        return cache;
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
                    this.graph.closeTx();
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

    private void invalidCache() {
        this.usersCache.clear();
    }

    public Id createUser(HugeUser user) {
        this.invalidCache();
        return this.users.add(user);
    }

    public void updateUser(HugeUser user) {
        this.invalidCache();
        this.users.update(user);
    }

    public HugeUser deleteUser(Id id) {
        this.invalidCache();
        return this.users.delete(id);
    }

    public HugeUser findUser(String name) {
        Id key = IdGenerator.of(name);
        HugeUser user = (HugeUser) this.usersCache.get(key);
        if (user != null) {
            return user;
        }

        List<HugeUser> users = this.users.query(P.NAME, name, 2L);
        if (users.size() > 0) {
            assert users.size() == 1;
            user = users.get(0);
            this.usersCache.update(key, user);
        }
        return user;
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
        this.invalidCache();
        return this.groups.add(group);
    }

    public void updateGroup(HugeGroup group) {
        this.invalidCache();
        this.groups.update(group);
    }

    public HugeGroup deleteGroup(Id id) {
        this.invalidCache();
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
        this.invalidCache();
        return this.targets.add(target);
    }

    public void updateTarget(HugeTarget target) {
        this.invalidCache();
        this.targets.update(target);
    }

    public HugeTarget deleteTarget(Id id) {
        this.invalidCache();
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

    public Id createBelong(HugeBelong belong) {
        this.invalidCache();
        return this.belong.add(belong);
    }

    public Id updateBelong(HugeBelong belong) {
        this.invalidCache();
        return this.belong.update(belong);
    }

    public HugeBelong deleteBelong(Id id) {
        this.invalidCache();
        return this.belong.delete(id);
    }

    public HugeBelong getBelong(Id id) {
        return this.belong.get(id);
    }

    public List<HugeBelong> listBelong(List<Id> ids) {
        return this.belong.list(ids);
    }

    public List<HugeBelong> listAllBelong(long limit) {
        return this.belong.list(limit);
    }

    public List<HugeBelong> listBelongByUser(Id user, long limit) {
        return this.belong.list(user, Directions.OUT,
                                HugeBelong.P.BELONG, limit);
    }

    public List<HugeBelong> listBelongByGroup(Id group, long limit) {
        return this.belong.list(group, Directions.IN,
                                HugeBelong.P.BELONG, limit);
    }

    public Id createAccess(HugeAccess access) {
        this.invalidCache();
        return this.access.add(access);
    }

    public Id updateAccess(HugeAccess access) {
        this.invalidCache();
        return this.access.update(access);
    }

    public HugeAccess deleteAccess(Id id) {
        this.invalidCache();
        return this.access.delete(id);
    }

    public HugeAccess getAccess(Id id) {
        return this.access.get(id);
    }

    public List<HugeAccess> listAccess(List<Id> ids) {
        return this.access.list(ids);
    }

    public List<HugeAccess> listAllAccess(long limit) {
        return this.access.list(limit);
    }

    public List<HugeAccess> listAccessByGroup(Id group, long limit) {
        return this.access.list(group, Directions.OUT,
                                HugeAccess.P.ACCESS, limit);
    }

    public List<HugeAccess> listAccessByTarget(Id target, long limit) {
        return this.access.list(target, Directions.IN,
                                HugeAccess.P.ACCESS, limit);
    }

    public HugeUser matchUser(String name, String password) {
        E.checkArgumentNotNull(name, "User name can't be null");
        E.checkArgumentNotNull(password, "User password can't be null");
        HugeUser user = this.findUser(name);
        if (user != null &&
            StringEncoding.checkPassword(password, user.password())) {
            return user;
        }
        return null;
    }

    public Object roleAction(HugeUser user) {
        if (user.role() != null) {
            // Return cached role (40ms => 10ms)
            return user.role();
        }

        List<HugeAccess> accesses = new ArrayList<>();;
        List<HugeBelong> belongs = this.listBelongByUser(user.id(), -1);
        for (HugeBelong belong : belongs) {
            accesses.addAll(this.listAccessByGroup(belong.target(), -1));
        }

        Map<String, Map<String, List<HugeResource>>> owners = new HashMap<>();
        for (HugeAccess access : accesses) {
            String accessPerm = access.permission().string();
            HugeTarget target = this.getTarget(access.target());
            String graph = target.graph();
            Map<String, List<HugeResource>> permissions = owners.get(graph);
            if (permissions == null) {
                permissions = new HashMap<>();
                owners.put(graph, permissions);
            }
            List<HugeResource> resources = permissions.get(accessPerm);
            if (resources == null) {
                resources = new ArrayList<>();
                permissions.put(accessPerm, resources);
            }
            resources.addAll(target.resources());
        }
        user.role(owners);
        return owners;
    }

    public void initSchemaIfNeeded() {
        this.invalidCache();
        HugeUser.schema(this.graph).initSchemaIfNeeded();
        HugeGroup.schema(this.graph).initSchemaIfNeeded();
        HugeTarget.schema(this.graph).initSchemaIfNeeded();
        HugeBelong.schema(this.graph).initSchemaIfNeeded();
        HugeAccess.schema(this.graph).initSchemaIfNeeded();
    }
}
