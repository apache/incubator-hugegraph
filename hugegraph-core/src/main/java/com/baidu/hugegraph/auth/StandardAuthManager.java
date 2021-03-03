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
import java.util.List;
import java.util.Set;

import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.auth.HugeUser.P;
import com.baidu.hugegraph.auth.SchemaDefine.UserElement;
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

public class StandardAuthManager implements AuthManager {

    private static final long CACHE_EXPIRE = Duration.ofDays(1L).toMillis();

    private final HugeGraphParams graph;
    private final EventListener eventListener;
    private final Cache<Id, HugeUser> usersCache;

    private final EntityManager<HugeUser> users;
    private final EntityManager<HugeGroup> groups;
    private final EntityManager<HugeTarget> targets;

    private final RelationshipManager<HugeBelong> belong;
    private final RelationshipManager<HugeAccess> access;

    public StandardAuthManager(HugeGraphParams graph) {
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

    @Override
    public boolean close() {
        this.unlistenChanges();
        return true;
    }

    private void initSchemaIfNeeded() {
        this.invalidCache();
        HugeUser.schema(this.graph).initSchemaIfNeeded();
        HugeGroup.schema(this.graph).initSchemaIfNeeded();
        HugeTarget.schema(this.graph).initSchemaIfNeeded();
        HugeBelong.schema(this.graph).initSchemaIfNeeded();
        HugeAccess.schema(this.graph).initSchemaIfNeeded();
    }

    private void invalidCache() {
        this.usersCache.clear();
    }

    @Override
    public Id createUser(HugeUser user) {
        this.invalidCache();
        return this.users.add(user);
    }

    @Override
    public Id updateUser(HugeUser user) {
        this.invalidCache();
        return this.users.update(user);
    }

    @Override
    public HugeUser deleteUser(Id id) {
        this.invalidCache();
        return this.users.delete(id);
    }

    @Override
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

    @Override
    public HugeUser getUser(Id id) {
        return this.users.get(id);
    }

    @Override
    public List<HugeUser> listUsers(List<Id> ids) {
        return this.users.list(ids);
    }

    @Override
    public List<HugeUser> listAllUsers(long limit) {
        return this.users.list(limit);
    }

    @Override
    public Id createGroup(HugeGroup group) {
        this.invalidCache();
        return this.groups.add(group);
    }

    @Override
    public Id updateGroup(HugeGroup group) {
        this.invalidCache();
        return this.groups.update(group);
    }

    @Override
    public HugeGroup deleteGroup(Id id) {
        this.invalidCache();
        return this.groups.delete(id);
    }

    @Override
    public HugeGroup getGroup(Id id) {
        return this.groups.get(id);
    }

    @Override
    public List<HugeGroup> listGroups(List<Id> ids) {
        return this.groups.list(ids);
    }

    @Override
    public List<HugeGroup> listAllGroups(long limit) {
        return this.groups.list(limit);
    }

    @Override
    public Id createTarget(HugeTarget target) {
        this.invalidCache();
        return this.targets.add(target);
    }

    @Override
    public Id updateTarget(HugeTarget target) {
        this.invalidCache();
        return this.targets.update(target);
    }

    @Override
    public HugeTarget deleteTarget(Id id) {
        this.invalidCache();
        return this.targets.delete(id);
    }

    @Override
    public HugeTarget getTarget(Id id) {
        return this.targets.get(id);
    }

    @Override
    public List<HugeTarget> listTargets(List<Id> ids) {
        return this.targets.list(ids);
    }

    @Override
    public List<HugeTarget> listAllTargets(long limit) {
        return this.targets.list(limit);
    }

    @Override
    public Id createBelong(HugeBelong belong) {
        this.invalidCache();
        E.checkArgument(this.users.exists(belong.source()),
                        "Not exists user '%s'", belong.source());
        E.checkArgument(this.groups.exists(belong.target()),
                        "Not exists group '%s'", belong.target());
        return this.belong.add(belong);
    }

    @Override
    public Id updateBelong(HugeBelong belong) {
        this.invalidCache();
        return this.belong.update(belong);
    }

    @Override
    public HugeBelong deleteBelong(Id id) {
        this.invalidCache();
        return this.belong.delete(id);
    }

    @Override
    public HugeBelong getBelong(Id id) {
        return this.belong.get(id);
    }

    @Override
    public List<HugeBelong> listBelong(List<Id> ids) {
        return this.belong.list(ids);
    }

    @Override
    public List<HugeBelong> listAllBelong(long limit) {
        return this.belong.list(limit);
    }

    @Override
    public List<HugeBelong> listBelongByUser(Id user, long limit) {
        return this.belong.list(user, Directions.OUT,
                                HugeBelong.P.BELONG, limit);
    }

    @Override
    public List<HugeBelong> listBelongByGroup(Id group, long limit) {
        return this.belong.list(group, Directions.IN,
                                HugeBelong.P.BELONG, limit);
    }

    @Override
    public Id createAccess(HugeAccess access) {
        this.invalidCache();
        E.checkArgument(this.groups.exists(access.source()),
                        "Not exists group '%s'", access.source());
        E.checkArgument(this.targets.exists(access.target()),
                        "Not exists target '%s'", access.target());
        return this.access.add(access);
    }

    @Override
    public Id updateAccess(HugeAccess access) {
        this.invalidCache();
        return this.access.update(access);
    }

    @Override
    public HugeAccess deleteAccess(Id id) {
        this.invalidCache();
        return this.access.delete(id);
    }

    @Override
    public HugeAccess getAccess(Id id) {
        return this.access.get(id);
    }

    @Override
    public List<HugeAccess> listAccess(List<Id> ids) {
        return this.access.list(ids);
    }

    @Override
    public List<HugeAccess> listAllAccess(long limit) {
        return this.access.list(limit);
    }

    @Override
    public List<HugeAccess> listAccessByGroup(Id group, long limit) {
        return this.access.list(group, Directions.OUT,
                                HugeAccess.P.ACCESS, limit);
    }

    @Override
    public List<HugeAccess> listAccessByTarget(Id target, long limit) {
        return this.access.list(target, Directions.IN,
                                HugeAccess.P.ACCESS, limit);
    }

    @Override
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

    @Override
    public RolePermission rolePermission(UserElement element) {
        if (element instanceof HugeUser) {
            return this.rolePermission((HugeUser) element);
        } else if (element instanceof HugeTarget) {
            return this.rolePermission((HugeTarget) element);
        }

        List<HugeAccess> accesses = new ArrayList<>();;
        if (element instanceof HugeBelong) {
            HugeBelong belong = (HugeBelong) element;
            accesses.addAll(this.listAccessByGroup(belong.target(), -1));
        } else if (element instanceof HugeGroup) {
            HugeGroup group = (HugeGroup) element;
            accesses.addAll(this.listAccessByGroup(group.id(), -1));
        } else if (element instanceof HugeAccess) {
            HugeAccess access = (HugeAccess) element;
            accesses.add(access);
        } else {
            E.checkArgument(false, "Invalid type for role permission: %s",
                            element);
        }

        return this.rolePermission(accesses);
    }

    private RolePermission rolePermission(HugeUser user) {
        if (user.role() != null) {
            // Return cached role (40ms => 10ms)
            return user.role();
        }

        // Collect accesses by user
        List<HugeAccess> accesses = new ArrayList<>();;
        List<HugeBelong> belongs = this.listBelongByUser(user.id(), -1);
        for (HugeBelong belong : belongs) {
            accesses.addAll(this.listAccessByGroup(belong.target(), -1));
        }

        // Collect permissions by accesses
        RolePermission role = this.rolePermission(accesses);

        user.role(role);
        return role;
    }

    private RolePermission rolePermission(List<HugeAccess> accesses) {
        // Mapping of: graph -> action -> resource
        RolePermission role = new RolePermission();
        for (HugeAccess access : accesses) {
            HugePermission accessPerm = access.permission();
            HugeTarget target = this.getTarget(access.target());
            role.add(target.graph(), accessPerm, target.resources());
        }
        return role;
    }

    private RolePermission rolePermission(HugeTarget target) {
        RolePermission role = new RolePermission();
        // TODO: improve for the actual meaning
        role.add(target.graph(), HugePermission.READ, target.resources());
        return role;
    }

    @Override
    public RolePermission loginUser(String username, String password) {
        HugeUser user = this.matchUser(username, password);
        if (user == null) {
            return null;
        }
        return this.rolePermission(user);
    }
}
