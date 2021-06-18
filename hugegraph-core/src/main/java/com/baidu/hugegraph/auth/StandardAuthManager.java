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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.security.sasl.AuthenticationException;

import javax.ws.rs.ForbiddenException;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.auth.HugeUser.P;
import com.baidu.hugegraph.auth.SchemaDefine.AuthElement;
import com.baidu.hugegraph.backend.cache.Cache;
import com.baidu.hugegraph.backend.cache.CacheManager;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.config.AuthOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.event.EventListener;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Events;
import com.baidu.hugegraph.util.LockUtil;
import com.baidu.hugegraph.util.Log;
import com.baidu.hugegraph.util.StringEncoding;
import com.google.common.collect.ImmutableMap;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import io.jsonwebtoken.Claims;


public class StandardAuthManager implements AuthManager {

    protected static final Logger LOG = Log.logger(StandardAuthManager.class);

    private final HugeGraphParams graph;
    private final EventListener eventListener;

    // Cache <username, HugeUser>
    private final Cache<Id, HugeUser> usersCache;
    // Cache <userId, passwd>
    private final Cache<Id, String> pwdCache;
    // Cache <token, username>
    private final Cache<Id, String> tokenCache;

    private final EntityManager<HugeUser> users;
    private final EntityManager<HugeGroup> groups;
    private final EntityManager<HugeTarget> targets;
    private final EntityManager<HugeProject> project;

    private final RelationshipManager<HugeBelong> belong;
    private final RelationshipManager<HugeAccess> access;

    private final TokenGenerator tokenGenerator;
    private final long tokenExpire;

    public StandardAuthManager(HugeGraphParams graph) {
        E.checkNotNull(graph, "graph");
        HugeConfig config = graph.configuration();
        long expired = config.get(AuthOptions.AUTH_CACHE_EXPIRE);
        long capacity = config.get(AuthOptions.AUTH_CACHE_CAPACITY);
        this.tokenExpire = config.get(AuthOptions.AUTH_TOKEN_EXPIRE);

        this.graph = graph;
        this.eventListener = this.listenChanges();
        this.usersCache = this.cache("users", capacity, expired);
        this.pwdCache = this.cache("users_pwd", capacity, expired);
        this.tokenCache = this.cache("token", capacity, expired);

        this.users = new EntityManager<>(this.graph, HugeUser.P.USER,
                                         HugeUser::fromVertex);
        this.groups = new EntityManager<>(this.graph, HugeGroup.P.GROUP,
                                          HugeGroup::fromVertex);
        this.targets = new EntityManager<>(this.graph, HugeTarget.P.TARGET,
                                           HugeTarget::fromVertex);
        this.project = new EntityManager<>(this.graph, HugeProject.P.PROJECT,
                                           HugeProject::fromVertex);

        this.belong = new RelationshipManager<>(this.graph, HugeBelong.P.BELONG,
                                                HugeBelong::fromEdge);
        this.access = new RelationshipManager<>(this.graph, HugeAccess.P.ACCESS,
                                                HugeAccess::fromEdge);

        this.tokenGenerator = new TokenGenerator(config);
    }

    private <V> Cache<Id, V> cache(String prefix, long capacity,
                                   long expiredTime) {
        String name = prefix + "-" + this.graph.name();
        Cache<Id, V> cache = CacheManager.instance().cache(name, capacity);
        if (expiredTime > 0L) {
            cache.expire(Duration.ofSeconds(expiredTime).toMillis());
        } else {
            cache.expire(expiredTime);
        }
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
        this.invalidateUserCache();
        HugeUser.schema(this.graph).initSchemaIfNeeded();
        HugeGroup.schema(this.graph).initSchemaIfNeeded();
        HugeTarget.schema(this.graph).initSchemaIfNeeded();
        HugeBelong.schema(this.graph).initSchemaIfNeeded();
        HugeAccess.schema(this.graph).initSchemaIfNeeded();
        HugeProject.schema(this.graph).initSchemaIfNeeded();
    }

    private void invalidateUserCache() {
        this.usersCache.clear();
    }

    private void invalidatePasswdCache(Id id) {
        this.pwdCache.invalidate(id);
        // Clear all tokenCache because can't get userId in it
        this.tokenCache.clear();
    }

    @Override
    public Id createUser(HugeUser user) {
        this.invalidateUserCache();
        return this.users.add(user);
    }

    @Override
    public Id updateUser(HugeUser user) {
        this.invalidateUserCache();
        this.invalidatePasswdCache(user.id());
        return this.users.update(user);
    }

    @Override
    public HugeUser deleteUser(Id id) {
        this.invalidateUserCache();
        this.invalidatePasswdCache(id);
        return this.users.delete(id);
    }

    @Override
    public HugeUser findUser(String name) {
        Id username = IdGenerator.of(name);
        HugeUser user = this.usersCache.get(username);
        if (user != null) {
            return user;
        }

        List<HugeUser> users = this.users.query(P.NAME, name, 2L);
        if (users.size() > 0) {
            assert users.size() == 1;
            user = users.get(0);
            this.usersCache.update(username, user);
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
        this.invalidateUserCache();
        return this.groups.add(group);
    }

    @Override
    public Id updateGroup(HugeGroup group) {
        this.invalidateUserCache();
        return this.groups.update(group);
    }

    @Override
    public HugeGroup deleteGroup(Id id) {
        this.invalidateUserCache();
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
        this.invalidateUserCache();
        return this.targets.add(target);
    }

    @Override
    public Id updateTarget(HugeTarget target) {
        this.invalidateUserCache();
        return this.targets.update(target);
    }

    @Override
    public HugeTarget deleteTarget(Id id) {
        this.invalidateUserCache();
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
        this.invalidateUserCache();
        E.checkArgument(this.users.exists(belong.source()),
                        "Not exists user '%s'", belong.source());
        E.checkArgument(this.groups.exists(belong.target()),
                        "Not exists group '%s'", belong.target());
        return this.belong.add(belong);
    }

    @Override
    public Id updateBelong(HugeBelong belong) {
        this.invalidateUserCache();
        return this.belong.update(belong);
    }

    @Override
    public HugeBelong deleteBelong(Id id) {
        this.invalidateUserCache();
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
        this.invalidateUserCache();
        E.checkArgument(this.groups.exists(access.source()),
                        "Not exists group '%s'", access.source());
        E.checkArgument(this.targets.exists(access.target()),
                        "Not exists target '%s'", access.target());
        return this.access.add(access);
    }

    @Override
    public Id updateAccess(HugeAccess access) {
        this.invalidateUserCache();
        return this.access.update(access);
    }

    @Override
    public HugeAccess deleteAccess(Id id) {
        this.invalidateUserCache();
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
        if (user == null) {
            return null;
        }

        if (password.equals(this.pwdCache.get(user.id()))) {
            return user;
        }

        if (StringEncoding.checkPassword(password, user.password())) {
            this.pwdCache.update(user.id(), password);
            return user;
        }
        return null;
    }

    @Override
    public RolePermission rolePermission(AuthElement element) {
        if (element instanceof HugeUser) {
            return this.rolePermission((HugeUser) element);
        } else if (element instanceof HugeTarget) {
            return this.rolePermission((HugeTarget) element);
        }

        List<HugeAccess> accesses = new ArrayList<>();
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
        List<HugeAccess> accesses = new ArrayList<>();
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
    public String loginUser(String username, String password)
                            throws AuthenticationException {
        HugeUser user = this.matchUser(username, password);
        if (user == null) {
            String msg = "Incorrect username or password";
            throw new AuthenticationException(msg);
        }

        Map<String, ?> payload = ImmutableMap.of(AuthConstant.TOKEN_USER_NAME,
                                                 username,
                                                 AuthConstant.TOKEN_USER_ID,
                                                 user.id.asString());
        String token = this.tokenGenerator.create(payload, this.tokenExpire);

        this.tokenCache.update(IdGenerator.of(token), username);
        return token;
    }

    @Override
    public void logoutUser(String token) {
        this.tokenCache.invalidate(IdGenerator.of(token));
    }

    @Override
    public UserWithRole validateUser(String username, String password) {
        HugeUser user = this.matchUser(username, password);
        if (user == null) {
            return new UserWithRole(username);
        }
        return new UserWithRole(user.id, username, this.rolePermission(user));
    }

    @Override
    public UserWithRole validateUser(String token) {
        String username = this.tokenCache.get(IdGenerator.of(token));

        Claims payload = null;
        boolean needBuildCache = false;
        if (username == null) {
            payload = this.tokenGenerator.verify(token);
            username = (String) payload.get(AuthConstant.TOKEN_USER_NAME);
            needBuildCache = true;
        }

        HugeUser user = this.findUser(username);
        if (user == null) {
            return new UserWithRole(username);
        } else if (needBuildCache) {
            long expireAt = payload.getExpiration().getTime();
            long bornTime = tokenCache.expire() -
                            (expireAt - System.currentTimeMillis());
            this.tokenCache.update(IdGenerator.of(token), username,
                                   Math.negateExact(bornTime));
        }

        return new UserWithRole(user.id(), username, this.rolePermission(user));
    }

    @Override
    public Id createProject(HugeProject project) {
        E.checkArgument(!Strings.isNullOrEmpty(project.name()),
                        "Name of project not be null or empty");
        return commit(() -> {
            //Create admin group
            if (Strings.isNullOrEmpty(project.adminGroupId())) {
                HugeGroup adminGroup = new HugeGroup(project.adminGroupName());
                adminGroup.creator(project.creator());
                Id adminGroupId = this.createGroup(adminGroup);
                project.adminGroupId(adminGroupId.asString());
            }
            //Create op group
            if (Strings.isNullOrEmpty(project.opGroupId())) {
                HugeGroup opGroup = new HugeGroup(project.opGroupName());
                opGroup.creator(project.creator());
                Id opGroupId = this.createGroup(opGroup);
                project.opGroupId(opGroupId.asString());
            }

            Id projectId = this.project.add(project);
            E.checkState(projectId != null, "Create project failed");

            //Create target
            HugeResource resource = new HugeResource(ResourceType.PROJECT,
                                                     projectId.asString(),
                                                     null);
            HugeTarget target = new HugeTarget(project.updateTargetName(),
                                               this.graph.name(),
                                               "localhost:8080",
                                               ImmutableList.of(resource));
            target.creator(project.creator());
            Id targetId = this.targets.add(target);
            project.targetId(targetId.asString());

            Id adminGroupId = IdGenerator.of(project.adminGroupId());
            Id opGroupId = IdGenerator.of(project.opGroupId());
            HugeAccess adminGroupWriteAccess2Target =
                    new HugeAccess(adminGroupId,
                                   targetId,
                                   HugePermission.WRITE);
            adminGroupWriteAccess2Target.creator(project.creator());
            HugeAccess adminGroupReadAccess2Target =
                    new HugeAccess(adminGroupId,
                                   targetId,
                                   HugePermission.READ);
            adminGroupReadAccess2Target.creator(project.creator());
            HugeAccess adminGroupDeleteAccess2Target =
                    new HugeAccess(adminGroupId,
                                   targetId,
                                   HugePermission.DELETE);
            adminGroupDeleteAccess2Target.creator(project.creator());
            HugeAccess opGroupReadAccess2Target = new HugeAccess(opGroupId,
                                                                 targetId,
                                                                 HugePermission.READ);
            opGroupReadAccess2Target.creator(project.creator());
            this.access.add(adminGroupWriteAccess2Target);
            this.access.add(adminGroupReadAccess2Target);
            this.access.add(adminGroupDeleteAccess2Target);
            this.access.add(opGroupReadAccess2Target);

            //Update targetId property of project
            HugeProject newProject = this.project.get(projectId);
            E.checkState(newProject != null, "Create project failed");
            newProject.targetId(targetId.asString());
            return this.project.update(newProject);
        });
    }

    @Override
    public HugeProject deleteProject(Id id) {
        return this.commit(() -> {
            LockUtil.Locks locks = new LockUtil.Locks(this.graph.name());
            try {
                locks.lockWrites(LockUtil.VERTEX_LABEL_ADD_UPDATE, id);

                HugeProject oldProject = this.project.get(id);
                E.checkArgumentNotNull(oldProject,
                                       "Project that id is %s not exist!",
                                       id.asString());
                //Check not graph bind this project
                if (oldProject.graphs() != null &&
                    !oldProject.graphs().isEmpty()) {
                    throw new ForbiddenException(
                            String.format("Project that id " +
                                          "is %s must not " +
                                          "graph bind it", id.asString()));
                }
                HugeProject project = this.project.delete(id);
                E.checkArgumentNotNull(project, "Deleting project failed");
                E.checkArgument(!Strings.isNullOrEmpty(project.adminGroupId()),
                                "Deleting %s failed, project.adminGroupId"
                                + " can't be null or empty", id.asString());
                E.checkArgument(!Strings.isNullOrEmpty(project.opGroupId()),
                                "Deleting %s failed, project.opGroupId"
                                + " can't be null or empty", id.asString());
                E.checkArgument(!Strings.isNullOrEmpty(project.targetId()),
                                "Deleting %s failed, project.updateTargetId "
                                + "can't be null or empty",
                                id.asString());
                //Deleting admin group
                this.groups.delete(IdGenerator.of(project.adminGroupId()));
                //Deleting op group
                this.groups.delete(IdGenerator.of(project.opGroupId()));
                //Deleting project_target
                this.targets.delete(IdGenerator.of(project.targetId()));
                return project;
            } finally {
                locks.unlock();
            }
        });
    }

    @Override
    public Id updateProject(HugeProject project) {
        E.checkArgumentNotNull(project, "Project not be null");
        E.checkArgumentNotNull(project.id(),
                               "The id of the project can't be empty");
        E.checkArgument(project.desc() != null,
                        "The description of project '%s' can't be empty",
                        project.id().asString());

        LockUtil.Locks locks = new LockUtil.Locks(this.graph.name());
        try {
            locks.lockWrites(LockUtil.VERTEX_LABEL_ADD_UPDATE, project.id());

            HugeProject source = this.project.get(project.id());
            source.desc(project.desc());
            return this.project.update(source);
        } finally {
            locks.unlock();
        }
    }

    @Override
    public Id updateProjectAddGraph(Id id, String graph) {
        E.checkArgument(!Strings.isNullOrEmpty(graph),
                        "Add graph to project failed, graph "
                        + "can't be null or empty");

        LockUtil.Locks locks = new LockUtil.Locks(this.graph.name());
        try {
            locks.lockWrites(LockUtil.VERTEX_LABEL_ADD_UPDATE, id);

            HugeProject project = this.project.get(id);
            E.checkArgumentNotNull(project,
                                   "Project '%s' is not found",
                                   id.asString());
            Set<String> graphs = project.graphs();
            if (graphs == null) {
                graphs = new HashSet<>();
            } else {
                E.checkArgument(!graphs.contains(graph),
                                "Graph has included in " +
                                "project '%s'",
                                id);
            }
            graphs.add(graph);
            project.graphs(graphs);
            return this.project.update(project);
        } finally {
            locks.unlock();
        }
    }

    @Override
    public Id updateProjectRemoveGraph(Id id, String graph) {
        E.checkArgument(!Strings.isNullOrEmpty(graph),
                        "Delete graph from project failed, graph "
                        + "can't be null or empty");

        LockUtil.Locks locks = new LockUtil.Locks(this.graph.name());
        try {
            locks.lockWrites(LockUtil.VERTEX_LABEL_ADD_UPDATE, id);

            HugeProject project = this.project.get(id);
            E.checkArgumentNotNull(project,
                                   "Project '%s' is not found",
                                   id.asString());
            Set<String> graphs = project.graphs();
            if (graphs == null || !graphs.contains(graph)) {
                return id;
            }
            graphs.remove(graph);
            project.graphs(graphs);
            return this.project.update(project);
        } finally {
            locks.unlock();
        }
    }

    @Override
    public HugeProject getProject(Id id) {
        return this.project.get(id);
    }

    @Override
    public List<HugeProject> listAllProject(long limit) {
        return this.project.list(limit);
    }

    /**
     * Maybe can define an proxy class to choose forward or call local
     */
    public static boolean isLocal(AuthManager authManager) {
        return authManager instanceof StandardAuthManager;
    }

    public <R> R commit(Callable<R> callable) {
        this.groups.shouldCommitTrans(false);
        this.access.shouldCommitTrans(false);
        this.targets.shouldCommitTrans(false);
        this.project.shouldCommitTrans(false);
        this.belong.shouldCommitTrans(false);
        this.users.shouldCommitTrans(false);

        try {
            R result = callable.call();
            this.graph.systemTransaction().commit();
            return result;
        } catch (Throwable e) {
            this.groups.shouldCommitTrans(true);
            this.access.shouldCommitTrans(true);
            this.targets.shouldCommitTrans(true);
            this.project.shouldCommitTrans(true);
            this.belong.shouldCommitTrans(true);
            this.users.shouldCommitTrans(true);
            try {
                this.graph.systemTransaction().rollback();
            } catch (Throwable rollbackException) {
                LOG.error("Failed to rollback", rollbackException);
            }
            if (e instanceof HugeException) {
                throw (HugeException) e;
            } else {
                throw new HugeException("Call failed", e);
            }
        }
    }
}
