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

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.auth.SchemaDefine.AuthElement;
import com.baidu.hugegraph.backend.cache.Cache;
import com.baidu.hugegraph.backend.cache.CacheManager;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.meta.MetaManager;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.baidu.hugegraph.util.StringEncoding;
import com.google.common.collect.ImmutableMap;
import io.jsonwebtoken.Claims;
import org.slf4j.Logger;

import javax.security.sasl.AuthenticationException;
import javax.ws.rs.ForbiddenException;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class StandardAuthManager implements AuthManager {

    protected static final Logger LOG = Log.logger(StandardAuthManager.class);

    // Cache <username, HugeUser>
    private final Cache<Id, HugeUser> usersCache;
    // Cache <userId, passwd>
    private final Cache<Id, String> pwdCache;
    // Cache <token, username>
    private final Cache<Id, String> tokenCache;

    private final TokenGenerator tokenGenerator;

    private MetaManager metaManager;

    private static final long AUTH_CACHE_EXPIRE = 10 * 60L;
    private static final long AUTH_CACHE_CAPACITY = 1024 * 10L;
    private static final long AUTH_TOKEN_EXPIRE = 3600 * 24L;

    public StandardAuthManager(MetaManager metaManager, HugeConfig conf) {
        this.metaManager = metaManager;
        this.usersCache = this.cache("users", AUTH_CACHE_CAPACITY, AUTH_CACHE_EXPIRE);
        this.pwdCache = this.cache("users_pwd", AUTH_CACHE_CAPACITY, AUTH_CACHE_EXPIRE);
        this.tokenCache = this.cache("token", AUTH_CACHE_CAPACITY, AUTH_CACHE_EXPIRE);
        this.tokenGenerator = new TokenGenerator(conf);
    }

    public StandardAuthManager(MetaManager metaManager, String secretKey) {
        this.metaManager = metaManager;
        this.usersCache = this.cache("users", AUTH_CACHE_CAPACITY, AUTH_CACHE_EXPIRE);
        this.pwdCache = this.cache("users_pwd", AUTH_CACHE_CAPACITY, AUTH_CACHE_EXPIRE);
        this.tokenCache = this.cache("token", AUTH_CACHE_CAPACITY, AUTH_CACHE_EXPIRE);
        this.tokenGenerator = new TokenGenerator(secretKey);
    }

    private <V> Cache<Id, V> cache(String prefix, long capacity,
                                   long expiredTime) {
        String name = prefix + "-auth";
        Cache<Id, V> cache = CacheManager.instance().cache(name, capacity);
        if (expiredTime > 0L) {
            cache.expire(Duration.ofSeconds(expiredTime).toMillis());
        } else {
            cache.expire(expiredTime);
        }
        return cache;
    }

    @Override
    public boolean close() {
        return true;
    }

    private void invalidateUserCache() {
        this.usersCache.clear();
    }

    private void invalidatePasswordCache(Id id) {
        this.pwdCache.invalidate(id);
        // Clear all tokenCache because can't get userId in it
        this.tokenCache.clear();
    }

    private AuthElement updateCreator(AuthElement elem) {
        String username = currentUsername();
        if (username != null && elem.creator() == null) {
            elem.creator(username);
        }
        elem.update(new Date());
        return elem;
    }

    private String currentUsername() {
        HugeGraphAuthProxy.Context context = HugeGraphAuthProxy.getContext();
        if (context != null) {
            return context.user().username();
        }
        return null;
    }

    private <V> V verifyResPermission(HugePermission actionPerm,
                                      boolean throwIfNoPerm,
                                      Supplier<ResourceObject<V>> fetcher,
                                      Supplier<Boolean> checker) {
        // TODO: call verifyPermission() before actual action
        HugeGraphAuthProxy.Context context = HugeGraphAuthProxy.getContext();
        E.checkState(context != null,
                     "Missing authentication context " +
                     "when verifying resource permission");
        String username = context.user().username();
        Object role = context.user().role();
        ResourceObject<V> ro = fetcher.get();
        String action = actionPerm.string();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Verify permission {} {} for user '{}' with role {}",
                      action, ro, username, role);
        }

        V result = ro.operated();
        // Verify role permission
        if (!HugeAuthenticator.RolePerm.match(role, actionPerm, ro)) {
            result = null;
        }
        // Verify permission for one access another, like: granted <= user role
        else if (ro.type().isGrantOrUser()) {
            AuthElement element = (AuthElement) ro.operated();
            RolePermission grant = rolePermission(element);
            if (!HugeAuthenticator.RolePerm.match(role, grant, ro)) {
                result = null;
            }
        }

        // Check resource detail if needed
        if (result != null && checker != null && !checker.get()) {
            result = null;
        }

        if (!(actionPerm == HugePermission.READ && ro.type().isSchema())) {
            String status = result == null ? "denied" : "allowed";
            LOG.info("User '{}' is {} to {} {}", username, status, action, ro);
        }

        // result = null means no permission, throw if needed
        if (result == null && throwIfNoPerm) {
            String error = String.format("Permission denied: %s %s",
                    action, ro);
            throw new ForbiddenException(error);
        }
        return result;
    }

    private <V> V verifyResPermission(HugePermission actionPerm,
                                      boolean throwIfNoPerm,
                                      Supplier<ResourceObject<V>> fetcher) {
        return verifyResPermission(actionPerm, throwIfNoPerm, fetcher, null);
    }

    private <V extends AuthElement> V verifyUserPermission(
            HugePermission actionPerm,
            boolean throwIfNoPerm,
            Supplier<V> elementFetcher) {
        return verifyResPermission(actionPerm, throwIfNoPerm, () -> {
            V elem = elementFetcher.get();
            @SuppressWarnings("unchecked")
            ResourceObject<V> r = (ResourceObject<V>)
                              ResourceObject.of("SYSTEM", "SYSTEM", elem);
            return r;
        });
    }

    private <V extends AuthElement> V verifyUserPermission(
            HugePermission actionPerm,
            V elementFetcher) {
        return verifyUserPermission(actionPerm, true, () -> elementFetcher);
    }

    private <V extends AuthElement> List<V> verifyUserPermission(
                                            HugePermission actionPerm,
                                            List<V> elems) {
        List<V> results = new ArrayList<>();
        for (V elem : elems) {
            V r = verifyUserPermission(actionPerm, false, () -> elem);
            if (r != null) {
                results.add(r);
            }
        }
        return results;
    }

    @Override
    public Id createUser(HugeUser user, boolean required) {
        if (required) {
            verifyUserPermission(HugePermission.WRITE, user);
        }
        Id username = IdGenerator.of(user.name());
        HugeUser existed = this.usersCache.get(username);
        E.checkArgument(existed == null,
                        "The user name '%s' has existed", user.name());

        try {
            this.updateCreator(user);
            this.metaManager.createUser(user);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "serialize user", e);
        }

        return username;
    }

    @Override
    public Id updateUser(HugeUser user, boolean required) {
        Id username = IdGenerator.of(user.name());
        try {
            HugeUser existed = this.findUser(user.name(), false);
            if (required && !existed.name().equals(currentUsername())) {
                verifyUserPermission(HugePermission.WRITE, user);
            }

            this.updateCreator(user);
            this.metaManager.updateUser(user);
            this.invalidateUserCache();
            this.invalidatePasswordCache(user.id());
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "serialize user", e);
        }

        return username;
    }

    protected void deleteBelongsByUser(Id id) {
        List<String> spaces = this.listGraphSpace();
        for (String space : spaces) {
            List<HugeBelong> belongs = this.listBelongByUser(space, id,
                                                             -1, false);
            for (HugeBelong belong : belongs) {
                this.deleteBelong(space, belong.id(), false);
            }
        }
    }

    @Override
    public HugeUser deleteUser(Id id, boolean required) {
        if (id.asString().equals("admin")) {
            throw new HugeException("admin could not be removed");
        }
        HugeUser user = this.usersCache.get(id);
        if (user != null) {
            this.invalidateUserCache();
            this.invalidatePasswordCache(id);
        }

        try {
            user = this.findUser(id.asString(), false);
            E.checkArgument(user != null,
                            "The user name '%s' is not existed",
                            id.asString());
            E.checkArgument(!HugeAuthenticator.USER_ADMIN.equals(user.name()),
                            "Can't delete user '%s'", user.name());
            if (required) {
                verifyUserPermission(HugePermission.DELETE, user);
            }

            this.deleteBelongsByUser(id);
            return this.metaManager.deleteUser(id);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize user", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize user", e);
        }
    }

    /**
     * findUser: not verifyUserPermission
     */
    @Override
    public HugeUser findUser(String name, boolean required) {
        Id username = IdGenerator.of(name);
        HugeUser user = this.usersCache.get(username);
        if (user == null) {
            try {
                user = this.metaManager.findUser(name);
                if (user != null) {
                    this.usersCache.update(username, user);
                }
            } catch (IOException e) {
                throw new HugeException("IOException occurs when " +
                                        "deserialize user", e);
            } catch (ClassNotFoundException e) {
                throw new HugeException("ClassNotFoundException occurs when " +
                                        "deserialize user", e);
            }
        }

        return user;
    }

    @Override
    public HugeUser getUser(Id id, boolean required) {
        if (required) {
            return verifyUserPermission(HugePermission.READ,
                                        this.findUser(id.asString(), false));
        }
        return this.findUser(id.asString(), false);
    }

    @Override
    public List<HugeUser> listUsers(List<Id> ids, boolean required) {
        try {
            if (required) {
                return verifyUserPermission(HugePermission.READ,
                                            this.metaManager.listUsers(ids));
            }
            return this.metaManager.listUsers(ids);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize user", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize user", e);
        }
    }

    @Override
    public List<HugeUser> listAllUsers(long limit, boolean required) {
        try {
            if (required) {
                return verifyUserPermission(HugePermission.READ,
                       this.metaManager.listAllUsers(limit));
            }
            return this.metaManager.listAllUsers(limit);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize user", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize user", e);
        }
    }

    @Override
    public Id createGroup(String graphSpace, HugeGroup group,
                          boolean required) {
        try {
            if (required) {
                verifyUserPermission(HugePermission.WRITE, group);
            }
            this.updateCreator(group);
            Id result = this.metaManager.createGroup(graphSpace, group);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "serialize group", e);
        }
    }

    @Override
    public Id updateGroup(String graphSpace, HugeGroup group,
                          boolean required) {
        this.invalidateUserCache();
        try {
            if (required) {
                verifyUserPermission(HugePermission.WRITE, group);
            }
            this.updateCreator(group);
            return this.metaManager.updateGroup(graphSpace, group);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "serialize group", e);
        }
    }

    @Override
    public HugeGroup deleteGroup(String graphSpace, Id id, boolean required) {
        try {
            if (required) {
                verifyUserPermission(HugePermission.DELETE,
                                     this.metaManager.getGroup(graphSpace,
                                                               id));
            }

            List<HugeBelong> belongs = this.listBelongByGroup(graphSpace, id,
                                                              -1, false);
            for (HugeBelong belong : belongs) {
                this.deleteBelong(graphSpace, belong.id(), false);
            }

            List<HugeAccess> accesses = this.listAccessByGroup(graphSpace, id,
                                                               -1, false);
            for (HugeAccess access : accesses) {
                this.deleteAccess(graphSpace, access.id(), false);
            }

            HugeGroup result = this.metaManager.deleteGroup(graphSpace, id);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize group", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize group", e);
        }
    }

    @Override
    public HugeGroup getGroup(String graphSpace, Id id, boolean required) {
        try {
            if (required) {
                return verifyUserPermission(HugePermission.READ,
                       this.metaManager.getGroup(graphSpace, id));
            }
            return this.metaManager.getGroup(graphSpace, id);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize group", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize group", e);
        }
    }

    @Override
    public List<HugeGroup> listGroups(String graphSpace, List<Id> ids,
                                      boolean required) {
        try {
            if (required) {
                return verifyUserPermission(HugePermission.READ,
                       this.metaManager.listGroups(graphSpace, ids));
            }
            return this.metaManager.listGroups(graphSpace, ids);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize group", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize group", e);
        }
    }

    @Override
    public List<HugeGroup> listAllGroups(String graphSpace, long limit,
                                         boolean required) {
        try {
            if (required) {
                return verifyUserPermission(HugePermission.READ,
                       this.metaManager.listAllGroups(graphSpace, limit));
            }
            return this.metaManager.listAllGroups(graphSpace, limit);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize group", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize group", e);
        }
    }

    @Override
    public Id createTarget(String graphSpace, HugeTarget target,
                           boolean required) {
        try {
            if (required) {
                verifyUserPermission(HugePermission.WRITE, target);
            }
            this.updateCreator(target);
            Id result = this.metaManager.createTarget(graphSpace, target);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "serialize target", e);
        }
    }

    @Override
    public Id updateTarget(String graphSpace, HugeTarget target,
                           boolean required) {
        try {
            this.updateCreator(target);
            if (required) {
                verifyUserPermission(HugePermission.WRITE, target);
            }
            Id result = this.metaManager.updateTarget(graphSpace, target);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "serialize target", e);
        }
    }

    @Override
    public HugeTarget deleteTarget(String graphSpace, Id id,
                                   boolean required) {
        try {
            if (required) {
                verifyUserPermission(HugePermission.DELETE,
                                     this.metaManager.getTarget(graphSpace,
                                                                id));
            }
            List<HugeAccess> accesses = this.listAccessByTarget(graphSpace, id,
                                                                -1, false);
            for (HugeAccess access : accesses) {
                this.deleteAccess(graphSpace, access.id(), false);
            }
            HugeTarget target = this.metaManager.deleteTarget(graphSpace, id);
            this.invalidateUserCache();
            return target;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize target", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize target", e);
        }
    }

    @Override
    public HugeTarget getTarget(String graphSpace, Id id, boolean required) {
        try {
            if (required) {
                return verifyUserPermission(HugePermission.READ,
                       this.metaManager.getTarget(graphSpace, id));
            }
            return this.metaManager.getTarget(graphSpace, id);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize target", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize target", e);
        }
    }

    @Override
    public List<HugeTarget> listTargets(String graphSpace, List<Id> ids,
                                        boolean required) {
        try {
            if (required) {
                return verifyUserPermission(HugePermission.READ,
                       this.metaManager.listTargets(graphSpace, ids));
            }
            return this.metaManager.listTargets(graphSpace, ids);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize target", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize target", e);
        }
    }

    @Override
    public List<HugeTarget> listAllTargets(String graphSpace, long limit,
                                           boolean required) {
        try {
            if (required) {
                return verifyUserPermission(HugePermission.READ,
                       this.metaManager.listAllTargets(graphSpace, limit));
            }
            return this.metaManager.listAllTargets(graphSpace, limit);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "deserialize target", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "deserialize target", e);
        }
    }

    @Override
    public Id createBelong(String graphSpace, HugeBelong belong,
                           boolean required) {
        try {
            if (required) {
                verifyUserPermission(HugePermission.WRITE, belong);
            }
            this.updateCreator(belong);
            this.invalidateUserCache();
            return this.metaManager.createBelong(graphSpace, belong);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "create belong", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "create belong", e);
        }
    }

    @Override
    public Id updateBelong(String graphSpace, HugeBelong belong,
                           boolean required) {
        try {
            if (required) {
                verifyUserPermission(HugePermission.WRITE, belong);
            }
            this.updateCreator(belong);
            Id result = this.metaManager.updateBelong(graphSpace, belong);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "update belong", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "update belong", e);
        }
    }

    @Override
    public HugeBelong deleteBelong(String graphSpace, Id id,
                                   boolean required) {
        try {
            if (required) {
                verifyUserPermission(HugePermission.DELETE,
                                     this.metaManager.getBelong(graphSpace,
                                                                id));
            }
            HugeBelong result = this.metaManager.deleteBelong(graphSpace, id);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "delete belong", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "delete belong", e);
        }
    }

    @Override
    public HugeBelong getBelong(String graphSpace, Id id, boolean required) {
        try {
            if (required) {
                return verifyUserPermission(HugePermission.READ,
                       this.metaManager.getBelong(graphSpace, id));
            }
            return this.metaManager.getBelong(graphSpace, id);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get belong", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get belong", e);
        }
    }

    @Override
    public List<HugeBelong> listBelong(String graphSpace, List<Id> ids,
                                       boolean required) {
        try {
            if (required) {
                return verifyUserPermission(HugePermission.READ,
                       this.metaManager.listBelong(graphSpace, ids));
            }
            return this.metaManager.listBelong(graphSpace, ids);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get belong list by ids", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get belong list by ids", e);
        }
    }

    @Override
    public List<HugeBelong> listAllBelong(String graphSpace, long limit,
                                          boolean required) {
        try {
            if (required) {
                return verifyUserPermission(HugePermission.READ,
                       this.metaManager.listAllBelong(graphSpace, limit));
            }
            return this.metaManager.listAllBelong(graphSpace, limit);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get all belong list", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get all belong list", e);
        }
    }

    @Override
    public List<HugeBelong> listBelongByUser(String graphSpace, Id user,
                                             long limit, boolean required) {
        try {
            if (required) {
                return verifyUserPermission(HugePermission.READ,
                       this.metaManager.listBelongByUser(graphSpace, user,
                                                         limit));
            }
            return this.metaManager.listBelongByUser(graphSpace, user, limit);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get belong list by user", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get belong list by user", e);
        }
    }

    @Override
    public List<HugeBelong> listBelongByGroup(String graphSpace, Id group,
                                              long limit, boolean required) {
        try {
            if (required) {
                return verifyUserPermission(HugePermission.READ,
                       this.metaManager.listBelongByGroup(graphSpace, group,
                                                          limit));
            }
            return this.metaManager.listBelongByGroup(graphSpace, group, limit);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get belong list by group", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get belong list by group", e);
        }
    }

    @Override
    public Id createAccess(String graphSpace, HugeAccess access,
                           boolean required) {
        try {
            if (required) {
                verifyUserPermission(HugePermission.WRITE, access);
            }
            this.updateCreator(access);
            Id result = this.metaManager.createAccess(graphSpace, access);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "create access", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "create access", e);
        }
    }

    @Override
    public Id updateAccess(String graphSpace, HugeAccess access,
                           boolean required) {
        try {
            if (required) {
                verifyUserPermission(HugePermission.WRITE, access);
            }
            this.updateCreator(access);
            Id result = this.metaManager.updateAccess(graphSpace, access);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "update access", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "update access", e);
        }
    }

    @Override
    public HugeAccess deleteAccess(String graphSpace, Id id,
                                   boolean required) {

        try {
            if (required) {
                verifyUserPermission(HugePermission.DELETE,
                                     this.metaManager.getAccess(graphSpace,
                                                                id));
            }
            HugeAccess result = this.metaManager.deleteAccess(graphSpace, id);
            this.invalidateUserCache();
            return result;
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "delete access", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "delete access", e);
        }
    }

    @Override
    public HugeAccess getAccess(String graphSpace, Id id, boolean required) {
        try {
            if (required) {
                return verifyUserPermission(HugePermission.READ,
                       this.metaManager.getAccess(graphSpace, id));
            }
            return this.metaManager.getAccess(graphSpace, id);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get access", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get access", e);
        }
    }

    @Override
    public List<HugeAccess> listAccess(String graphSpace, List<Id> ids,
                                       boolean required) {
        try {
            if (required) {
                return verifyUserPermission(HugePermission.READ,
                       this.metaManager.listAccess(graphSpace, ids));
            }
            return this.metaManager.listAccess(graphSpace, ids);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get access list", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get access list", e);
        }
    }

    @Override
    public List<HugeAccess> listAllAccess(String graphSpace, long limit,
                                          boolean required) {
        try {
            if (required) {
                return verifyUserPermission(HugePermission.READ,
                       this.metaManager.listAllAccess(graphSpace, limit));
            }
            return this.metaManager.listAllAccess(graphSpace, limit);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get all access list", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get all access list", e);
        }
    }

    @Override
    public List<HugeAccess> listAccessByGroup(String graphSpace, Id group,
                                              long limit, boolean required) {
        try {
            if (required) {
                return verifyUserPermission(HugePermission.READ,
                       this.metaManager.listAccessByGroup(graphSpace, group,
                                                          limit));
            }
            return this.metaManager.listAccessByGroup(graphSpace, group,
                                                      limit);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get access list by group", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get access list by group", e);
        }
    }

    @Override
    public List<HugeAccess> listAccessByTarget(String graphSpace, Id target,
                                               long limit, boolean required) {
        try {
            if (required) {
                return verifyUserPermission(HugePermission.READ,
                       this.metaManager.listAccessByTarget(graphSpace, target,
                                                           limit));
            }
            return this.metaManager.listAccessByTarget(graphSpace,
                                                       target, limit);
        } catch (IOException e) {
            throw new HugeException("IOException occurs when " +
                                    "get access list by target", e);
        } catch (ClassNotFoundException e) {
            throw new HugeException("ClassNotFoundException occurs when " +
                                    "get access list by target", e);
        }
    }

    @Override
    public List<String> listGraphSpace() {
        return metaManager.listGraphSpace();
    }

    @Override
    public HugeUser matchUser(String name, String password) {
        E.checkArgumentNotNull(name, "User name can't be null");
        E.checkArgumentNotNull(password, "User password can't be null");

        HugeUser user = this.findUser(name, false);
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
        /*
        if (!(element instanceof HugeUser) ||
                !((HugeUser) element).name().equals(username)) {
            verifyUserPermission(HugePermission.READ, element);
        } */
        return this.rolePermissionInner(element);
    }

    public RolePermission rolePermissionInner(AuthElement element) {
        if (element instanceof HugeUser) {
            return this.rolePermission((HugeUser) element);
        } else if (element instanceof HugeTarget) {
            return this.rolePermission((HugeTarget) element);
        }

        List<HugeAccess> accesses = new ArrayList<>();
        if (element instanceof HugeBelong) {
            HugeBelong belong = (HugeBelong) element;
            accesses.addAll(this.listAccessByGroup(belong.graphSpace(),
                            belong.target(), -1, false));
        } else if (element instanceof HugeGroup) {
            HugeGroup group = (HugeGroup) element;
            accesses.addAll(this.listAccessByGroup(group.graphSpace(),
                            group.id(), -1, false));
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
        RolePermission role = new RolePermission();
        List<String> graphSpaces = this.listGraphSpace();
        for (String graphSpace : graphSpaces) {
            List<HugeBelong> belongs = this.listBelongByUser(graphSpace,
                                       user.id(), -1, false);
            for (HugeBelong belong : belongs) {
                List<HugeAccess> accesses = this.listAccessByGroup(graphSpace,
                                            belong.target(), -1, false);
                for (HugeAccess access : accesses) {
                    HugePermission accessPerm = access.permission();
                    HugeTarget target = this.getTarget(graphSpace,
                                        access.target(), false);
                    role.add(graphSpace, target.graph(),
                             accessPerm, target.resources());
                }
            }
        }

        user.role(role);
        return role;
    }

    private RolePermission rolePermission(List<HugeAccess> accesses) {
        // Mapping of: graph -> action -> resource
        RolePermission role = new RolePermission();
        for (HugeAccess access : accesses) {
            HugePermission accessPerm = access.permission();
            HugeTarget target = this.getTarget(access.graphSpace(),
                                               access.target(), false);
            role.add(target.graphSpace(), target.graph(),
                     accessPerm, target.resources());
        }
        return role;
    }

    private RolePermission rolePermission(HugeTarget target) {
        RolePermission role = new RolePermission();
        // TODO: improve for the actual meaning
        role.add(target.graphSpace(), target.graph(), HugePermission.READ, target.resources());
        return role;
    }

    @Override
    public String loginUser(String username, String password,
                            long expire)
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
        expire = expire == 0L ? AUTH_TOKEN_EXPIRE : expire;
        String token = this.tokenGenerator.create(payload, expire * 1000);
        this.tokenCache.update(IdGenerator.of(token), username);
        return token;
    }

    @Override
    public void logoutUser(String token) {
        this.tokenCache.invalidate(IdGenerator.of(token));
    }

    @Override
    public String createToken(String username) {
        HugeUser user = this.findUser(username, false);
        if (user == null) {
            return null;
        }

        Map<String, ?> payload = ImmutableMap.of(AuthConstant.TOKEN_USER_NAME,
                                                 username,
                                                 AuthConstant.TOKEN_USER_ID,
                                                 user.id.asString());
        String token = this.tokenGenerator.create(payload, AUTH_TOKEN_EXPIRE);
        this.tokenCache.update(IdGenerator.of(token), username);
        return token;
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

        Claims payload = this.tokenGenerator.verify(token);
        boolean needBuildCache = false;
        if (username == null) {
            username = (String) payload.get(AuthConstant.TOKEN_USER_NAME);
            needBuildCache = true;
        }

        HugeUser user = this.findUser(username, false);
        if (user == null) {
            return new UserWithRole(username);
        } else if (needBuildCache) {
            long expireAt = payload.getExpiration().getTime();
            long bornTime = this.tokenCache.expire() -
                            (expireAt - System.currentTimeMillis());
            this.tokenCache.update(IdGenerator.of(token), username,
                                   Math.negateExact(bornTime));
        }

        return new UserWithRole(user.id(), username, this.rolePermission(user));
    }

    public void initAdmin() {
        HugeUser user = new HugeUser("admin");
        user.password(StringEncoding.hashPassword("admin"));
        user.creator("system");
        user.phone("18888886666");
        user.email("admin@hugegraph.com");
        user.description("None");
        user.update(new Date());
        user.create(new Date());
        user.avatar("/image.png");
        try {
            this.metaManager.createUser(user);
            this.metaManager.initDefaultGraphSpace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Maybe can define an proxy class to choose forward or call local
     */
    public static boolean isLocal(AuthManager authManager) {
        return authManager instanceof StandardAuthManager;
    }
}
