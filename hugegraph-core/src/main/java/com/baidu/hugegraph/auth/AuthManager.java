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

import java.util.List;
import javax.security.sasl.AuthenticationException;

import com.baidu.hugegraph.auth.SchemaDefine.AuthElement;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.meta.MetaManager;

public interface AuthManager {

    public boolean close();

    public Id createUser(HugeUser user, boolean required);
    public HugeUser updateUser(HugeUser user, boolean required);
    public HugeUser deleteUser(Id id, boolean required);
    public HugeUser findUser(String name, boolean required);
    public HugeUser getUser(Id id, boolean required);
    public List<HugeUser> listUsers(List<Id> ids, boolean required);
    public List<HugeUser> listAllUsers(long limit, boolean required);

    public Id createSpaceManager(String graphSpace, String user);
    public void deleteSpaceManager(String graphSpace, String user);
    public Id createAdminManager(String user);
    public void deleteAdminManager(String user);

    public Id createGroup(String graphSpace, HugeGroup group,
                          boolean required);
    public HugeGroup updateGroup(String graphSpace, HugeGroup group,
                                 boolean required);
    public HugeGroup deleteGroup(String graphSpace, Id id, boolean required);
    public HugeGroup getGroup(String graphSpace, Id id, boolean required);
    public List<HugeGroup> listGroups(String graphSpace, List<Id> ids,
                                      boolean required);
    public List<HugeGroup> listAllGroups(String graphSpace, long limit,
                                         boolean required);

    public Id createTarget(String graphSpace, HugeTarget target,
                           boolean required);
    public HugeTarget updateTarget(String graphSpace, HugeTarget target,
                                   boolean required);
    public HugeTarget deleteTarget(String graphSpace, Id id, boolean required);
    public HugeTarget getTarget(String graphSpace, Id id, boolean required);
    public List<HugeTarget> listTargets(String graphSpace, List<Id> ids,
                                        boolean required);
    public List<HugeTarget> listAllTargets(String graphSpace, long limit,
                                           boolean required);

    public Id createBelong(String graphSpace, HugeBelong belong,
                           boolean required);
    public HugeBelong updateBelong(String graphSpace, HugeBelong belong,
                                   boolean required);
    public HugeBelong deleteBelong(String graphSpace, Id id, boolean required);
    public HugeBelong getBelong(String graphSpace, Id id, boolean required);
    public List<HugeBelong> listBelong(String graphSpace, List<Id> ids,
                                       boolean required);
    public List<HugeBelong> listAllBelong(String graphSpace, long limit,
                                          boolean required);
    public List<HugeBelong> listBelongByUser(String graphSpace, Id user,
                                             long limit, boolean required);
    public List<HugeBelong> listBelongByGroup(String graphSpace, Id group,
                                              long limit, boolean required);

    public Id createAccess(String graphSpace, HugeAccess access,
                           boolean required);
    public HugeAccess updateAccess(String graphSpace, HugeAccess access,
                                   boolean required);
    public HugeAccess deleteAccess(String graphSpace, Id id, boolean required);
    public HugeAccess getAccess(String graphSpace, Id id, boolean required);
    public List<HugeAccess> listAccess(String graphSpace, List<Id> ids,
                                       boolean required);
    public List<HugeAccess> listAllAccess(String graphSpace, long limit,
                                          boolean required);
    public List<HugeAccess> listAccessByGroup(String graphSpace, Id group,
                                              long limit, boolean required);
    public List<HugeAccess> listAccessByTarget(String graphSpace, Id target,
                                               long limit, boolean required);

    public List<String> listGraphSpace();
    public HugeUser matchUser(String name, String password);
    public RolePermission rolePermission(AuthElement element);

    public String loginUser(String username, String password,
                            long expire)
                            throws AuthenticationException;
    public void logoutUser(String token);
    public String createToken(String username);

    public Id createKgUser(HugeUser user);
    public String createToken(String username, long expire);

    public void processEvent(MetaManager.AuthEvent event);

    public UserWithRole validateUser(String username, String password);
    public UserWithRole validateUser(String token);
}
