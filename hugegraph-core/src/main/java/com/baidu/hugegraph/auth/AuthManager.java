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

public interface AuthManager {

    public boolean close();

    public Id createUser(HugeUser user);
    public Id updateUser(HugeUser user);
    public HugeUser deleteUser(Id id);
    public HugeUser findUser(String name);
    public HugeUser getUser(Id id);
    public List<HugeUser> listUsers(List<Id> ids);
    public List<HugeUser> listAllUsers(long limit);

    public Id createGroup(String graphSpace, HugeGroup group);
    public Id updateGroup(String graphSpace, HugeGroup group);
    public HugeGroup deleteGroup(String graphSpace, Id id);
    public HugeGroup getGroup(String graphSpace, Id id);
    public List<HugeGroup> listGroups(String graphSpace, List<Id> ids);
    public List<HugeGroup> listAllGroups(String graphSpace, long limit);

    public Id createTarget(String graphSpace, HugeTarget target);
    public Id updateTarget(String graphSpace, HugeTarget target);
    public HugeTarget deleteTarget(String graphSpace, Id id);
    public HugeTarget getTarget(String graphSpace, Id id);
    public List<HugeTarget> listTargets(String graphSpace, List<Id> ids);
    public List<HugeTarget> listAllTargets(String graphSpace, long limit);

    public Id createBelong(String graphSpace, HugeBelong belong);
    public Id updateBelong(String graphSpace, HugeBelong belong);
    public HugeBelong deleteBelong(String graphSpace, Id id);
    public HugeBelong getBelong(String graphSpace, Id id);
    public List<HugeBelong> listBelong(String graphSpace, List<Id> ids);
    public List<HugeBelong> listAllBelong(String graphSpace, long limit);
    public List<HugeBelong> listBelongByUser(String graphSpace,
                                             Id user, long limit);
    public List<HugeBelong> listBelongByGroup(String graphSpace,
                                              Id group, long limit);

    public Id createAccess(String graphSpace, HugeAccess access);
    public Id updateAccess(String graphSpace, HugeAccess access);
    public HugeAccess deleteAccess(String graphSpace, Id id);
    public HugeAccess getAccess(String graphSpace, Id id);
    public List<HugeAccess> listAccess(String graphSpace, List<Id> ids);
    public List<HugeAccess> listAllAccess(String graphSpace, long limit);
    public List<HugeAccess> listAccessByGroup(String graphSpace,
                                              Id group, long limit);
    public List<HugeAccess> listAccessByTarget(String graphSpace,
                                               Id target, long limit);

    public List<String> listGraphSpace();
    public HugeUser matchUser(String name, String password);
    public RolePermission rolePermission(AuthElement element);

    public String loginUser(String username, String password,
                            long expire)
                            throws AuthenticationException;
    public void logoutUser(String token);
    public String createToken(String username);

    public UserWithRole validateUser(String username, String password);
    public UserWithRole validateUser(String token);
}
