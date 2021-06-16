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

    public Id createGroup(HugeGroup group);
    public Id updateGroup(HugeGroup group);
    public HugeGroup deleteGroup(Id id);
    public HugeGroup getGroup(Id id);
    public List<HugeGroup> listGroups(List<Id> ids);
    public List<HugeGroup> listAllGroups(long limit);

    public Id createTarget(HugeTarget target);
    public Id updateTarget(HugeTarget target);
    public HugeTarget deleteTarget(Id id);
    public HugeTarget getTarget(Id id);
    public List<HugeTarget> listTargets(List<Id> ids);
    public List<HugeTarget> listAllTargets(long limit);

    public Id createBelong(HugeBelong belong);
    public Id updateBelong(HugeBelong belong);
    public HugeBelong deleteBelong(Id id);
    public HugeBelong getBelong(Id id);
    public List<HugeBelong> listBelong(List<Id> ids);
    public List<HugeBelong> listAllBelong(long limit);
    public List<HugeBelong> listBelongByUser(Id user, long limit);
    public List<HugeBelong> listBelongByGroup(Id group, long limit);

    public Id createAccess(HugeAccess access);
    public Id updateAccess(HugeAccess access);
    public HugeAccess deleteAccess(Id id);
    public HugeAccess getAccess(Id id);
    public List<HugeAccess> listAccess(List<Id> ids);
    public List<HugeAccess> listAllAccess(long limit);
    public List<HugeAccess> listAccessByGroup(Id group, long limit);
    public List<HugeAccess> listAccessByTarget(Id target, long limit);

    public HugeUser matchUser(String name, String password);
    public RolePermission rolePermission(AuthElement element);

    public String loginUser(String username, String password)
                            throws AuthenticationException;
    public void logoutUser(String token);

    public UserWithRole validateUser(String username, String password);
    public UserWithRole validateUser(String token);

    public Id createProject(HugeProject project);
    public HugeProject deleteProject(Id id);
    public Id updateProject(HugeProject project);
    public Id updateProjectAddGraph(Id id, String graph);
    public Id updateProjectRemoveGraph(Id id, String graph);
    public HugeProject getProject(Id id);
    public List<HugeProject> listAllProject(long limit);
}
