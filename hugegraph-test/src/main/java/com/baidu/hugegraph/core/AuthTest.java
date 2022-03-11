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

package com.baidu.hugegraph.core;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.sasl.AuthenticationException;

import com.baidu.hugegraph.auth.SchemaDefine;
import com.baidu.hugegraph.util.SafeDateUtil;
import org.junit.After;
import org.junit.Test;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.auth.AuthManager;
import com.baidu.hugegraph.auth.HugeAccess;
import com.baidu.hugegraph.auth.HugeBelong;
import com.baidu.hugegraph.auth.HugeGroup;
import com.baidu.hugegraph.auth.HugePermission;
import com.baidu.hugegraph.auth.HugeResource;
import com.baidu.hugegraph.auth.HugeTarget;
import com.baidu.hugegraph.auth.HugeUser;
import com.baidu.hugegraph.auth.RolePermission;
import com.baidu.hugegraph.auth.UserWithRole;
import com.baidu.hugegraph.backend.cache.Cache;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.StringEncoding;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class AuthTest extends BaseCoreTest {

    @After
    public void clearAll() {
        AuthManager authManager = authManager();

        for (HugeUser user : authManager.listAllUsers(-1, false)) {
            Assert.assertNotNull(user);
            if (!user.name().equals("admin")) {
                Assert.assertNotNull(user.id());
                authManager.deleteUser(user.id(), false);
            }
        }
        for (HugeGroup group : authManager.listAllGroups(DEFAULT_GRAPH_SPACE,
                                                         -1, false)) {
            Assert.assertNotNull(group);
            Assert.assertNotNull(group.id());
            authManager.deleteGroup(DEFAULT_GRAPH_SPACE, group.id(), false);
        }
        for (HugeTarget target : authManager.listAllTargets(
                                 DEFAULT_GRAPH_SPACE, -1, false)) {
            Assert.assertNotNull(target);
            Assert.assertNotNull(target.id());
            authManager.deleteTarget(DEFAULT_GRAPH_SPACE, target.id(), false);
        }

        Assert.assertEquals(0, authManager.listAllAccess(DEFAULT_GRAPH_SPACE,
                            -1, false).size());
        Assert.assertEquals(0, authManager.listAllBelong(DEFAULT_GRAPH_SPACE,
                            -1, false).size());
    }

    @Test
    public void testCreateUser() {
        AuthManager authManager = authManager();

        Id id = authManager.createUser(makeUser("tom001", "pass1"), false);

        HugeUser user = authManager.getUser(id, false);
        Assert.assertEquals("tom001", user.name());
        Assert.assertEquals("pass1", user.password());
        Assert.assertEquals(user.create(), user.update());
        Assert.assertNull(user.phone());
        Assert.assertNull(user.email());
        Assert.assertNull(user.avatar());

        Map<String, Object> expected = new HashMap<>();
        expected.putAll(ImmutableMap.of("user_name", "tom001",
                                        "user_password", "pass1",
                                        "user_creator", "admin"));
        expected.putAll(ImmutableMap.of("user_create",
                                        SafeDateUtil.format(user.create(), SchemaDefine.FORMATTER),
                                        "user_update",
                                        SafeDateUtil.format(user.update(), SchemaDefine.FORMATTER),
                                        "id", user.id()));

        Assert.assertEquals(expected, user.asMap());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            authManager.createUser(makeUser("tom001", "pass1"), false);
        }, e -> {
            Assert.assertContains("The user name", e.getMessage());
            Assert.assertContains("has existed", e.getMessage());
        });
    }

    @Test
    public void testCreateUserWithDetailsInfo() {
        AuthManager authManager = authManager();

        HugeUser user = new HugeUser("james");
        user.password("pass2");
        user.phone("13812345678");
        user.email("test@hugegraph.com");
        user.avatar("http://image.hugegraph.com/image1");
        user.creator("admin");

        Id id = authManager.createUser(user, false);

        user = authManager.getUser(id, false);
        Assert.assertEquals("james", user.name());
        Assert.assertEquals("pass2", user.password());
        Assert.assertEquals(user.create(), user.update());
        Assert.assertEquals("13812345678", user.phone());
        Assert.assertEquals("test@hugegraph.com", user.email());
        Assert.assertEquals("http://image.hugegraph.com/image1", user.avatar());
    }

    @Test
    public void testListUsers() {
        AuthManager authManager = authManager();

        Id id1 = authManager.createUser(makeUser("tom001", "pass1"), false);
        Id id2 = authManager.createUser(makeUser("james", "pass2"), false);

        List<HugeUser> users = authManager.listUsers(
                               ImmutableList.of(id1, id2), false);
        Assert.assertEquals(2, users.size());
        Assert.assertEquals("tom001", users.get(0).name());
        Assert.assertEquals("james", users.get(1).name());

        users = authManager.listUsers(ImmutableList.of(id1, id2, id2), false);
        Assert.assertEquals(3, users.size());
        Assert.assertEquals("tom001", users.get(0).name());
        Assert.assertEquals("james", users.get(1).name());
        Assert.assertEquals("james", users.get(2).name());

        users = authManager.listUsers(
                ImmutableList.of(id1, id2, IdGenerator.of("fake")), false);
        Assert.assertEquals(2, users.size());
    }

    @Test
    public void testListAllUsers() {
        AuthManager authManager = authManager();

        authManager.createUser(makeUser("tom001", "pass1"), false);
        authManager.createUser(makeUser("james", "pass2"), false);

        List<HugeUser> users = authManager.listAllUsers(-1, false);
        Assert.assertEquals(3, users.size());
        Assert.assertEquals(ImmutableSet.of("admin", "james", "tom001"),
                            ImmutableSet.of(users.get(0).name(),
                                            users.get(1).name(),
                                            users.get(2).name()));

        Assert.assertEquals(0, authManager.listAllUsers(0, false).size());
        Assert.assertEquals(1, authManager.listAllUsers(1, false).size());
        Assert.assertEquals(2, authManager.listAllUsers(2, false).size());
        Assert.assertEquals(3, authManager.listAllUsers(3, false).size());
    }

    @Test
    public void testGetUser() {
        AuthManager authManager = authManager();

        Id id = authManager.createUser(makeUser("tom002", "pass2"), false);

        HugeUser user = authManager.getUser(id, false);
        Assert.assertEquals("tom002", user.name());
        Assert.assertEquals("pass2", user.password());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            authManager.getUser(IdGenerator.of("fake"), false);
        });

        Assert.assertThrows(NullPointerException.class, () -> {
            authManager.getUser(null, false);
        });
    }

    @Test
    public void testMatchUser() {
        AuthManager authManager = authManager();

        String password = StringEncoding.hashPassword("pass1");
        authManager.createUser(makeUser("tom00m", password), false);

        Assert.assertNotNull(authManager.matchUser("tom00m", "pass1"));
        Assert.assertNull(authManager.matchUser("tom00m", "pass2"));
        Assert.assertNull(authManager.matchUser("Tom001", "pass1"));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            authManager.matchUser("Tom001", null);
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            authManager.matchUser(null, "pass1");
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            authManager.matchUser(null, null);
        });
    }

    @Test
    public void testUpdateUser() throws InterruptedException {
        AuthManager authManager = authManager();

        Id id = authManager.createUser(makeUser("tom001", "pass1"), false);
        HugeUser user = authManager.getUser(id, false);
        Assert.assertEquals("tom001", user.name());
        Assert.assertEquals("pass1", user.password());
        Assert.assertEquals(user.create(), user.update());

        Date oldUpdateTime = user.update();
        Thread.sleep(1000L);

        user.password("pass2");
        authManager.updateUser(user, false);

        HugeUser user2 = authManager.getUser(id, false);
        Assert.assertEquals("tom001", user2.name());
        Assert.assertEquals("pass2", user2.password());
        Assert.assertEquals(oldUpdateTime, user2.create());
        Assert.assertNotEquals(oldUpdateTime, user2.update());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            authManager.updateUser(makeUser("tom002", "pass1"), false);
        }, e -> {
            Assert.assertContains("The user name", e.getMessage());
            Assert.assertContains("does not existed", e.getMessage());
        });
    }

    @Test
    public void testDeleteUser() {
        AuthManager authManager = authManager();

        Id id1 = authManager.createUser(makeUser("tom001", "pass1"), false);
        Id id2 = authManager.createUser(makeUser("james", "pass2"), false);
        Assert.assertEquals(3, authManager.listAllUsers(-1, false).size());

        HugeUser user = authManager.deleteUser(id1, false);
        Assert.assertEquals("tom001", user.name());
        Assert.assertEquals(2, authManager.listAllUsers(-1, false).size());

        user = authManager.deleteUser(id2, false);
        Assert.assertEquals("james", user.name());
        Assert.assertEquals(1, authManager.listAllUsers(-1, false).size());
    }

    @Test
    public void testCreateGroup() {
        AuthManager authManager = authManager();

        HugeGroup group = makeGroup("group1");
        Id id = authManager.createGroup(DEFAULT_GRAPH_SPACE, group, false);

        group = authManager.getGroup(DEFAULT_GRAPH_SPACE, id, false);
        Assert.assertEquals("group1", group.name());
        Assert.assertEquals(null, group.description());
        Assert.assertEquals(group.create(), group.update());
        Assert.assertEquals(DEFAULT_GRAPH_SPACE, group.graphSpace());
        Assert.assertEquals("group1", group.id().asString());

        group = makeGroup("group2");
        group.description("something");
        id = authManager.createGroup(DEFAULT_GRAPH_SPACE, group ,false);

        group = authManager.getGroup(DEFAULT_GRAPH_SPACE, id, false);
        Assert.assertEquals("group2", group.name());
        Assert.assertEquals("something", group.description());
        Assert.assertEquals(group.create(), group.update());
    }

    @Test
    public void testListGroups() {
        AuthManager authManager = authManager();

        Id id1 = authManager.createGroup(DEFAULT_GRAPH_SPACE,
                                         makeGroup("group1"), false);
        Id id2 = authManager.createGroup(DEFAULT_GRAPH_SPACE,
                                         makeGroup("group2"), false);

        List<HugeGroup> groups = authManager.listGroups(DEFAULT_GRAPH_SPACE,
                                                        ImmutableList.of(id1,
                                                                         id2),
                                                        false);
        Assert.assertEquals(2, groups.size());
        Assert.assertEquals("group1", groups.get(0).name());
        Assert.assertEquals("group2", groups.get(1).name());

        groups = authManager.listGroups(DEFAULT_GRAPH_SPACE,
                                        ImmutableList.of(id1, id2, id2),
                                        false);
        Assert.assertEquals(3, groups.size());
        Assert.assertEquals("group1", groups.get(0).name());
        Assert.assertEquals("group2", groups.get(1).name());
        Assert.assertEquals("group2", groups.get(2).name());

        groups = authManager.listGroups(DEFAULT_GRAPH_SPACE, ImmutableList.of(
                                        id1, id2, IdGenerator.of("fake")),
                                        false);
        Assert.assertEquals(2, groups.size());
    }

    @Test
    public void testListAllGroups() {
        AuthManager authManager = authManager();

        authManager.createGroup(DEFAULT_GRAPH_SPACE,
                                makeGroup("group1"), false);
        authManager.createGroup(DEFAULT_GRAPH_SPACE,
                                makeGroup("group2"), false);

        List<HugeGroup> groups = authManager.listAllGroups(DEFAULT_GRAPH_SPACE,
                                                     -1, false);
        Assert.assertEquals(2, groups.size());
        Assert.assertEquals(ImmutableSet.of("group1", "group2"),
                            ImmutableSet.of(groups.get(0).name(),
                                            groups.get(1).name()));

        Assert.assertEquals(0, authManager.listAllGroups(DEFAULT_GRAPH_SPACE, 0, false).size());
        Assert.assertEquals(1, authManager.listAllGroups(DEFAULT_GRAPH_SPACE, 1, false).size());
        Assert.assertEquals(2, authManager.listAllGroups(DEFAULT_GRAPH_SPACE, 2, false).size());
        Assert.assertEquals(2, authManager.listAllGroups(DEFAULT_GRAPH_SPACE, 3, false).size());
    }

    @Test
    public void testGetGroup() {
        AuthManager authManager = authManager();

        Id id = authManager.createGroup(DEFAULT_GRAPH_SPACE,
                                        makeGroup("group-test"), false);
        HugeGroup group = authManager.getGroup(DEFAULT_GRAPH_SPACE, id, false);
        Assert.assertEquals("group-test", group.name());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            authManager.getGroup(DEFAULT_GRAPH_SPACE, IdGenerator.of("fake"),
                                 false);
        });

        Assert.assertThrows(NullPointerException.class, () -> {
            authManager.getGroup(DEFAULT_GRAPH_SPACE, null, false);
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id user = authManager.createUser(makeUser("tom001", "pass1"),
                                             false);
            authManager.getGroup(DEFAULT_GRAPH_SPACE, user, false);
        });
    }

    @Test
    public void testUpdateGroup() throws InterruptedException {
        AuthManager authManager = authManager();

        HugeGroup group = makeGroup("group1");
        group.description("description1");
        Id id = authManager.createGroup(DEFAULT_GRAPH_SPACE, group, false);

        group = authManager.getGroup(DEFAULT_GRAPH_SPACE, id, false);
        Assert.assertEquals("group1", group.name());
        Assert.assertEquals("description1", group.description());
        Assert.assertEquals(group.create(), group.update());

        Date oldUpdateTime = group.update();
        Thread.sleep(1000L);

        group.description("description2");
        authManager.updateGroup(DEFAULT_GRAPH_SPACE, group, false);

        HugeGroup group2 = authManager.getGroup(DEFAULT_GRAPH_SPACE, id,
                                                false);
        Assert.assertEquals("group1", group2.name());
        Assert.assertEquals("description2", group2.description());
        Assert.assertEquals(oldUpdateTime, group2.create());
        Assert.assertNotEquals(oldUpdateTime, group2.update());
    }

    @Test
    public void testDeleteGroup() {
        AuthManager authManager = authManager();

        Id id1 = authManager.createGroup(DEFAULT_GRAPH_SPACE,
                                         makeGroup("group1"), false);
        Id id2 = authManager.createGroup(DEFAULT_GRAPH_SPACE,
                                         makeGroup("group2"), false);
        Assert.assertEquals(2, authManager.listAllGroups(DEFAULT_GRAPH_SPACE,
                                        -1, false).size());

        HugeGroup group = authManager.deleteGroup(DEFAULT_GRAPH_SPACE, id1,
                                                  false);
        Assert.assertEquals("group1", group.name());
        Assert.assertEquals(1, authManager.listAllGroups(DEFAULT_GRAPH_SPACE,
                                        -1, false).size());

        group = authManager.deleteGroup(DEFAULT_GRAPH_SPACE, id2, false);
        Assert.assertEquals("group2", group.name());
        Assert.assertEquals(0, authManager.listAllGroups(DEFAULT_GRAPH_SPACE,
                                        -1, false).size());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id user = authManager.createUser(makeUser("tom001", "pass1"),
                                             false);
            authManager.deleteGroup(DEFAULT_GRAPH_SPACE, user, false);
        });
    }

    @Test
    public void testCreateTarget() {
        AuthManager authManager = authManager();

        HugeTarget target = makeTarget("graph1");
        target.creator("admin");
        Id id = authManager.createTarget(DEFAULT_GRAPH_SPACE, target, false);

        target = authManager.getTarget(DEFAULT_GRAPH_SPACE, id, false);
        Assert.assertEquals("graph1", target.name());
        Assert.assertEquals(target.create(), target.update());

        HashMap<String, Object> expected = new HashMap<>();
        expected.putAll(ImmutableMap.of("target_name", "graph1",
                                        "target_graph", "graph1",
                                        "target_creator", "admin"));
        expected.putAll(ImmutableMap.of("target_create", target.create(),
                                        "target_update", target.update(),
                                        "id", target.id()));
    }

    @Test
    public void testCreateTargetWithRess() {
        AuthManager authManager = authManager();

        String ress = "[{\"type\": \"VERTEX\", \"label\": \"person\", " +
                      "\"properties\":{\"city\": \"Beijing\"}}, " +
                      "{\"type\": \"EDGE\", \"label\": \"transfer\"}]";
        HugeTarget target = makeTarget("graph1");
        target.resources(ress);
        Id id = authManager.createTarget(DEFAULT_GRAPH_SPACE, target, false);

        target = authManager.getTarget(DEFAULT_GRAPH_SPACE, id, false);
        Assert.assertEquals("graph1", target.name());
        Assert.assertEquals(target.create(), target.update());

        String expect = "[{\"type\":\"VERTEX\",\"label\":\"person\"," +
                        "\"properties\":{\"city\":\"Beijing\"}}," +
                        "{\"type\":\"EDGE\",\"label\":\"transfer\"," +
                        "\"properties\":null}]";
        Assert.assertEquals(expect, JsonUtil.toJson(target.asMap()
                                                    .get("target_resources")));
    }

    @Test
    public void testListTargets() {
        AuthManager authManager = authManager();

        Id id1 = authManager.createTarget(DEFAULT_GRAPH_SPACE,
                 makeTarget("target1"), false);
        Id id2 = authManager.createTarget(DEFAULT_GRAPH_SPACE,
                 makeTarget("target2"), false);

        List<HugeTarget> targets = authManager.listTargets(DEFAULT_GRAPH_SPACE,
                                   ImmutableList.of(id1, id2), false);
        Assert.assertEquals(2, targets.size());
        Assert.assertEquals("target1", targets.get(0).name());
        Assert.assertEquals("target2", targets.get(1).name());

        targets = authManager.listTargets(DEFAULT_GRAPH_SPACE,
                  ImmutableList.of(id1, id2, id2), false);
        Assert.assertEquals(3, targets.size());
        Assert.assertEquals("target1", targets.get(0).name());
        Assert.assertEquals("target2", targets.get(1).name());
        Assert.assertEquals("target2", targets.get(2).name());

        targets = authManager.listTargets(DEFAULT_GRAPH_SPACE,
                  ImmutableList.of(id1, id2, IdGenerator.of("fake")), false);
        Assert.assertEquals(2, targets.size());
    }

    @Test
    public void testListAllTargets() {
        AuthManager authManager = authManager();

        authManager.createTarget(DEFAULT_GRAPH_SPACE, makeTarget("target1"),
                                 false);
        authManager.createTarget(DEFAULT_GRAPH_SPACE, makeTarget("target2"),
                                 false);

        List<HugeTarget> targets = authManager.listAllTargets(
                                   DEFAULT_GRAPH_SPACE, -1, false);
        Assert.assertEquals(2, targets.size());
        Assert.assertEquals(ImmutableSet.of("target1", "target2"),
                            ImmutableSet.of(targets.get(0).name(),
                                            targets.get(1).name()));

        Assert.assertEquals(0, authManager.listAllTargets(DEFAULT_GRAPH_SPACE,
                                        0, false).size());
        Assert.assertEquals(1, authManager.listAllTargets(DEFAULT_GRAPH_SPACE,
                                        1, false).size());
        Assert.assertEquals(2, authManager.listAllTargets(DEFAULT_GRAPH_SPACE,
                                        2, false).size());
        Assert.assertEquals(2, authManager.listAllTargets(DEFAULT_GRAPH_SPACE,
                                        3, false).size());
    }

    @Test
    public void testGetTarget() {
        AuthManager authManager = authManager();

        Id id = authManager.createTarget(DEFAULT_GRAPH_SPACE,
                                         makeTarget("target-test"),
                                         false);
        HugeTarget target = authManager.getTarget(DEFAULT_GRAPH_SPACE,
                                                  id,
                                                  false);
        Assert.assertEquals("target-test", target.name());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            authManager.getTarget(DEFAULT_GRAPH_SPACE,
                                  IdGenerator.of("fake"),
                                  false);
        });

        Assert.assertThrows(NullPointerException.class, () -> {
            authManager.getTarget(DEFAULT_GRAPH_SPACE, null, false);
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id user = authManager.createUser(makeUser("tom001", "pass1"),
                                             false);
            authManager.getTarget(DEFAULT_GRAPH_SPACE, user, false);
        });
    }

    @Test
    public void testUpdateTarget() throws InterruptedException {
        AuthManager authManager = authManager();

        HugeTarget target = makeTarget("target1");
        Id id = authManager.createTarget(DEFAULT_GRAPH_SPACE, target, false);

        target = authManager.getTarget(DEFAULT_GRAPH_SPACE, id, false);
        Assert.assertEquals("target1", target.name());
        Assert.assertEquals(target.create(), target.update());

        Date oldUpdateTime = target.update();
        Thread.sleep(1000L);

        authManager.updateTarget(DEFAULT_GRAPH_SPACE, target, false);

        HugeTarget target2 = authManager.getTarget(DEFAULT_GRAPH_SPACE,
                                                   id,
                                                   false);
        Assert.assertEquals("target1", target2.name());
        Assert.assertEquals(oldUpdateTime, target2.create());
        Assert.assertNotEquals(oldUpdateTime, target2.update());
    }

    @Test
    public void testDeleteTarget() {
        AuthManager authManager = authManager();

        Id id1 = authManager.createTarget(DEFAULT_GRAPH_SPACE,
                                          makeTarget("target1"),
                                          false);
        Id id2 = authManager.createTarget(DEFAULT_GRAPH_SPACE,
                                          makeTarget("target2"),
                                          false);
        Assert.assertEquals(2, authManager.listAllTargets(DEFAULT_GRAPH_SPACE,
                                        -1, false).size());

        HugeTarget target = authManager.deleteTarget(DEFAULT_GRAPH_SPACE,
                                                     id1,
                                                     false);
        Assert.assertEquals("target1", target.name());
        Assert.assertEquals(1, authManager.listAllTargets(DEFAULT_GRAPH_SPACE,
                                        -1, false).size());

        target = authManager.deleteTarget(DEFAULT_GRAPH_SPACE, id2, false);
        Assert.assertEquals("target2", target.name());
        Assert.assertEquals(0, authManager.listAllTargets(DEFAULT_GRAPH_SPACE,
                                        -1, false).size());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id user = authManager.createUser(makeUser("tom001", "pass1"),
                                             false);
            authManager.deleteTarget(DEFAULT_GRAPH_SPACE, user, false);
        });
    }

    @Test
    public void testCreateBelong() {
        AuthManager authManager = authManager();

        Id user = authManager.createUser(makeUser("tom", "pass1"), false);
        Id group1 = authManager.createGroup(DEFAULT_GRAPH_SPACE,
                                            makeGroup("group1"),
                                            false);
        Id group2 = authManager.createGroup(DEFAULT_GRAPH_SPACE,
                                            makeGroup("group2"),
                                            false);

        Id id1 = authManager.createBelong(DEFAULT_GRAPH_SPACE,
                                          makeBelong(user, group1),
                                          false);
        Id id2 = authManager.createBelong(DEFAULT_GRAPH_SPACE,
                                          makeBelong(user, group2),
                                          false);

        HugeBelong belong = authManager.getBelong(DEFAULT_GRAPH_SPACE,
                                                  id1,
                                                  false);
        Assert.assertEquals(user, belong.source());
        Assert.assertEquals(group1, belong.target());
        Assert.assertEquals(null, belong.description());
        Assert.assertEquals(belong.create(), belong.update());

        belong = authManager.getBelong(DEFAULT_GRAPH_SPACE, id2, false);
        Assert.assertEquals(user, belong.source());
        Assert.assertEquals(group2, belong.target());
        Assert.assertEquals(null, belong.description());
        Assert.assertEquals(belong.create(), belong.update());

        List<HugeBelong> belongs = authManager.listBelongByUser(
                         DEFAULT_GRAPH_SPACE, user, -1, false);
        Assert.assertEquals(2, belongs.size());

        belongs = authManager.listBelongByGroup(DEFAULT_GRAPH_SPACE, group1,
                                                -1, false);
        Assert.assertEquals(1, belongs.size());

        belongs = authManager.listBelongByGroup(DEFAULT_GRAPH_SPACE, group2,
                                                -1, false);
        Assert.assertEquals(1, belongs.size());

        // Create belong with description
        Id user1 = authManager.createUser(makeUser("user1", "pass1"), false);
        belong = makeBelong(user1, group1);
        belong.description("something2");
        Id id3 = authManager.createBelong(DEFAULT_GRAPH_SPACE, belong, false);
        belong = authManager.getBelong(DEFAULT_GRAPH_SPACE, id3, false);
        Assert.assertEquals(user1, belong.source());
        Assert.assertEquals(group1, belong.target());
        Assert.assertEquals("something2", belong.description());
        Assert.assertEquals(belong.create(), belong.update());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            authManager.createBelong(DEFAULT_GRAPH_SPACE,
                                     makeBelong(user, group1),
                                     false);
        }, e -> {
            Assert.assertContains("The belong name", e.getMessage());
            Assert.assertContains("has existed", e.getMessage());
        });
    }

    @Test
    public void testListBelong() {
        AuthManager authManager = authManager();

        Id user = authManager.createUser(makeUser("tom001", "pass1"),
                                         false);
        Id group1 = authManager.createGroup(DEFAULT_GRAPH_SPACE,
                                            makeGroup("group1"),
                                            false);
        Id group2 = authManager.createGroup(DEFAULT_GRAPH_SPACE,
                                            makeGroup("group2"),
                                            false);

        Id id1 = authManager.createBelong(DEFAULT_GRAPH_SPACE,
                                          makeBelong(user, group1),
                                          false);
        Id id2 = authManager.createBelong(DEFAULT_GRAPH_SPACE,
                                          makeBelong(user, group2),
                                          false);

        List<HugeBelong> belongs = authManager.listBelong(DEFAULT_GRAPH_SPACE,
                         ImmutableList.of(id1, id2), false);
        Assert.assertEquals(2, belongs.size());
        Assert.assertEquals(user, belongs.get(0).source());
        Assert.assertEquals(user, belongs.get(1).source());
        Assert.assertEquals(group1, belongs.get(0).target());
        Assert.assertEquals(group2, belongs.get(1).target());

        belongs = authManager.listBelong(DEFAULT_GRAPH_SPACE,
                                         ImmutableList.of(id1, id2, id2),
                                         false);
        Assert.assertEquals(3, belongs.size());

        belongs = authManager.listBelong(DEFAULT_GRAPH_SPACE,
                  ImmutableList.of(id1, id2, IdGenerator.of("fake")), false);
        Assert.assertEquals(2, belongs.size());

        belongs = authManager.listBelongByUser(DEFAULT_GRAPH_SPACE, user,
                                               -1, false);
        Assert.assertEquals(2, belongs.size());
        Assert.assertEquals(user, belongs.get(0).source());
        Assert.assertEquals(user, belongs.get(1).source());

        belongs = authManager.listBelongByGroup(DEFAULT_GRAPH_SPACE, group1,
                                                -1, false);
        Assert.assertEquals(1, belongs.size());
        Assert.assertEquals(user, belongs.get(0).source());
        Assert.assertEquals(group1, belongs.get(0).target());

        belongs = authManager.listBelongByGroup(DEFAULT_GRAPH_SPACE, group2,
                                                -1, false);
        Assert.assertEquals(1, belongs.size());
        Assert.assertEquals(user, belongs.get(0).source());
        Assert.assertEquals(group2, belongs.get(0).target());
    }

    @Test
    public void testListAllBelong() {
        AuthManager authManager = authManager();

        Id user = authManager.createUser(makeUser("tom001", "pass1"), false);
        Id group1 = authManager.createGroup(DEFAULT_GRAPH_SPACE,
                                            makeGroup("group1"),
                                            false);
        Id group2 = authManager.createGroup(DEFAULT_GRAPH_SPACE,
                                            makeGroup("group2"),
                                            false);

        authManager.createBelong(DEFAULT_GRAPH_SPACE,
                                 makeBelong(user, group1),
                                 false);
        authManager.createBelong(DEFAULT_GRAPH_SPACE,
                                 makeBelong(user, group2),
                                 false);

        List<HugeBelong> belongs = authManager.listAllBelong(
                         DEFAULT_GRAPH_SPACE, -1, false);
        Assert.assertEquals(2, belongs.size());
        Assert.assertEquals(ImmutableSet.of(group1, group2),
                            ImmutableSet.of(belongs.get(0).target(),
                                            belongs.get(1).target()));

        Assert.assertEquals(0, authManager.listAllBelong(DEFAULT_GRAPH_SPACE,
                                        0, false).size());
        Assert.assertEquals(1, authManager.listAllBelong(DEFAULT_GRAPH_SPACE,
                                        1, false).size());
        Assert.assertEquals(2, authManager.listAllBelong(DEFAULT_GRAPH_SPACE,
                                        2, false).size());
        Assert.assertEquals(2, authManager.listAllBelong(DEFAULT_GRAPH_SPACE,
                                        3, false).size());
    }

    @Test
    public void testGetBelong() {
        AuthManager authManager = authManager();

        Id user = authManager.createUser(makeUser("tom001", "pass1"), false);
        Id group1 = authManager.createGroup(DEFAULT_GRAPH_SPACE,
                                            makeGroup("group1"),
                                            false);
        Id group2 = authManager.createGroup(DEFAULT_GRAPH_SPACE,
                                            makeGroup("group2"),
                                            false);

        Id id1 = authManager.createBelong(DEFAULT_GRAPH_SPACE,
                                          makeBelong(user, group1),
                                          false);
        Id id2 = authManager.createBelong(DEFAULT_GRAPH_SPACE,
                                          makeBelong(user, group2),
                                          false);

        HugeBelong belong1 = authManager.getBelong(DEFAULT_GRAPH_SPACE,
                                                   id1,
                                                   false);
        Assert.assertEquals(group1, belong1.target());

        HugeBelong belong2 = authManager.getBelong(DEFAULT_GRAPH_SPACE,
                                                   id2,
                                                   false);
        Assert.assertEquals(group2, belong2.target());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            authManager.getBelong(DEFAULT_GRAPH_SPACE,
                                  IdGenerator.of("fake"),
                                  false);
        });

        Assert.assertThrows(NullPointerException.class, () -> {
            authManager.getBelong(DEFAULT_GRAPH_SPACE, null, false);
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id target = authManager.createTarget(DEFAULT_GRAPH_SPACE,
                                                 makeTarget("graph1"),
                                                 false);
            Id access = authManager.createAccess(DEFAULT_GRAPH_SPACE,
               makeAccess(group1, target, HugePermission.READ), false);
            authManager.getBelong(DEFAULT_GRAPH_SPACE, access, false);
        });
    }

    @Test
    public void testUpdateBelong() throws InterruptedException {
        AuthManager authManager = authManager();

        Id user = authManager.createUser(makeUser("tom001", "pass1"), false);
        Id group = authManager.createGroup(DEFAULT_GRAPH_SPACE,
                                           makeGroup("group1"),
                                           false);

        HugeBelong belong = makeBelong(user, group);
        belong.description("description1");
        Id id = authManager.createBelong(DEFAULT_GRAPH_SPACE, belong, false);

        belong = authManager.getBelong(DEFAULT_GRAPH_SPACE, id, false);
        Assert.assertEquals(user, belong.source());
        Assert.assertEquals(group, belong.target());
        Assert.assertEquals("description1", belong.description());
        Assert.assertEquals(belong.create(), belong.update());

        Date oldUpdateTime = belong.update();
        Thread.sleep(1000L);

        belong.description("description2");
        authManager.updateBelong(DEFAULT_GRAPH_SPACE, belong, false);

        HugeBelong belong2 = authManager.getBelong(DEFAULT_GRAPH_SPACE,
                                                   id, false);
        Assert.assertEquals(user, belong.source());
        Assert.assertEquals(group, belong.target());
        Assert.assertEquals("description2", belong.description());
        Assert.assertEquals(oldUpdateTime, belong2.create());
        Assert.assertNotEquals(oldUpdateTime, belong2.update());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id group2 = authManager.createGroup(DEFAULT_GRAPH_SPACE,
                                                makeGroup("group2"),
                                                false);
            HugeBelong belong3 = makeBelong(user, group2);
            authManager.updateBelong(DEFAULT_GRAPH_SPACE, belong3, false);
        }, e -> {
            Assert.assertContains("The belong name", e.getMessage());
            Assert.assertContains("is not existed", e.getMessage());
        });
    }

    @Test
    public void testDeleteBelong() {
        AuthManager authManager = authManager();

        Id user = authManager.createUser(makeUser("tom001", "pass1"), false);
        Id group1 = authManager.createGroup(DEFAULT_GRAPH_SPACE,
                                            makeGroup("group1"),
                                            false);
        Id group2 = authManager.createGroup(DEFAULT_GRAPH_SPACE,
                                            makeGroup("group2"),
                                            false);

        Id id1 = authManager.createBelong(DEFAULT_GRAPH_SPACE,
                                          makeBelong(user, group1),
                                          false);
        Id id2 = authManager.createBelong(DEFAULT_GRAPH_SPACE,
                                          makeBelong(user, group2),
                                          false);

        Assert.assertEquals(2, authManager.listAllBelong(DEFAULT_GRAPH_SPACE,
                                        -1, false).size());

        HugeBelong belong = authManager.deleteBelong(DEFAULT_GRAPH_SPACE,
                                                     id1,
                                                     false);
        Assert.assertEquals(group1, belong.target());
        Assert.assertEquals(1, authManager.listAllBelong(DEFAULT_GRAPH_SPACE,
                                        -1, false).size());
        Assert.assertEquals(1, authManager.listAllBelong(DEFAULT_GRAPH_SPACE,
                                        2, false).size());

        belong = authManager.deleteBelong(DEFAULT_GRAPH_SPACE, id2, false);
        Assert.assertEquals(group2, belong.target());
        Assert.assertEquals(0, authManager.listAllBelong(DEFAULT_GRAPH_SPACE,
                                        -1, false).size());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id target = authManager.createTarget(DEFAULT_GRAPH_SPACE,
                                                 makeTarget("graph1"),
                                                 false);
            Id access = authManager.createAccess(DEFAULT_GRAPH_SPACE,
               makeAccess(group1, target, HugePermission.READ), false);
            authManager.deleteBelong(DEFAULT_GRAPH_SPACE, access, false);
        });
    }

    @Test
    public void testCreateAccess() {
        AuthManager authManager = authManager();

        Id group1 = authManager.createGroup(DEFAULT_GRAPH_SPACE,
                                            makeGroup("group1"),
                                            false);
        Id group2 = authManager.createGroup(DEFAULT_GRAPH_SPACE,
                                            makeGroup("group2"),
                                            false);
        Id target1 = authManager.createTarget(DEFAULT_GRAPH_SPACE,
                                              makeTarget("graph1"),
                                              false);
        Id target2 = authManager.createTarget(DEFAULT_GRAPH_SPACE,
                                              makeTarget("graph2"),
                                              false);

        Id id1 = authManager.createAccess(DEFAULT_GRAPH_SPACE,
                 makeAccess(group1, target1, HugePermission.READ), false);
        Id id2 = authManager.createAccess(DEFAULT_GRAPH_SPACE,
                 makeAccess(group1, target1, HugePermission.WRITE), false);
        Id id3 = authManager.createAccess(DEFAULT_GRAPH_SPACE,
                 makeAccess(group1, target2, HugePermission.READ), false);
        Id id4 = authManager.createAccess(DEFAULT_GRAPH_SPACE,
                 makeAccess(group2, target2, HugePermission.READ), false);

        HugeAccess access = authManager.getAccess(DEFAULT_GRAPH_SPACE,
                                                  id1,
                                                  false);
        Assert.assertEquals(group1, access.source());
        Assert.assertEquals(target1, access.target());
        Assert.assertEquals(HugePermission.READ, access.permission());
        Assert.assertEquals(access.create(), access.update());

        access = authManager.getAccess(DEFAULT_GRAPH_SPACE, id2, false);
        Assert.assertEquals(group1, access.source());
        Assert.assertEquals(target1, access.target());
        Assert.assertEquals(HugePermission.WRITE, access.permission());
        Assert.assertEquals(access.create(), access.update());

        access = authManager.getAccess(DEFAULT_GRAPH_SPACE, id3, false);
        Assert.assertEquals(group1, access.source());
        Assert.assertEquals(target2, access.target());
        Assert.assertEquals(HugePermission.READ, access.permission());
        Assert.assertEquals(access.create(), access.update());

        access = authManager.getAccess(DEFAULT_GRAPH_SPACE, id4, false);
        Assert.assertEquals(group2, access.source());
        Assert.assertEquals(target2, access.target());
        Assert.assertEquals(HugePermission.READ, access.permission());
        Assert.assertEquals(access.create(), access.update());

        List<HugeAccess> accesses = authManager.listAccessByGroup(
                         DEFAULT_GRAPH_SPACE, group1, -1, false);
        Assert.assertEquals(3, accesses.size());

        accesses = authManager.listAccessByGroup(DEFAULT_GRAPH_SPACE, group2,
                                                 -1, false);
        Assert.assertEquals(1, accesses.size());

        accesses = authManager.listAccessByTarget(DEFAULT_GRAPH_SPACE, target1,
                                                  -1, false);
        Assert.assertEquals(2, accesses.size());

        accesses = authManager.listAccessByTarget(DEFAULT_GRAPH_SPACE, target2,
                                                  -1, false);
        Assert.assertEquals(2, accesses.size());

        // Create access with description
        access = makeAccess(group2, target2, HugePermission.WRITE);
        access.description("something3");
        Id id5 = authManager.createAccess(DEFAULT_GRAPH_SPACE, access, false);
        access = authManager.getAccess(DEFAULT_GRAPH_SPACE, id5, false);
        Assert.assertEquals(group2, access.source());
        Assert.assertEquals(target2, access.target());
        Assert.assertEquals(HugePermission.WRITE, access.permission());
        Assert.assertEquals("something3", access.description());
        Assert.assertEquals(access.create(), access.update());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            authManager.createAccess(DEFAULT_GRAPH_SPACE,
                                     makeAccess(group1, target1,
                                                HugePermission.READ),
                                     false);
        }, e -> {
            Assert.assertContains("The access name", e.getMessage());
            Assert.assertContains("has existed", e.getMessage());
        });
    }

    @Test
    public void testListAccess() {
        AuthManager authManager = authManager();

        Id group = authManager.createGroup(DEFAULT_GRAPH_SPACE,
                                           makeGroup("group1"),
                                           false);
        Id target1 = authManager.createTarget(DEFAULT_GRAPH_SPACE,
                                              makeTarget("graph1"),
                                              false);
        Id target2 = authManager.createTarget(DEFAULT_GRAPH_SPACE,
                                              makeTarget("graph2"),
                                              false);

        Id id1 = authManager.createAccess(DEFAULT_GRAPH_SPACE,
                                          makeAccess(group, target1,
                                                     HugePermission.READ),
                                          false);
        Id id2 = authManager.createAccess(DEFAULT_GRAPH_SPACE,
                                          makeAccess(group, target2,
                                                     HugePermission.READ),
                                          false);

        List<HugeAccess> access = authManager.listAccess(DEFAULT_GRAPH_SPACE,
                                  ImmutableList.of(id1, id2), false);
        Assert.assertEquals(2, access.size());
        Assert.assertEquals(group, access.get(0).source());
        Assert.assertEquals(group, access.get(1).source());
        Assert.assertEquals(target1, access.get(0).target());
        Assert.assertEquals(target2, access.get(1).target());

        access = authManager.listAccess(DEFAULT_GRAPH_SPACE,
                                        ImmutableList.of(id1, id2, id2),
                                        false);
        Assert.assertEquals(3, access.size());

        access = authManager.listAccess(DEFAULT_GRAPH_SPACE,
                 ImmutableList.of(id1, id2, IdGenerator.of("fake")), false);
        Assert.assertEquals(2, access.size());

        access = authManager.listAccessByGroup(DEFAULT_GRAPH_SPACE, group,
                                               -1, false);
        Assert.assertEquals(2, access.size());
        Assert.assertEquals(group, access.get(0).source());
        Assert.assertEquals(group, access.get(1).source());

        access = authManager.listAccessByTarget(DEFAULT_GRAPH_SPACE, target1,
                                                -1, false);
        Assert.assertEquals(1, access.size());
        Assert.assertEquals(group, access.get(0).source());
        Assert.assertEquals(target1, access.get(0).target());

        access = authManager.listAccessByTarget(DEFAULT_GRAPH_SPACE, target2,
                                                -1, false);
        Assert.assertEquals(1, access.size());
        Assert.assertEquals(group, access.get(0).source());
        Assert.assertEquals(target2, access.get(0).target());
    }

    @Test
    public void testListAllAccess() {
        AuthManager authManager = authManager();

        Id group = authManager.createGroup(DEFAULT_GRAPH_SPACE,
                                           makeGroup("group1"),
                                           false);
        Id target1 = authManager.createTarget(DEFAULT_GRAPH_SPACE,
                                              makeTarget("graph1"),
                                              false);
        Id target2 = authManager.createTarget(DEFAULT_GRAPH_SPACE,
                                              makeTarget("graph2"),
                                              false);

        authManager.createAccess(DEFAULT_GRAPH_SPACE,
                                 makeAccess(group, target1,
                                            HugePermission.READ),
                                 false);
        authManager.createAccess(DEFAULT_GRAPH_SPACE,
                                 makeAccess(group, target2,
                                            HugePermission.READ),
                                 false);

        List<HugeAccess> access = authManager.listAllAccess(
                                  DEFAULT_GRAPH_SPACE, -1, false);
        Assert.assertEquals(2, access.size());
        Assert.assertEquals(ImmutableSet.of(target1, target2),
                            ImmutableSet.of(access.get(0).target(),
                                            access.get(1).target()));

        Assert.assertEquals(0, authManager.listAllAccess(DEFAULT_GRAPH_SPACE,
                            0, false).size());
        Assert.assertEquals(1, authManager.listAllAccess(DEFAULT_GRAPH_SPACE,
                            1, false).size());
        Assert.assertEquals(2, authManager.listAllAccess(DEFAULT_GRAPH_SPACE,
                            2, false).size());
        Assert.assertEquals(2, authManager.listAllAccess(DEFAULT_GRAPH_SPACE,
                            3, false).size());
    }

    @Test
    public void testGetAccess() {
        AuthManager authManager = authManager();

        Id group = authManager.createGroup(DEFAULT_GRAPH_SPACE,
                                           makeGroup("group1"),
                                           false);
        Id target1 = authManager.createTarget(DEFAULT_GRAPH_SPACE,
                                              makeTarget("graph1"),
                                              false);
        Id target2 = authManager.createTarget(DEFAULT_GRAPH_SPACE,
                                              makeTarget("graph2"),
                                              false);

        Id id1 = authManager.createAccess(DEFAULT_GRAPH_SPACE,
                                          makeAccess(group, target1,
                                                     HugePermission.READ),
                                          false);
        Id id2 = authManager.createAccess(DEFAULT_GRAPH_SPACE,
                                          makeAccess(group, target2,
                                                     HugePermission.READ),
                                          false);

        HugeAccess access1 = authManager.getAccess(DEFAULT_GRAPH_SPACE,
                                                   id1, false);
        Assert.assertEquals(target1, access1.target());

        HugeAccess access2 = authManager.getAccess(DEFAULT_GRAPH_SPACE,
                                                   id2, false);
        Assert.assertEquals(target2, access2.target());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            authManager.getAccess(DEFAULT_GRAPH_SPACE,
                                  IdGenerator.of("fake"),
                                  false);
        });

        Assert.assertThrows(NullPointerException.class, () -> {
            authManager.getAccess(DEFAULT_GRAPH_SPACE, null, false);
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id user = authManager.createUser(makeUser("tom001", "pass1"),
                                                      false);
            Id belong = authManager.createBelong(DEFAULT_GRAPH_SPACE,
                                                 makeBelong(user, group),
                                                 false);
            authManager.getAccess(DEFAULT_GRAPH_SPACE, belong, false);
        });
    }

    @Test
    public void testUpdateAccess() throws InterruptedException {
        AuthManager authManager = authManager();

        Id group = authManager.createGroup(DEFAULT_GRAPH_SPACE,
                                           makeGroup("group1"),
                                           false);
        Id target = authManager.createTarget(DEFAULT_GRAPH_SPACE,
                                             makeTarget("graph1"),
                                             false);
        Id id = authManager.createAccess(DEFAULT_GRAPH_SPACE,
                                         makeAccess(group, target,
                                                    HugePermission.READ),
                                         false);

        HugeAccess access = authManager.getAccess(DEFAULT_GRAPH_SPACE,
                                                  id, false);
        Assert.assertEquals(group, access.source());
        Assert.assertEquals(target, access.target());
        Assert.assertEquals(HugePermission.READ, access.permission());
        Assert.assertEquals(access.create(), access.update());

        Date oldUpdateTime = access.update();
        Thread.sleep(1000L);

        access.permission(HugePermission.READ);
        authManager.updateAccess(DEFAULT_GRAPH_SPACE, access, false);

        HugeAccess access2 = authManager.getAccess(DEFAULT_GRAPH_SPACE,
                                                   id, false);
        Assert.assertEquals(group, access.source());
        Assert.assertEquals(target, access.target());
        Assert.assertEquals(HugePermission.READ, access.permission());
        Assert.assertEquals(oldUpdateTime, access2.create());
        Assert.assertNotEquals(oldUpdateTime, access2.update());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            access.permission(HugePermission.WRITE);
            authManager.updateAccess(DEFAULT_GRAPH_SPACE, access, false);
        }, e -> {
            Assert.assertContains("The access name", e.getMessage());
            Assert.assertContains("is not existed", e.getMessage());
        });

        access.permission(HugePermission.READ);
        access.description("description updated");
        HugeAccess access1 = authManager.updateAccess(DEFAULT_GRAPH_SPACE, access, false);

        HugeAccess access3 = authManager.getAccess(DEFAULT_GRAPH_SPACE,
                                                   access1.id(), false);
        Assert.assertEquals(group, access3.source());
        Assert.assertEquals(target, access3.target());
        Assert.assertEquals("description updated", access3.description());
        Assert.assertEquals(HugePermission.READ, access3.permission());
        Assert.assertEquals(oldUpdateTime, access3.create());
        Assert.assertNotEquals(access3.create(), access3.update());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            HugeAccess access4 = makeAccess(group, target,
                                            HugePermission.DELETE);
            authManager.updateAccess(DEFAULT_GRAPH_SPACE, access4, false);
        }, e -> {
            Assert.assertContains("The access name", e.getMessage());
            Assert.assertContains("is not existed", e.getMessage());
        });
    }

    @Test
    public void testDeleteAccess() {
        AuthManager authManager = authManager();

        Id group = authManager.createGroup(DEFAULT_GRAPH_SPACE,
                                           makeGroup("group1"),
                                           false);
        Id target1 = authManager.createTarget(DEFAULT_GRAPH_SPACE,
                                              makeTarget("graph1"),
                                              false);
        Id target2 = authManager.createTarget(DEFAULT_GRAPH_SPACE,
                                              makeTarget("graph2"),
                                              false);

        Id id1 = authManager.createAccess(DEFAULT_GRAPH_SPACE,
                                          makeAccess(group, target1,
                                                     HugePermission.READ),
                                          false);
        Id id2 = authManager.createAccess(DEFAULT_GRAPH_SPACE,
                                          makeAccess(group, target2,
                                                     HugePermission.READ),
                                          false);

        Assert.assertEquals(2, authManager.listAllAccess(DEFAULT_GRAPH_SPACE,
                            -1, false).size());

        HugeAccess access = authManager.deleteAccess(DEFAULT_GRAPH_SPACE,
                                                     id1, false);
        Assert.assertEquals(target1, access.target());
        Assert.assertEquals(1, authManager.listAllAccess(DEFAULT_GRAPH_SPACE,
                            -1, false).size());
        Assert.assertEquals(1, authManager.listAllAccess(DEFAULT_GRAPH_SPACE,
                            2, false).size());

        access = authManager.deleteAccess(DEFAULT_GRAPH_SPACE, id2, false);
        Assert.assertEquals(target2, access.target());
        Assert.assertEquals(0, authManager.listAllAccess(DEFAULT_GRAPH_SPACE,
                            -1, false).size());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id user = authManager.createUser(makeUser("tom001", "pass1"),
                                             false);
            Id belong = authManager.createBelong(DEFAULT_GRAPH_SPACE,
                                                 makeBelong(user, group),
                                                 false);
            authManager.deleteAccess(DEFAULT_GRAPH_SPACE, belong, false);
        });
    }

    @Test
    public void testRolePermission() {
        AuthManager authManager = authManager();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            authManager.createUser(makeUser("admin", "passadmin"), false);
        });

        Id user0 = authManager.createUser(makeUser("hugegraph", "pass0"),
                                          false);
        Id user1 = authManager.createUser(makeUser("hugegraph1", "pass1"),
                                          false);

        Id group1 = authManager.createGroup(DEFAULT_GRAPH_SPACE,
                                            makeGroup("group1"),
                                            false);
        Id group2 = authManager.createGroup(DEFAULT_GRAPH_SPACE,
                                            makeGroup("group2"),
                                            false);

        Id graph1 = authManager.createTarget(DEFAULT_GRAPH_SPACE,
                                             makeTarget("hugegraph"),
                                             false);
        Id graph2 = authManager.createTarget(DEFAULT_GRAPH_SPACE,
                                             makeTarget("hugegraph1"),
                                             false);

        List<HugeResource> rv = HugeResource.parseResources(
            "[{\"type\": \"VERTEX\", \"label\": \"person\", " +
            "\"properties\":{\"city\": \"Beijing\", \"age\": \"P.gte(20)\"}}," +
            " {\"type\": \"VERTEX_LABEL\", \"label\": \"*\"}," +
            " {\"type\": \"PROPERTY_KEY\", \"label\": \"*\"}]");
        List<HugeResource> re = HugeResource.parseResources(
            "[{\"type\": \"EDGE\", \"label\": \"write\"}, " +
            " {\"type\": \"PROPERTY_KEY\"}, {\"type\": \"VERTEX_LABEL\"}, " +
            " {\"type\": \"EDGE_LABEL\"}, {\"type\": \"INDEX_LABEL\"}]");
        List<HugeResource> rg = HugeResource.parseResources(
            "[{\"type\": \"GREMLIN\"}]");
        Id graph1v = authManager.createTarget(DEFAULT_GRAPH_SPACE,
                                              makeTarget("hugegraph-v",
                                                         "hugegraph",
                                                         rv),
                                              false);
        Id graph1e = authManager.createTarget(DEFAULT_GRAPH_SPACE,
                                              makeTarget("hugegraph-e",
                                                         "hugegraph",
                                                         re),
                                              false);
        Id graph1gremlin = authManager.createTarget(DEFAULT_GRAPH_SPACE,
                                                    makeTarget("hugegraph-g",
                                                               "hugegraph",
                                                               rg),
                                                    false);

        Id belong1 = authManager.createBelong(DEFAULT_GRAPH_SPACE,
                                              makeBelong(user0, group1),
                                              false);
        Id belong2 = authManager.createBelong(DEFAULT_GRAPH_SPACE,
                                              makeBelong(user1, group2),
                                              false);

        authManager.createAccess(DEFAULT_GRAPH_SPACE,
                                 makeAccess(group1, graph1,
                                            HugePermission.READ),
                                 false);
        authManager.createAccess(DEFAULT_GRAPH_SPACE,
                                 makeAccess(group1, graph1,
                                            HugePermission.WRITE),
                                 false);
        authManager.createAccess(DEFAULT_GRAPH_SPACE,
                                 makeAccess(group1, graph2,
                                            HugePermission.READ),
                                 false);
        authManager.createAccess(DEFAULT_GRAPH_SPACE,
                                 makeAccess(group2, graph2,
                                            HugePermission.READ),
                                 false);

        Id access1v = authManager.createAccess(DEFAULT_GRAPH_SPACE,
                                               makeAccess(group1, graph1v,
                                                          HugePermission.READ),
                                               false);
        authManager.createAccess(DEFAULT_GRAPH_SPACE,
                                 makeAccess(group1, graph1v,
                                            HugePermission.WRITE),
                                 false);
        authManager.createAccess(DEFAULT_GRAPH_SPACE,
                                 makeAccess(group1, graph1e,
                                            HugePermission.READ),
                                 false);
        Id access1g = authManager.createAccess(DEFAULT_GRAPH_SPACE,
                                               makeAccess(group1,
                                                          graph1gremlin,
                                                          HugePermission.EXECUTE),
                                               false);

        RolePermission role;
        role = authManager.rolePermission(authManager.getUser(user0, false));
        String expected = "{\"roles\":{\"DEFAULT\":{\"hugegraph\":{\"READ\":[{\"type\":\"EDGE\",\"label\":\"write\",\"properties\":null},{\"type\":\"PROPERTY_KEY\",\"label\":\"*\",\"properties\":null},{\"type\":\"VERTEX_LABEL\",\"label\":\"*\",\"properties\":null},{\"type\":\"EDGE_LABEL\",\"label\":\"*\",\"properties\":null},{\"type\":\"INDEX_LABEL\",\"label\":\"*\",\"properties\":null},{\"type\":\"VERTEX\",\"label\":\"person\",\"properties\":{\"city\":\"Beijing\",\"age\":\"P.gte(20)\"}},{\"type\":\"VERTEX_LABEL\",\"label\":\"*\",\"properties\":null},{\"type\":\"PROPERTY_KEY\",\"label\":\"*\",\"properties\":null}],\"WRITE\":[{\"type\":\"VERTEX\",\"label\":\"person\",\"properties\":{\"city\":\"Beijing\",\"age\":\"P.gte(20)\"}},{\"type\":\"VERTEX_LABEL\",\"label\":\"*\",\"properties\":null},{\"type\":\"PROPERTY_KEY\",\"label\":\"*\",\"properties\":null}],\"EXECUTE\":[{\"type\":\"GREMLIN\",\"label\":\"*\",\"properties\":null}]},\"hugegraph1\":{\"READ\":[]}}}}";
        Assert.assertEquals(expected, role.toJson());

        role = authManager.rolePermission(
               authManager.getBelong(DEFAULT_GRAPH_SPACE, belong1, false));
        Assert.assertEquals(expected, role.toJson());

        role = authManager.rolePermission(
               authManager.getGroup(DEFAULT_GRAPH_SPACE, group1, false));
        Assert.assertEquals(expected, role.toJson());

        role = authManager.rolePermission(
               authManager.getAccess(DEFAULT_GRAPH_SPACE, access1v, false));
        expected = "{\"roles\":{\"DEFAULT\":{\"hugegraph\":{\"READ\":[{\"type\":\"VERTEX\",\"label\":\"person\",\"properties\":{\"city\":\"Beijing\",\"age\":\"P.gte(20)\"}},{\"type\":\"VERTEX_LABEL\",\"label\":\"*\",\"properties\":null},{\"type\":\"PROPERTY_KEY\",\"label\":\"*\",\"properties\":null}]}}}}";
        Assert.assertEquals(expected, role.toJson());

        role = authManager.rolePermission(
               authManager.getAccess(DEFAULT_GRAPH_SPACE, access1g, false));
        expected = "{\"roles\":{\"DEFAULT\":{\"hugegraph\":{\"EXECUTE\":[{\"type\":\"GREMLIN\",\"label\":\"*\",\"properties\":null}]}}}}";
        Assert.assertEquals(expected, role.toJson());

        role = authManager.rolePermission(authManager.getUser(user1, false));
        expected = "{\"roles\":{\"DEFAULT\":{\"hugegraph1\":{\"READ\":[]}}}}";
        Assert.assertEquals(expected, role.toJson());

        role = authManager.rolePermission(
               authManager.getBelong(DEFAULT_GRAPH_SPACE, belong2, false));
        expected = "{\"roles\":{\"DEFAULT\":{\"hugegraph1\":{\"READ\":[]}}}}";
        Assert.assertEquals(expected, role.toJson());

        role = authManager.rolePermission(authManager.getTarget(DEFAULT_GRAPH_SPACE, graph1v, false));
        expected = "{\"roles\":{\"DEFAULT\":{\"hugegraph\":{\"READ\":[{\"type\":\"VERTEX\",\"label\":\"person\",\"properties\":{\"city\":\"Beijing\",\"age\":\"P.gte(20)\"}},{\"type\":\"VERTEX_LABEL\",\"label\":\"*\",\"properties\":null},{\"type\":\"PROPERTY_KEY\",\"label\":\"*\",\"properties\":null}]}}}}";
        Assert.assertEquals(expected, role.toJson());
    }

    @Test
    public void testLogin() throws AuthenticationException {
        AuthManager authManager = authManager();

        HugeUser user = makeUser("test001",
                                 StringEncoding.hashPassword("pass001"));
        authManager.createUser(user, false);

        // Login
        authManager.loginUser("test001", "pass001", 0);

        // Invalid username or password
        Assert.assertThrows(AuthenticationException.class, () -> {
            authManager.loginUser("huge", "graph", 0);
        }, e -> {
            Assert.assertContains("Incorrect username or password",
                                  e.getMessage());
        });
    }

    @Test
    public void testValidateUserByToken() throws AuthenticationException {
        AuthManager authManager = authManager();

        HugeUser user = makeUser("test001",
                                 StringEncoding.hashPassword("pass001"));
        Id userId = authManager.createUser(user, false);

        String token = authManager.loginUser("test001", "pass001", 0L);

        UserWithRole userWithRole;
        userWithRole = authManager.validateUser(token);
        Assert.assertEquals(userId, userWithRole.userId());
        Assert.assertEquals("test001", userWithRole.username());
        Assert.assertEquals("{\"roles\":{}}", userWithRole.role().toJson());

        // Token cache missed
        Cache<Id, String> tokenCache = Whitebox.getInternalState(authManager,
                                                                 "tokenCache");
        tokenCache.invalidate(IdGenerator.of(token));
        Assert.assertFalse(tokenCache.containsKey(IdGenerator.of(token)));

        userWithRole = authManager.validateUser(token);
        Assert.assertEquals(userId, userWithRole.userId());
        Assert.assertEquals("test001", userWithRole.username());
        Assert.assertEquals("{\"roles\":{}}", userWithRole.role().toJson());
        Assert.assertTrue(tokenCache.containsKey(IdGenerator.of(token)));

        // User deleted after login and token not expire
        authManager.deleteUser(userId, false);
        userWithRole = authManager.validateUser(token);
        Assert.assertEquals("test001", userWithRole.username());
        Assert.assertEquals(null, userWithRole.role());
    }

    @Test
    public void testLogout() throws AuthenticationException {
        AuthManager authManager = authManager();

        HugeUser user = makeUser("test001",
                                 StringEncoding.hashPassword("pass001"));
        Id userId = authManager.createUser(user, false);

        // Login
        String token = authManager.loginUser("test001", "pass001", 0);

        // Logout
        Cache<Id, String> tokenCache = Whitebox.getInternalState(authManager,
                                                                 "tokenCache");
        Assert.assertTrue(tokenCache.containsKey(IdGenerator.of(token)));
        authManager.logoutUser(token);
        Assert.assertFalse(tokenCache.containsKey(IdGenerator.of(token)));
    }

    @Test
    public void testValidateUserByNameAndPassword() {
        AuthManager authManager = authManager();

        HugeUser user = makeUser("test001", StringEncoding.hashPassword("pass001"));
        Id userId = authManager.createUser(user, false);

        UserWithRole userWithRole;
        userWithRole = authManager.validateUser("test001", "pass001");
        Assert.assertEquals(userId, userWithRole.userId());
        Assert.assertEquals("test001", userWithRole.username());
        Assert.assertEquals("{\"roles\":{}}", userWithRole.role().toJson());

        // Error case
        userWithRole = authManager.validateUser("huge", "graph");
        Assert.assertNull(userWithRole.userId());
        Assert.assertEquals("huge", userWithRole.username());
        Assert.assertNull(userWithRole.role());
    }

    private static HugeUser makeUser(String name, String password) {
        HugeUser user = new HugeUser(name);
        user.password(password);
        user.creator("admin");
        return user;
    }

    private static HugeGroup makeGroup(String name) {
        HugeGroup group = new HugeGroup(name, DEFAULT_GRAPH_SPACE);
        group.creator("admin");
        return group;
    }

    private static HugeTarget makeTarget(String name) {
        HugeTarget target = new HugeTarget(name, DEFAULT_GRAPH_SPACE);
        target.creator("admin");
        return target;
    }

    private static HugeTarget makeTarget(String name, String graph,
                                         List<HugeResource> ress) {
        HugeTarget target = new HugeTarget(name, DEFAULT_GRAPH_SPACE,  graph, ress);
        target.creator("admin");
        return target;
    }

    private static HugeBelong makeBelong(Id user, Id group) {
        HugeBelong belong = new HugeBelong(DEFAULT_GRAPH_SPACE, user, group);
        belong.creator("admin");
        return belong;
    }

    private static HugeAccess makeAccess(Id group, Id target,
                                         HugePermission permission) {
        HugeAccess access = new HugeAccess(DEFAULT_GRAPH_SPACE, group, target, permission);
        access.creator("admin");
        return access;
    }
}
