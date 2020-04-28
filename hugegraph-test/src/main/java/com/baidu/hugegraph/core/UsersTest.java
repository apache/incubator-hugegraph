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

import org.junit.After;
import org.junit.Test;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.auth.HugeAccess;
import com.baidu.hugegraph.auth.HugeBelong;
import com.baidu.hugegraph.auth.HugeGroup;
import com.baidu.hugegraph.auth.HugePermission;
import com.baidu.hugegraph.auth.HugeTarget;
import com.baidu.hugegraph.auth.HugeUser;
import com.baidu.hugegraph.auth.UserManager;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.StringEncoding;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class UsersTest extends BaseCoreTest {

    @After
    public void clearAll() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        for (HugeUser user : userManager.listAllUsers(-1)) {
            userManager.deleteUser(user.id());
        }
        for (HugeGroup group : userManager.listAllGroups(-1)) {
            userManager.deleteGroup(group.id());
        }
        for (HugeTarget target : userManager.listAllTargets(-1)) {
            userManager.deleteTarget(target.id());
        }

        Assert.assertEquals(0, userManager.listAllAccess(-1).size());
        Assert.assertEquals(0, userManager.listAllBelong(-1).size());
    }

    @Test
    public void testCreateUser() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        Id id = userManager.createUser(makeUser("tom", "pass1"));

        HugeUser user = userManager.getUser(id);
        Assert.assertEquals("tom", user.name());
        Assert.assertEquals("pass1", user.password());
        Assert.assertEquals(user.create(), user.update());
        Assert.assertNull(user.phone());
        Assert.assertNull(user.email());
        Assert.assertNull(user.avatar());

        Map<String, Object> expected = new HashMap<>();
        expected.putAll(ImmutableMap.of("user_name", "tom",
                                        "user_password", "pass1",
                                        "user_creator", "admin"));
        expected.putAll(ImmutableMap.of("user_create", user.create(),
                                        "user_update", user.update(),
                                        "id", user.id()));

        Assert.assertEquals(expected, user.asMap());
    }

    @Test
    public void testCreateUserWithDetailsInfo() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        HugeUser user = new HugeUser("james");
        user.password("pass2");
        user.phone("13812345678");
        user.email("test@baidu.com");
        user.avatar("http://image.baidu.com/image1");
        user.creator("admin");

        Id id = userManager.createUser(user);

        user = userManager.getUser(id);
        Assert.assertEquals("james", user.name());
        Assert.assertEquals("pass2", user.password());
        Assert.assertEquals(user.create(), user.update());
        Assert.assertEquals("13812345678", user.phone());
        Assert.assertEquals("test@baidu.com", user.email());
        Assert.assertEquals("http://image.baidu.com/image1", user.avatar());

        Map<String, Object> expected = new HashMap<>();
        expected.put("user_name", "james");
        expected.put("user_password", "pass2");
        expected.put("user_creator", "admin");
        expected.put("user_create", user.create());
        expected.put("user_update", user.update());
        expected.put("user_phone", user.phone());
        expected.put("user_email", user.email());
        expected.put("user_avatar", user.avatar());
        expected.put("id", user.id());
        Assert.assertEquals(expected, user.asMap());
    }

    @Test
    public void testListUsers() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        Id id1 = userManager.createUser(makeUser("tom", "pass1"));
        Id id2 = userManager.createUser(makeUser("james", "pass2"));

        List<HugeUser> users = userManager.listUsers(ImmutableList.of(id1,
                                                                      id2));
        Assert.assertEquals(2, users.size());
        Assert.assertEquals("tom", users.get(0).name());
        Assert.assertEquals("james", users.get(1).name());

        users = userManager.listUsers(ImmutableList.of(id1, id2, id2));
        Assert.assertEquals(3, users.size());
        Assert.assertEquals("tom", users.get(0).name());
        Assert.assertEquals("james", users.get(1).name());
        Assert.assertEquals("james", users.get(2).name());

        users = userManager.listUsers(ImmutableList.of(id1, id2,
                                                       IdGenerator.of("fake")));
        Assert.assertEquals(2, users.size());
    }

    @Test
    public void testListAllUsers() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        userManager.createUser(makeUser("tom", "pass1"));
        userManager.createUser(makeUser("james", "pass2"));

        List<HugeUser> users = userManager.listAllUsers(-1);
        Assert.assertEquals(2, users.size());
        Assert.assertEquals(ImmutableSet.of("tom", "james"),
                            ImmutableSet.of(users.get(0).name(),
                                            users.get(1).name()));

        Assert.assertEquals(0, userManager.listAllUsers(0).size());
        Assert.assertEquals(1, userManager.listAllUsers(1).size());
        Assert.assertEquals(2, userManager.listAllUsers(2).size());
        Assert.assertEquals(2, userManager.listAllUsers(3).size());
    }

    @Test
    public void testGetUser() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        Id id = userManager.createUser(makeUser("tom", "pass1"));

        HugeUser user = userManager.getUser(id);
        Assert.assertEquals("tom", user.name());
        Assert.assertEquals("pass1", user.password());

        Assert.assertThrows(NotFoundException.class, () -> {
            userManager.getUser(IdGenerator.of("fake"));
        });

        Assert.assertThrows(NotFoundException.class, () -> {
            userManager.getUser(null);
        });
    }

    @Test
    public void testMatchUser() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        String password = StringEncoding.hashPassword("pass1");
        userManager.createUser(makeUser("tom", password));

        Assert.assertNotNull(userManager.matchUser("tom", "pass1"));
        Assert.assertNull(userManager.matchUser("tom", "pass2"));
        Assert.assertNull(userManager.matchUser("Tom", "pass1"));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            userManager.matchUser("Tom", null);
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            userManager.matchUser(null, "pass1");
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            userManager.matchUser(null, null);
        });
    }

    @Test
    public void testUpdateUser() throws InterruptedException {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        Id id = userManager.createUser(makeUser("tom", "pass1"));
        HugeUser user = userManager.getUser(id);
        Assert.assertEquals("tom", user.name());
        Assert.assertEquals("pass1", user.password());
        Assert.assertEquals(user.create(), user.update());

        Date oldUpdateTime = user.update();
        Thread.sleep(1L);

        user.password("pass2");
        userManager.updateUser(user);

        HugeUser user2 = userManager.getUser(id);
        Assert.assertEquals("tom", user2.name());
        Assert.assertEquals("pass2", user2.password());
        Assert.assertEquals(oldUpdateTime, user2.create());
        Assert.assertNotEquals(oldUpdateTime, user2.update());
    }

    @Test
    public void testDeleteUser() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        Id id1 = userManager.createUser(makeUser("tom", "pass1"));
        Id id2 = userManager.createUser(makeUser("james", "pass2"));
        Assert.assertEquals(2, userManager.listAllUsers(-1).size());

        HugeUser user = userManager.deleteUser(id1);
        Assert.assertEquals("tom", user.name());
        Assert.assertEquals(1, userManager.listAllUsers(-1).size());

        user = userManager.deleteUser(id2);
        Assert.assertEquals("james", user.name());
        Assert.assertEquals(0, userManager.listAllUsers(-1).size());
    }

    @Test
    public void testCreateGroup() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        HugeGroup group = makeGroup("group1");
        Id id = userManager.createGroup(group);

        group = userManager.getGroup(id);
        Assert.assertEquals("group1", group.name());
        Assert.assertEquals(null, group.description());
        Assert.assertEquals(group.create(), group.update());

        Assert.assertEquals(ImmutableMap.of("group_name", "group1",
                                            "group_create", group.create(),
                                            "group_update", group.update(),
                                            "group_creator", "admin",
                                            "id", group.id()),
                            group.asMap());

        group = makeGroup("group2");
        group.description("something");
        id = userManager.createGroup(group);

        group = userManager.getGroup(id);
        Assert.assertEquals("group2", group.name());
        Assert.assertEquals("something", group.description());
        Assert.assertEquals(group.create(), group.update());

        HashMap<String, Object> expected = new HashMap<>();
        expected.putAll(ImmutableMap.of("group_name", "group2",
                                        "group_description", "something",
                                        "group_creator", "admin"));
        expected.putAll(ImmutableMap.of("group_create", group.create(),
                                        "group_update", group.update(),
                                        "id", group.id()));

        Assert.assertEquals(expected, group.asMap());
    }

    @Test
    public void testListGroups() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        Id id1 = userManager.createGroup(makeGroup("group1"));
        Id id2 = userManager.createGroup(makeGroup("group2"));

        List<HugeGroup> groups = userManager.listGroups(ImmutableList.of(id1,
                                                                         id2));
        Assert.assertEquals(2, groups.size());
        Assert.assertEquals("group1", groups.get(0).name());
        Assert.assertEquals("group2", groups.get(1).name());

        groups = userManager.listGroups(ImmutableList.of(id1, id2, id2));
        Assert.assertEquals(3, groups.size());
        Assert.assertEquals("group1", groups.get(0).name());
        Assert.assertEquals("group2", groups.get(1).name());
        Assert.assertEquals("group2", groups.get(2).name());

        groups = userManager.listGroups(ImmutableList.of(
                                        id1, id2, IdGenerator.of("fake")));
        Assert.assertEquals(2, groups.size());
    }

    @Test
    public void testListAllGroups() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        userManager.createGroup(makeGroup("group1"));
        userManager.createGroup(makeGroup("group2"));

        List<HugeGroup> groups = userManager.listAllGroups(-1);
        Assert.assertEquals(2, groups.size());
        Assert.assertEquals(ImmutableSet.of("group1", "group2"),
                            ImmutableSet.of(groups.get(0).name(),
                                            groups.get(1).name()));

        Assert.assertEquals(0, userManager.listAllGroups(0).size());
        Assert.assertEquals(1, userManager.listAllGroups(1).size());
        Assert.assertEquals(2, userManager.listAllGroups(2).size());
        Assert.assertEquals(2, userManager.listAllGroups(3).size());
    }

    @Test
    public void testGetGroup() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        Id id = userManager.createGroup(makeGroup("group-test"));
        HugeGroup group = userManager.getGroup(id);
        Assert.assertEquals("group-test", group.name());

        Assert.assertThrows(NotFoundException.class, () -> {
            userManager.getGroup(IdGenerator.of("fake"));
        });

        Assert.assertThrows(NotFoundException.class, () -> {
            userManager.getGroup(null);
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id user = userManager.createUser(makeUser("tom", "pass1"));
            userManager.getGroup(user);
        });
    }

    @Test
    public void testUpdateGroup() throws InterruptedException {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        HugeGroup group = makeGroup("group1");
        group.description("description1");
        Id id = userManager.createGroup(group);

        group = userManager.getGroup(id);
        Assert.assertEquals("group1", group.name());
        Assert.assertEquals("description1", group.description());
        Assert.assertEquals(group.create(), group.update());

        Date oldUpdateTime = group.update();
        Thread.sleep(1L);

        group.description("description2");
        userManager.updateGroup(group);

        HugeGroup group2 = userManager.getGroup(id);
        Assert.assertEquals("group1", group2.name());
        Assert.assertEquals("description2", group2.description());
        Assert.assertEquals(oldUpdateTime, group2.create());
        Assert.assertNotEquals(oldUpdateTime, group2.update());
    }

    @Test
    public void testDeleteGroup() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        Id id1 = userManager.createGroup(makeGroup("group1"));
        Id id2 = userManager.createGroup(makeGroup("group2"));
        Assert.assertEquals(2, userManager.listAllGroups(-1).size());

        HugeGroup group = userManager.deleteGroup(id1);
        Assert.assertEquals("group1", group.name());
        Assert.assertEquals(1, userManager.listAllGroups(-1).size());

        group = userManager.deleteGroup(id2);
        Assert.assertEquals("group2", group.name());
        Assert.assertEquals(0, userManager.listAllGroups(-1).size());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id user = userManager.createUser(makeUser("tom", "pass1"));
            userManager.deleteGroup(user);
        });
    }

    @Test
    public void testCreateTarget() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        HugeTarget target = makeTarget("graph1", "127.0.0.1:8080");
        target.creator("admin");
        Id id = userManager.createTarget(target);

        target = userManager.getTarget(id);
        Assert.assertEquals("graph1", target.name());
        Assert.assertEquals("127.0.0.1:8080", target.url());
        Assert.assertEquals(target.create(), target.update());

        HashMap<String, Object> expected = new HashMap<>();
        expected.putAll(ImmutableMap.of("target_name", "graph1",
                                        "target_graph", "graph1",
                                        "target_url", "127.0.0.1:8080",
                                        "target_creator", "admin"));
        expected.putAll(ImmutableMap.of("target_create", target.create(),
                                        "target_update", target.update(),
                                        "id", target.id()));

        Assert.assertEquals(expected, target.asMap());
    }

    @Test
    public void testCreateTargetWithRess() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        String ress = "[{\"type\": \"VERTEX\", \"label\": \"person\", " +
                      "\"properties\":{\"city\": \"Beijing\"}}, " +
                      "{\"type\": \"EDGE\", \"label\": \"transfer\"}]";
        HugeTarget target = makeTarget("graph1", "127.0.0.1:8080");
        target.resources(ress);
        Id id = userManager.createTarget(target);

        target = userManager.getTarget(id);
        Assert.assertEquals("graph1", target.name());
        Assert.assertEquals("127.0.0.1:8080", target.url());
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
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        Id id1 = userManager.createTarget(makeTarget("target1", "url1"));
        Id id2 = userManager.createTarget(makeTarget("target2", "url2"));

        List<HugeTarget> targets = userManager.listTargets(ImmutableList.of(
                                                           id1, id2));
        Assert.assertEquals(2, targets.size());
        Assert.assertEquals("target1", targets.get(0).name());
        Assert.assertEquals("target2", targets.get(1).name());

        targets = userManager.listTargets(ImmutableList.of(id1, id2, id2));
        Assert.assertEquals(3, targets.size());
        Assert.assertEquals("target1", targets.get(0).name());
        Assert.assertEquals("target2", targets.get(1).name());
        Assert.assertEquals("target2", targets.get(2).name());

        targets = userManager.listTargets(ImmutableList.of(
                                          id1, id2, IdGenerator.of("fake")));
        Assert.assertEquals(2, targets.size());
    }

    @Test
    public void testListAllTargets() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        userManager.createTarget(makeTarget("target1", "url1"));
        userManager.createTarget(makeTarget("target2", "url1"));

        List<HugeTarget> targets = userManager.listAllTargets(-1);
        Assert.assertEquals(2, targets.size());
        Assert.assertEquals(ImmutableSet.of("target1", "target2"),
                            ImmutableSet.of(targets.get(0).name(),
                                            targets.get(1).name()));

        Assert.assertEquals(0, userManager.listAllTargets(0).size());
        Assert.assertEquals(1, userManager.listAllTargets(1).size());
        Assert.assertEquals(2, userManager.listAllTargets(2).size());
        Assert.assertEquals(2, userManager.listAllTargets(3).size());
    }

    @Test
    public void testGetTarget() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        Id id = userManager.createTarget(makeTarget("target-test", "url1"));
        HugeTarget target = userManager.getTarget(id);
        Assert.assertEquals("target-test", target.name());

        Assert.assertThrows(NotFoundException.class, () -> {
            userManager.getTarget(IdGenerator.of("fake"));
        });

        Assert.assertThrows(NotFoundException.class, () -> {
            userManager.getTarget(null);
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id user = userManager.createUser(makeUser("tom", "pass1"));
            userManager.getTarget(user);
        });
    }

    @Test
    public void testUpdateTarget() throws InterruptedException {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        HugeTarget target = makeTarget("target1", "url1");
        Id id = userManager.createTarget(target);

        target = userManager.getTarget(id);
        Assert.assertEquals("target1", target.name());
        Assert.assertEquals("url1", target.url());
        Assert.assertEquals(target.create(), target.update());

        Date oldUpdateTime = target.update();
        Thread.sleep(1L);

        target.url("url2");
        userManager.updateTarget(target);

        HugeTarget target2 = userManager.getTarget(id);
        Assert.assertEquals("target1", target2.name());
        Assert.assertEquals("url2", target2.url());
        Assert.assertEquals(oldUpdateTime, target2.create());
        Assert.assertNotEquals(oldUpdateTime, target2.update());
    }

    @Test
    public void testDeleteTarget() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        Id id1 = userManager.createTarget(makeTarget("target1", "url1"));
        Id id2 = userManager.createTarget(makeTarget("target2", "url2"));
        Assert.assertEquals(2, userManager.listAllTargets(-1).size());

        HugeTarget target = userManager.deleteTarget(id1);
        Assert.assertEquals("target1", target.name());
        Assert.assertEquals(1, userManager.listAllTargets(-1).size());

        target = userManager.deleteTarget(id2);
        Assert.assertEquals("target2", target.name());
        Assert.assertEquals(0, userManager.listAllTargets(-1).size());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id user = userManager.createUser(makeUser("tom", "pass1"));
            userManager.deleteTarget(user);
        });
    }

    @Test
    public void testCreateBelong() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        Id user = userManager.createUser(makeUser("tom", "pass1"));
        Id group1 = userManager.createGroup(makeGroup("group1"));
        Id group2 = userManager.createGroup(makeGroup("group2"));

        Id id1 = userManager.createBelong(makeBelong(user, group1));
        Id id2 = userManager.createBelong(makeBelong(user, group2));

        HugeBelong belong = userManager.getBelong(id1);
        Assert.assertEquals(user, belong.source());
        Assert.assertEquals(group1, belong.target());
        Assert.assertEquals(null, belong.description());
        Assert.assertEquals(belong.create(), belong.update());

        Assert.assertEquals(ImmutableMap.of("user", user,
                                            "group", group1,
                                            "belong_creator", "admin",
                                            "belong_create", belong.create(),
                                            "belong_update", belong.update()),
                            belong.asMap());

        belong = userManager.getBelong(id2);
        Assert.assertEquals(user, belong.source());
        Assert.assertEquals(group2, belong.target());
        Assert.assertEquals(null, belong.description());
        Assert.assertEquals(belong.create(), belong.update());

        Assert.assertEquals(ImmutableMap.of("user", user,
                                            "group", group2,
                                            "belong_creator", "admin",
                                            "belong_create", belong.create(),
                                            "belong_update", belong.update()),
                            belong.asMap());

        List<HugeBelong> belongs = userManager.listBelongByUser(user, -1);
        Assert.assertEquals(2, belongs.size());

        belongs = userManager.listBelongByGroup(group1, -1);
        Assert.assertEquals(1, belongs.size());

        belongs = userManager.listBelongByGroup(group2, -1);
        Assert.assertEquals(1, belongs.size());
    }

    @Test
    public void testListBelong() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        Id user = userManager.createUser(makeUser("tom", "pass1"));
        Id group1 = userManager.createGroup(makeGroup("group1"));
        Id group2 = userManager.createGroup(makeGroup("group2"));

        Id id1 = userManager.createBelong(makeBelong(user, group1));
        Id id2 = userManager.createBelong(makeBelong(user, group2));

        List<HugeBelong> belongs = userManager.listBelong(ImmutableList.of(
                                                          id1, id2));
        Assert.assertEquals(2, belongs.size());
        Assert.assertEquals(user, belongs.get(0).source());
        Assert.assertEquals(user, belongs.get(1).source());
        Assert.assertEquals(group1, belongs.get(0).target());
        Assert.assertEquals(group2, belongs.get(1).target());

        belongs = userManager.listBelong(ImmutableList.of(id1, id2, id2));
        Assert.assertEquals(3, belongs.size());

        belongs = userManager.listBelong(ImmutableList.of(
                                         id1, id2, IdGenerator.of("fake")));
        Assert.assertEquals(2, belongs.size());

        belongs = userManager.listBelongByUser(user, -1);
        Assert.assertEquals(2, belongs.size());
        Assert.assertEquals(user, belongs.get(0).source());
        Assert.assertEquals(user, belongs.get(1).source());

        belongs = userManager.listBelongByGroup(group1, -1);
        Assert.assertEquals(1, belongs.size());
        Assert.assertEquals(user, belongs.get(0).source());
        Assert.assertEquals(group1, belongs.get(0).target());

        belongs = userManager.listBelongByGroup(group2, -1);
        Assert.assertEquals(1, belongs.size());
        Assert.assertEquals(user, belongs.get(0).source());
        Assert.assertEquals(group2, belongs.get(0).target());
    }

    @Test
    public void testListAllBelong() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        Id user = userManager.createUser(makeUser("tom", "pass1"));
        Id group1 = userManager.createGroup(makeGroup("group1"));
        Id group2 = userManager.createGroup(makeGroup("group2"));

        userManager.createBelong(makeBelong(user, group1));
        userManager.createBelong(makeBelong(user, group2));

        List<HugeBelong> belongs = userManager.listAllBelong(-1);
        Assert.assertEquals(2, belongs.size());
        Assert.assertEquals(ImmutableSet.of(group1, group2),
                            ImmutableSet.of(belongs.get(0).target(),
                                            belongs.get(1).target()));

        Assert.assertEquals(0, userManager.listAllBelong(0).size());
        Assert.assertEquals(1, userManager.listAllBelong(1).size());
        Assert.assertEquals(2, userManager.listAllBelong(2).size());
        Assert.assertEquals(2, userManager.listAllBelong(3).size());
    }

    @Test
    public void testGetBelong() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        Id user = userManager.createUser(makeUser("tom", "pass1"));
        Id group1 = userManager.createGroup(makeGroup("group1"));
        Id group2 = userManager.createGroup(makeGroup("group2"));

        Id id1 = userManager.createBelong(makeBelong(user, group1));
        Id id2 = userManager.createBelong(makeBelong(user, group2));

        HugeBelong belong1 = userManager.getBelong(id1);
        Assert.assertEquals(group1, belong1.target());

        HugeBelong belong2 = userManager.getBelong(id2);
        Assert.assertEquals(group2, belong2.target());

        Assert.assertThrows(NotFoundException.class, () -> {
            userManager.getBelong(IdGenerator.of("fake"));
        });

        Assert.assertThrows(NotFoundException.class, () -> {
            userManager.getBelong(null);
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id target = userManager.createTarget(makeTarget("graph1", ""));
            Id access = userManager.createAccess(makeAccess(group1, target,
                                                 HugePermission.READ));
            userManager.getBelong(access);
        });
    }

    @Test
    public void testUpdateBelong() throws InterruptedException {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        Id user = userManager.createUser(makeUser("tom", "pass1"));
        Id group = userManager.createGroup(makeGroup("group1"));

        HugeBelong belong = makeBelong(user, group);
        belong.description("description1");
        Id id = userManager.createBelong(belong);

        belong = userManager.getBelong(id);
        Assert.assertEquals(user, belong.source());
        Assert.assertEquals(group, belong.target());
        Assert.assertEquals("description1", belong.description());
        Assert.assertEquals(belong.create(), belong.update());

        Date oldUpdateTime = belong.update();
        Thread.sleep(1L);

        belong.description("description2");
        userManager.updateBelong(belong);

        HugeBelong belong2 = userManager.getBelong(id);
        Assert.assertEquals(user, belong.source());
        Assert.assertEquals(group, belong.target());
        Assert.assertEquals("description2", belong.description());
        Assert.assertEquals(oldUpdateTime, belong2.create());
        Assert.assertNotEquals(oldUpdateTime, belong2.update());
    }

    @Test
    public void testDeleteBelong() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        Id user = userManager.createUser(makeUser("tom", "pass1"));
        Id group1 = userManager.createGroup(makeGroup("group1"));
        Id group2 = userManager.createGroup(makeGroup("group2"));

        Id id1 = userManager.createBelong(makeBelong(user, group1));
        Id id2 = userManager.createBelong(makeBelong(user, group2));

        Assert.assertEquals(2, userManager.listAllBelong(-1).size());

        HugeBelong belong = userManager.deleteBelong(id1);
        Assert.assertEquals(group1, belong.target());
        Assert.assertEquals(1, userManager.listAllBelong(-1).size());
        Assert.assertEquals(1, userManager.listAllBelong(2).size());

        belong = userManager.deleteBelong(id2);
        Assert.assertEquals(group2, belong.target());
        Assert.assertEquals(0, userManager.listAllBelong(-1).size());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id target = userManager.createTarget(makeTarget("graph1", ""));
            Id access = userManager.createAccess(makeAccess(group1, target,
                                                 HugePermission.READ));
            userManager.deleteBelong(access);
        });
    }

    @Test
    public void testCreateAccess() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        Id group1 = userManager.createGroup(makeGroup("group1"));
        Id group2 = userManager.createGroup(makeGroup("group2"));
        Id target1 = userManager.createTarget(makeTarget("graph1", "url1"));
        Id target2 = userManager.createTarget(makeTarget("graph2", "url2"));

        Id id1 = userManager.createAccess(makeAccess(group1, target1,
                                                     HugePermission.READ));
        Id id2 = userManager.createAccess(makeAccess(group1, target1,
                                                     HugePermission.WRITE));
        Id id3 = userManager.createAccess(makeAccess(group1, target2,
                                                     HugePermission.READ));
        Id id4 = userManager.createAccess(makeAccess(group2, target2,
                                                     HugePermission.READ));

        HugeAccess access = userManager.getAccess(id1);
        Assert.assertEquals(group1, access.source());
        Assert.assertEquals(target1, access.target());
        Assert.assertEquals(HugePermission.READ, access.permission());
        Assert.assertEquals(access.create(), access.update());

        Map<String, Object> expected = new HashMap<>();
        expected.putAll(ImmutableMap.of("group", group1,
                                        "target", target1,
                                        "access_permission",
                                        HugePermission.READ.string(),
                                        "access_creator", "admin"));
        expected.putAll(ImmutableMap.of("access_create", access.create(),
                                        "access_update", access.update()));
        Assert.assertEquals(expected, access.asMap());

        access = userManager.getAccess(id2);
        Assert.assertEquals(group1, access.source());
        Assert.assertEquals(target1, access.target());
        Assert.assertEquals(HugePermission.WRITE, access.permission());
        Assert.assertEquals(access.create(), access.update());

        expected = new HashMap<>();
        expected.putAll(ImmutableMap.of("group", group1,
                                        "target", target1,
                                        "access_permission",
                                        HugePermission.WRITE.string(),
                                        "access_creator", "admin"));
        expected.putAll(ImmutableMap.of("access_create", access.create(),
                                        "access_update", access.update()));
        Assert.assertEquals(expected, access.asMap());

        access = userManager.getAccess(id3);
        Assert.assertEquals(group1, access.source());
        Assert.assertEquals(target2, access.target());
        Assert.assertEquals(HugePermission.READ, access.permission());
        Assert.assertEquals(access.create(), access.update());

        expected = new HashMap<>();
        expected.putAll(ImmutableMap.of("group", group1,
                                        "target", target2,
                                        "access_permission",
                                        HugePermission.READ.string(),
                                        "access_creator", "admin"));
        expected.putAll(ImmutableMap.of("access_create", access.create(),
                                        "access_update", access.update()));
        Assert.assertEquals(expected, access.asMap());

        access = userManager.getAccess(id4);
        Assert.assertEquals(group2, access.source());
        Assert.assertEquals(target2, access.target());
        Assert.assertEquals(HugePermission.READ, access.permission());
        Assert.assertEquals(access.create(), access.update());

        expected = new HashMap<>();
        expected.putAll(ImmutableMap.of("group", group2,
                                        "target", target2,
                                        "access_permission",
                                        HugePermission.READ.string(),
                                        "access_creator", "admin"));
        expected.putAll(ImmutableMap.of("access_create", access.create(),
                                        "access_update", access.update()));
        Assert.assertEquals(expected, access.asMap());

        List<HugeAccess> accesses = userManager.listAccessByGroup(group1, -1);
        Assert.assertEquals(3, accesses.size());

        accesses = userManager.listAccessByGroup(group2, -1);
        Assert.assertEquals(1, accesses.size());

        accesses = userManager.listAccessByTarget(target1, -1);
        Assert.assertEquals(2, accesses.size());

        accesses = userManager.listAccessByTarget(target2, -1);
        Assert.assertEquals(2, accesses.size());
    }

    @Test
    public void testListAccess() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        Id group = userManager.createGroup(makeGroup("group1"));
        Id target1 = userManager.createTarget(makeTarget("graph1", "url1"));
        Id target2 = userManager.createTarget(makeTarget("graph2", "url2"));

        Id id1 = userManager.createAccess(makeAccess(group, target1,
                                                     HugePermission.READ));
        Id id2 = userManager.createAccess(makeAccess(group, target2,
                                                     HugePermission.READ));

        List<HugeAccess> access = userManager.listAccess(ImmutableList.of(
                                                         id1, id2));
        Assert.assertEquals(2, access.size());
        Assert.assertEquals(group, access.get(0).source());
        Assert.assertEquals(group, access.get(1).source());
        Assert.assertEquals(target1, access.get(0).target());
        Assert.assertEquals(target2, access.get(1).target());

        access = userManager.listAccess(ImmutableList.of(id1, id2, id2));
        Assert.assertEquals(3, access.size());

        access = userManager.listAccess(ImmutableList.of(
                                        id1, id2, IdGenerator.of("fake")));
        Assert.assertEquals(2, access.size());

        access = userManager.listAccessByGroup(group, -1);
        Assert.assertEquals(2, access.size());
        Assert.assertEquals(group, access.get(0).source());
        Assert.assertEquals(group, access.get(1).source());

        access = userManager.listAccessByTarget(target1, -1);
        Assert.assertEquals(1, access.size());
        Assert.assertEquals(group, access.get(0).source());
        Assert.assertEquals(target1, access.get(0).target());

        access = userManager.listAccessByTarget(target2, -1);
        Assert.assertEquals(1, access.size());
        Assert.assertEquals(group, access.get(0).source());
        Assert.assertEquals(target2, access.get(0).target());
    }

    @Test
    public void testListAllAccess() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        Id group = userManager.createGroup(makeGroup("group1"));
        Id target1 = userManager.createTarget(makeTarget("graph1", "url1"));
        Id target2 = userManager.createTarget(makeTarget("graph2", "url2"));

        userManager.createAccess(makeAccess(group, target1,
                                            HugePermission.READ));
        userManager.createAccess(makeAccess(group, target2,
                                            HugePermission.READ));

        List<HugeAccess> access = userManager.listAllAccess(-1);
        Assert.assertEquals(2, access.size());
        Assert.assertEquals(ImmutableSet.of(target1, target2),
                            ImmutableSet.of(access.get(0).target(),
                                            access.get(1).target()));

        Assert.assertEquals(0, userManager.listAllAccess(0).size());
        Assert.assertEquals(1, userManager.listAllAccess(1).size());
        Assert.assertEquals(2, userManager.listAllAccess(2).size());
        Assert.assertEquals(2, userManager.listAllAccess(3).size());
    }

    @Test
    public void testGetAccess() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        Id group = userManager.createGroup(makeGroup("group1"));
        Id target1 = userManager.createTarget(makeTarget("graph1", "url1"));
        Id target2 = userManager.createTarget(makeTarget("graph2", "url2"));

        Id id1 = userManager.createAccess(makeAccess(group, target1,
                                                     HugePermission.READ));
        Id id2 = userManager.createAccess(makeAccess(group, target2,
                                                     HugePermission.READ));

        HugeAccess access1 = userManager.getAccess(id1);
        Assert.assertEquals(target1, access1.target());

        HugeAccess access2 = userManager.getAccess(id2);
        Assert.assertEquals(target2, access2.target());

        Assert.assertThrows(NotFoundException.class, () -> {
            userManager.getAccess(IdGenerator.of("fake"));
        });

        Assert.assertThrows(NotFoundException.class, () -> {
            userManager.getAccess(null);
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id user = userManager.createUser(makeUser("tom", "pass1"));
            Id belong = userManager.createBelong(makeBelong(user, group));
            userManager.getAccess(belong);
        });
    }

    @Test
    public void testUpdateAccess() throws InterruptedException {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        Id group = userManager.createGroup(makeGroup("group1"));
        Id target = userManager.createTarget(makeTarget("graph1", "url1"));
        Id id = userManager.createAccess(makeAccess(group, target,
                                                    HugePermission.READ));

        HugeAccess access = userManager.getAccess(id);
        Assert.assertEquals(group, access.source());
        Assert.assertEquals(target, access.target());
        Assert.assertEquals(HugePermission.READ, access.permission());
        Assert.assertEquals(access.create(), access.update());

        Date oldUpdateTime = access.update();
        Thread.sleep(1L);

        access.permission(HugePermission.READ);
        userManager.updateAccess(access);

        HugeAccess access2 = userManager.getAccess(id);
        Assert.assertEquals(group, access.source());
        Assert.assertEquals(target, access.target());
        Assert.assertEquals(HugePermission.READ, access.permission());
        Assert.assertEquals(oldUpdateTime, access2.create());
        Assert.assertNotEquals(oldUpdateTime, access2.update());

        access.permission(HugePermission.WRITE);
        id = userManager.updateAccess(access);

        HugeAccess access3 = userManager.getAccess(id);
        Assert.assertEquals(group, access.source());
        Assert.assertEquals(target, access.target());
        Assert.assertEquals(HugePermission.WRITE, access.permission());
        Assert.assertEquals(oldUpdateTime, access3.create());
        Assert.assertNotEquals(access3.create(), access3.update());

        access2 = userManager.getAccess(access2.id());
        Assert.assertEquals(group, access2.source());
        Assert.assertEquals(target, access2.target());
        Assert.assertEquals(HugePermission.READ, access2.permission());
    }

    @Test
    public void testDeleteAccess() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        Id group = userManager.createGroup(makeGroup("group1"));
        Id target1 = userManager.createTarget(makeTarget("graph1", "url1"));
        Id target2 = userManager.createTarget(makeTarget("graph2", "url2"));

        Id id1 = userManager.createAccess(makeAccess(group, target1,
                                                     HugePermission.READ));
        Id id2 = userManager.createAccess(makeAccess(group, target2,
                                                     HugePermission.READ));

        Assert.assertEquals(2, userManager.listAllAccess(-1).size());

        HugeAccess access = userManager.deleteAccess(id1);
        Assert.assertEquals(target1, access.target());
        Assert.assertEquals(1, userManager.listAllAccess(-1).size());
        Assert.assertEquals(1, userManager.listAllAccess(2).size());

        access = userManager.deleteAccess(id2);
        Assert.assertEquals(target2, access.target());
        Assert.assertEquals(0, userManager.listAllAccess(-1).size());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id user = userManager.createUser(makeUser("tom", "pass1"));
            Id belong = userManager.createBelong(makeBelong(user, group));
            userManager.deleteAccess(belong);
        });
    }

    private static HugeUser makeUser(String name, String password) {
        HugeUser user = new HugeUser(name);
        user.password(password);
        user.creator("admin");
        return user;
    }

    private static HugeGroup makeGroup(String name) {
        HugeGroup group = new HugeGroup(name);
        group.creator("admin");
        return group;
    }

    private static HugeTarget makeTarget(String name, String url) {
        HugeTarget target = new HugeTarget(name, url);
        target.creator("admin");
        return target;
    }

    private static HugeBelong makeBelong(Id user, Id group) {
        HugeBelong belong = new HugeBelong(user, group);
        belong.creator("admin");
        return belong;
    }

    private static HugeAccess makeAccess(Id group, Id target,
                                         HugePermission permission) {
        HugeAccess access = new HugeAccess(group, target, permission);
        access.creator("admin");
        return access;
    }
}
