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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Test;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.auth.HugeUser;
import com.baidu.hugegraph.auth.UserManager;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.testutil.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class UsersTest extends BaseCoreTest {

    @After
    public void clearUsers() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        for (HugeUser user : userManager.listAllUsers(-1)) {
            userManager.deleteUser(user.id());
        }
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

        Assert.assertEquals(ImmutableMap.of("user_name", "tom",
                                            "user_password", "pass1",
                                            "user_create", user.create(),
                                            "user_update", user.update(),
                                            "id", user.id()),
                            user.asMap());
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
        Assert.assertTrue(ImmutableSet.of("tom", "james").contains(
                          users.get(0).name()));
        Assert.assertTrue(ImmutableSet.of("tom", "james").contains(
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

        userManager.createUser(makeUser("tom", "pass1"));

        Assert.assertEquals(true, userManager.matchUser("tom", "pass1"));
        Assert.assertEquals(false, userManager.matchUser("tom", "pass2"));
        Assert.assertEquals(false, userManager.matchUser("Tom", "pass1"));

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
    public void testUpdateUser() {
        HugeGraph graph = graph();
        UserManager userManager = graph.userManager();

        Id id = userManager.createUser(makeUser("tom", "pass1"));
        HugeUser user = userManager.getUser(id);
        Assert.assertEquals("tom", user.name());
        Assert.assertEquals("pass1", user.password());

        user.password("pass2");
        userManager.updateUser(user);

        HugeUser user2 = userManager.getUser(id);
        Assert.assertEquals("tom", user2.name());
        Assert.assertEquals("pass2", user2.password());
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

    private static HugeUser makeUser(String name, String password) {
        HugeUser user = new HugeUser(name);
        user.password(password);
        return user;
    }
}
