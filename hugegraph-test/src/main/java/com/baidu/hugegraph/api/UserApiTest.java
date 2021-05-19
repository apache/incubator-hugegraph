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

package com.baidu.hugegraph.api;

import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.baidu.hugegraph.util.JsonUtil;
import com.google.common.collect.ImmutableMap;

public class UserApiTest extends BaseApiTest {

    private static final String path = "graphs/hugegraph/auth/users";
    private static final int NO_LIMIT = -1;

    @After
    public void teardown() throws Exception {
        super.teardown();
        Response r = this.client().get(path,
                                       ImmutableMap.of("limit", NO_LIMIT));
        String result = r.readEntity(String.class);
        Map<String, List<Map<String, Object>>> resultMap =
                JsonUtil.fromJson(result,
                                  new TypeReference<Map<String,
                                  List<Map<String, Object>>>>() {});
        List<Map<String, Object>> users = resultMap.get("users");
        for (Map<String, Object> user : users) {
            if (user.get("user_name").equals("admin")) {
                continue;
            }
            this.client().delete(path, (String) user.get("id"));
        }
    }

    @Test
    public void testCreate() {
        String user1 = "{\"user_name\":\"user1\",\"user_password\":\"p1\"," +
                       "\"user_email\":\"user1@baidu.com\",\"user_phone\":" +
                       "\"123456789\",\"user_avatar\":\"image1.jpg\"}";

        String user2 = "{\"user_name\":\"user2\",\"user_password\":\"p2\"," +
                       "\"user_email\":\"user2@baidu.com\"," +
                       "\"user_phone\":\"1357924680\"," +
                       "\"user_avatar\":\"image2.jpg\"}";

        Response r = client().post(path, user1);
        String result = assertResponseStatus(201, r);
        Response r2 = client().post(path, user2);
        String result2 = assertResponseStatus(201, r2);

        assertJsonContains(result, "user_name");
        assertJsonContains(result, "user_password");
        assertJsonContains(result, "user_email");
        assertJsonContains(result, "user_phone");
        assertJsonContains(result, "user_avatar");

        assertJsonContains(result2, "user_name");
        assertJsonContains(result2, "user_password");
        assertJsonContains(result2, "user_email");
        assertJsonContains(result2, "user_phone");
        assertJsonContains(result2, "user_avatar");

        Response r3 = client().post(path, "{}");
        assertResponseStatus(400, r3);

        String user3 = "{\"user_name\":\"user1\",\"user_password\":\"p1\"," +
                       "\"user_email\":\"user1@baidu.com\"," +
                       "\"user_phone\":\"123456789\",\"user_avatar\":\"image1" +
                       ".jpg\"}";
        Response r4 = client().post(path, user3);
        String result4 = assertResponseStatus(400, r4);
        String message = assertJsonContains(result4, "message");
        Assert.assertThat(message,
                          CoreMatchers.containsString("that already exists"));
    }

    @Test
    public void testList() {
        createUser("test1");
        createUser("test2");
        createUser("test3");
        List<Map<String, Object>> users = listUsers();
        Assert.assertEquals(4, users.size());
    }

    @Test
    public void testGetUser() {
        createUser("test1");
        createUser("test2");
        List<Map<String, Object>> users = listUsers();
        for (Map<String, Object> user : users) {
            Response r = client().get(path, (String) user.get("id"));
            String result = assertResponseStatus(200, r);
            assertJsonContains(result, "user_name");
        }
    }

    @Test
    public void testUpdate() {
        createUser("test1");
        createUser("test2");
        List<Map<String, Object>> users = listUsers();
        for (Map<String, Object> user : users) {
            if (user.get("user_name").equals("admin")) {
                continue;
            }
            String user1 = "{\"user_password\":\"p1\"," +
                           "\"user_email\":\"user1@baidu.com\"," +
                           "\"user_phone\":\"111111\"," +
                           "\"user_avatar\":\"image1" +
                           ".jpg\"}";
            Response r = client().put(path, (String) user.get("id"), user1,
                                      ImmutableMap.of());
            assertResponseStatus(200, r);
        }
    }

    @Test
    public void testDelete() {
        createUser("test1");
        createUser("test2");
        createUser("test3");

        List<Map<String, Object>> users = listUsers();
        for (Map<String, Object> user : users) {
            if (user.get("user_name").equals("admin")) {
                continue;
            }
            Response r = client().delete(path, (String) user.get("id"));
        }
        Response r = client().delete(path, "test1");
        String result = assertResponseStatus(400, r);
        String message = assertJsonContains(result, "message");
        Assert.assertThat(message,
                          CoreMatchers.containsString("Invalid user id:"));
    }

    protected void createUser(String name) {
        String user = "{\"user_name\":\"" + name + "\",\"user_password\":\"p1" +
                      "\", \"user_email\":\"user1@baidu.com\", " +
                      "\"user_phone\"123456789\", \"user_avatar\":\"image1" +
                      ".jpg\"}";
        this.client().post(path, user);
    }

    protected List<Map<String, Object>> listUsers() {
        Response r = this.client().get(path, ImmutableMap.of("limit",
                                                             NO_LIMIT));
        String result = assertResponseStatus(200, r);
        Map<String, List<Map<String, Object>>> resultMap =
                JsonUtil.fromJson(result, new TypeReference<Map<String,
                                  List<Map<String, Object>>>>() {});
        return resultMap.get("users");
    }
}
