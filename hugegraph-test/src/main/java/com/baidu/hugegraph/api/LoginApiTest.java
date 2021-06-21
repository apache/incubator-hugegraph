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

import java.nio.file.Paths;
import java.util.Map;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.tinkerpop.shaded.jackson.core.type.TypeReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.util.JsonUtil;

public class LoginApiTest extends BaseApiTest {

    private static final String PATH = "graphs/hugegraph/auth";
    private static final String USER_PATH = "graphs/hugegraph/auth/users";
    private String userId4Test;

    @Before
    public void setup() {
        Response r = this.createUser("test", "test");
        Map<String, Object> user = r.readEntity(
                                     new GenericType<Map<String, Object>>(){});
        this.userId4Test = (String) user.get("id");
    }

    @After
    public void teardown() {
        Response r = this.deleteUser(userId4Test);
    }

    @Test
    public void testLogin() {
        Response r;

        r = this.login("test", "test");
        String result = assertResponseStatus(200, r);
        assertJsonContains(result, "token");

        r = this.login("test", "pass");
        assertResponseStatus(401, r);

        r = this.login("pass", "pass");
        assertResponseStatus(401, r);
    }

    @Test
    public void testLogout() {
        Response r;
        String result;

        r = this.login("test", "test");
        result = assertResponseStatus(200, r);
        assertJsonContains(result, "token");

        String token = this.tokenFromResponse(result);

        String path = Paths.get(PATH, "logout").toString();
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        headers.add(HttpHeaders.AUTHORIZATION, "Bearer " + token);
        r = client().delete(path, headers);
        assertResponseStatus(204, r);

        String invalidToken = "eyJhbGciOiJIUzI1NiJ9.eyJ1caVyX25hbWUiOiJ0ZXN0IiwidXNlcl9pZCI6Ii02Mzp0ZXN0IiwiZXhwIjoxNjI0MzUzMjUyfQ.kYot-3mSGlfSbEMzxrTs84q8YanhTTxtsKPPG25CNxA";
        headers = new MultivaluedHashMap<>();
        headers.add(HttpHeaders.AUTHORIZATION, "Bearer " + token);
        r = client().delete(path, headers);
        assertResponseStatus(204, r);
    }

    @Test
    public void testVerify() {
        Response r;
        String result;

        r = this.login("test", "test");
        result = assertResponseStatus(200, r);
        assertJsonContains(result, "token");

        String token = this.tokenFromResponse(result);

        String path = Paths.get(PATH, "verify").toString();
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        headers.add(HttpHeaders.AUTHORIZATION, "Bearer " + token);
        r = client().get(path, headers);

        result = assertResponseStatus(200, r);
        assertJsonContains(result, "user_id");
        assertJsonContains(result, "user_name");

        Map<String, Object> user = JsonUtil.fromJson(
                            result, new TypeReference<Map<String, Object>>(){});
        Assert.assertEquals(this.userId4Test, user.get("user_id"));
        Assert.assertEquals("test", user.get("user_name"));
    }

    private Response createUser(String name, String password) {
        String user = "{\"user_name\":\"%s\",\"user_password\":\"%s" +
                      "\",\"user_email\":\"user1@baidu.com\"," +
                      "\"user_phone\":\"123456789\",\"user_avatar\":\"image1" +
                      ".jpg\"}";
        return this.client().post(USER_PATH,
                                  String.format(user, name, password));
    }

    private Response deleteUser(String id) {
        return this.client().delete(USER_PATH, id);
    }

    private Response login(String name, String password) {
        String login = Paths.get(PATH, "login").toString();
        String loginUser = "{\"user_name\":\"%s\"," +
                           "\"user_password\":\"%s\"}";

        return client().post(login, String.format(loginUser,
                                                  name, password));
    }

    private String tokenFromResponse(String result) {
        Map<String, Object> loginResult = JsonUtil.fromJson(
                            result, new TypeReference<Map<String, Object>>(){});
        return (String) loginResult.get("token");
    }
}
