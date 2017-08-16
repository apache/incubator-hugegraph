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

import javax.ws.rs.core.Response;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class VertexApiTest extends BaseApiTest {

    private static String path = "/graphs/hugegraph/graph/vertices/";

    @BeforeClass
    public static void setup() {
        // add some vertices
        String lisa = "{"
                + "\"label\":\"person\","
                + "\"properties\":{"
                + "\"name\":\"Lisa\","
                + "\"city\":\"Beijing\","
                + "\"age\":20"
                + "}}";
        String hebe = "{"
                + "\"label\":\"person\","
                + "\"properties\":{"
                + "\"name\":\"Hebe\","
                + "\"city\":\"Taipei\","
                + "\"age\":21"
                + "}}";

        Response r = newClient().post(path, lisa);
        Assert.assertEquals(r.readEntity(String.class), 201, r.getStatus());

        r = newClient().post(path, hebe);
        Assert.assertEquals(r.readEntity(String.class), 201, r.getStatus());
    }

    @AfterClass
    public static void teardown() {
        newClient().delete(path, "person:Lisa");
        newClient().delete(path, "person:Hebe");
    }

    @Test
    public void testCreate() {
        String vertex = "{\"label\":\"person\","
                + "\"properties\":{"
                + "\"name\":\"James\","
                + "\"city\":\"Beijing\","
                + "\"age\":19}}";
        Assert.assertEquals(201, client().post(path, vertex).getStatus());
    }

    @Test
    public void testGet() {
        String vertex = "person:Lisa";
        Response r = client().get(path, vertex);
        Assert.assertEquals(200, r.getStatus());
    }

    @Test
    public void testGetNotFound() {
        String vertex = "person:!not-exists-this-vertex!";
        Response r = client().get(path, vertex);
        // TODO: improve to 404 (currently server returns 400 if not found)
        Assert.assertEquals(400, r.getStatus());
    }

    @Test
    public void testList() {
        Response r = client().get(path);
        System.out.println("testList(): " + r.readEntity(String.class));
        Assert.assertEquals(200, r.getStatus());
    }

    @Test
    public void testDelete() {
        String vertex = "person:Lisa";
        Response r = client().delete(path, vertex);
        Assert.assertEquals(204, r.getStatus());
    }
}
