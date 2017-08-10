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
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.baidu.hugegraph.api;

import javax.ws.rs.core.Response;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class EdgeApiTest extends BaseApiTest {

    private static String path = "/graphs/hugegraph/graph/edges/";

    @BeforeClass
    public static void setup() {
        // Add some edges (NOTE: vertices have been added before)
        String write2 = "{"
                + "\"label\":\"write\","
                + "\"outV\":\"author:1\","
                + "\"inV\":\"book:java-2\","
                + "\"properties\":{"
                + "\"time\":\"2017-5-18\""
                + "}}";
        String write3 = "{"
                + "\"label\":\"write\","
                + "\"outV\":\"author:1\","
                + "\"inV\":\"book:java-3\","
                + "\"properties\":{"
                + "\"time\":\"2017-5-18\""
                + "}}";

        Response r = newClient().post(path, write2);
        Assert.assertEquals(r.readEntity(String.class), 201, r.getStatus());

        r = newClient().post(path, write3);
        Assert.assertEquals(r.readEntity(String.class), 201, r.getStatus());
    }

    @AfterClass
    public static void teardown() {
        newClient().delete(path, "author:1>write>2017-5-18>book:java-3");
    }

    @Test
    public void testCreate() {
        String edge = "{"
                + "\"label\":\"authored\","
                + "\"outV\":\"author:1\","
                + "\"inV\":\"book:java-1\","
                + "\"properties\":{"
                + "\"contribution\":\"2017-5-18\""
                + "}}";
        System.out.println(client().post(path, edge).readEntity(String.class));
        Assert.assertEquals(201, client().post(path, edge).getStatus());
    }

    @Test
    public void testGet() {
        String edge = "author:1>write>2017-5-18>book:java-2";
        Response r = client().get(path, edge);
        Assert.assertEquals(200, r.getStatus());
    }

    @Test
    public void testGetNotFound() {
        String edge = "author:1>write>2017-5-18>book:!not-exists!";
        Response r = client().get(path, edge);
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
        String edge = "author:1>write>2017-5-18>book:java-3";
        Response r = client().delete(path, edge);
        Assert.assertEquals(204, r.getStatus());
    }
}
