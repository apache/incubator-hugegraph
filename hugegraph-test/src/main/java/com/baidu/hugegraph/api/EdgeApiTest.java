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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class EdgeApiTest extends BaseApiTest {

    private static String path = "/graphs/hugegraph/graph/edges/";

    @Before
    public void prepareSchema() {
        super.initPropertyKey();
        super.initVertexLabel();
        super.initEdgeLabel();
        super.initVertex();
    }

    @Test
    public void testCreate() {
        String edge = "{"
                + "\"label\": \"created\","
                + "\"outVLabel\": \"person\","
                + "\"inVLabel\": \"software\","
                + "\"outV\": \"person:peter\","
                + "\"inV\": \"software:lop\","
                + "\"properties\":{"
                + "\"date\": \"20170324\","
                + "\"city\": \"Hongkong\"}"
                + "}";
        Response r = client().post(path, edge);
        Assert.assertEquals(201, r.getStatus());
    }

    @Test
    public void testGet() {
        String edge = "{"
                + "\"label\": \"created\","
                + "\"outVLabel\": \"person\","
                + "\"inVLabel\": \"software\","
                + "\"outV\": \"person:peter\","
                + "\"inV\": \"software:lop\","
                + "\"properties\":{"
                + "\"date\": \"20170324\","
                + "\"city\": \"Hongkong\"}"
                + "}";
        Response r = client().post(path, edge);
        Assert.assertEquals(201, r.getStatus());

        String id = "person:peter>created>>software:lop";
        r = client().get(path, id);
        Assert.assertEquals(200, r.getStatus());
    }

    @Test
    public void testList() {
        String edge = "{"
                + "\"label\": \"created\","
                + "\"outVLabel\": \"person\","
                + "\"inVLabel\": \"software\","
                + "\"outV\": \"person:peter\","
                + "\"inV\": \"software:lop\","
                + "\"properties\":{"
                + "\"date\": \"20170324\","
                + "\"city\": \"Hongkong\"}"
                + "}";
        Response r = client().post(path, edge);
        Assert.assertEquals(201, r.getStatus());

        r = client().get(path);
        Assert.assertEquals(200, r.getStatus());
    }

    @Test
    public void testDelete() {
        String edge = "{"
                + "\"label\": \"created\","
                + "\"outVLabel\": \"person\","
                + "\"inVLabel\": \"software\","
                + "\"outV\": \"person:peter\","
                + "\"inV\": \"software:lop\","
                + "\"properties\":{"
                + "\"date\": \"20170324\","
                + "\"city\": \"Hongkong\"}"
                + "}";
        Response r = client().post(path, edge);
        Assert.assertEquals(201, r.getStatus());

        String id = "person:peter>created>>software:lop";
        r = client().delete(path, id);
        Assert.assertEquals(204, r.getStatus());
    }
}
