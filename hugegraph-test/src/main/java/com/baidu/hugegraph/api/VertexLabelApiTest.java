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

import java.util.Map;

import javax.ws.rs.core.Response;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class VertexLabelApiTest extends BaseApiTest {

    private static String path = "/graphs/hugegraph/schema/vertexlabels/";

    @Before
    public void prepareSchema() {
        BaseApiTest.initPropertyKey();
    }

    @Test
    public void testCreate() {
        String vertexLabel = "{"
                + "\"name\": \"person\","
                + "\"id_strategy\": \"PRIMARY_KEY\","
                + "\"properties\": [\"name\", \"age\", \"city\"],"
                + "\"primary_keys\":[\"name\"],"
                + "\"nullable_keys\":[\"city\"],"
                + "\"index_names\":[]"
                + "}";
        Response r = client().post(path, vertexLabel);
        Assert.assertEquals(201, r.getStatus());
    }

    @Test
    public void testAppend() {
        String vertexLabel = "{"
                + "\"name\": \"person\","
                + "\"id_strategy\": \"PRIMARY_KEY\","
                + "\"properties\": [\"name\", \"age\", \"city\"],"
                + "\"primary_keys\":[\"name\"],"
                + "\"nullable_keys\":[\"city\"],"
                + "\"index_names\":[]"
                + "}";
        Response r = client().post(path, vertexLabel);
        Assert.assertEquals(201, r.getStatus());

        vertexLabel = "{"
                + "\"name\": \"person\","
                + "\"id_strategy\":\"DEFAULT\","
                + "\"properties\":[\"lang\"],"
                + "\"primary_keys\":[],"
                + "\"nullable_keys\":[\"lang\"],"
                + "\"index_names\":[]"
                + "}";
        Map<String, Object> params = ImmutableMap.of("action", "append");
        r = client().put(path + "person", vertexLabel, params);
        Assert.assertEquals(200, r.getStatus());
    }

    @Test
    public void testGet() {
        String vertexLabel = "{"
                + "\"name\": \"person\","
                + "\"id_strategy\": \"PRIMARY_KEY\","
                + "\"properties\": [\"name\", \"age\", \"city\"],"
                + "\"primary_keys\":[\"name\"],"
                + "\"nullable_keys\":[\"city\"],"
                + "\"index_names\":[]"
                + "}";
        Response r = client().post(path, vertexLabel);
        Assert.assertEquals(201, r.getStatus());

        String name = "person";
        r = client().get(path, name);
        Assert.assertEquals(200, r.getStatus());
    }

    @Test
    public void testList() {
        String vertexLabel = "{"
                + "\"name\": \"person\","
                + "\"id_strategy\": \"PRIMARY_KEY\","
                + "\"properties\": [\"name\", \"age\", \"city\"],"
                + "\"primary_keys\":[\"name\"],"
                + "\"nullable_keys\":[\"city\"],"
                + "\"index_names\":[]"
                + "}";
        Response r = client().post(path, vertexLabel);
        Assert.assertEquals(201, r.getStatus());

        r = client().get(path);
        Assert.assertEquals(200, r.getStatus());
    }

    @Test
    public void testDelete() {
        String vertexLabel = "{"
                + "\"name\": \"person\","
                + "\"id_strategy\": \"PRIMARY_KEY\","
                + "\"properties\": [\"name\", \"age\", \"city\"],"
                + "\"primary_keys\":[\"name\"],"
                + "\"nullable_keys\":[\"city\"],"
                + "\"index_names\":[]"
                + "}";
        Response r = client().post(path, vertexLabel);
        Assert.assertEquals(201, r.getStatus());

        String name = "person";
        r = client().delete(path, name);
        Assert.assertEquals(204, r.getStatus());
    }
}
