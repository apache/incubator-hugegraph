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

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class EdgeLabelApiTest extends BaseApiTest {

    private static String path = "/graphs/hugegraph/schema/edgelabels/";

    @Before
    public void prepareSchema() {
        BaseApiTest.initPropertyKey();
        BaseApiTest.initVertexLabel();
    }

    @Test
    public void testCreate() {
        String edgeLabel = "{"
                + "\"name\": \"created\","
                + "\"source_label\": \"person\","
                + "\"target_label\": \"software\","
                + "\"frequency\": \"SINGLE\","
                + "\"properties\":[\"date\", \"city\"],"
                + "\"nullable_keys\":[\"city\"],"
                + "\"sort_keys\":[]"
                + "}";
        Response r = client().post(path, edgeLabel);
        assertResponseStatus(201, r);
    }

    @Test
    public void testAppend() {
        String edgeLabel = "{"
                + "\"name\": \"created\","
                + "\"source_label\": \"person\","
                + "\"target_label\": \"software\","
                + "\"frequency\": \"SINGLE\","
                + "\"properties\":[\"date\", \"city\"],"
                + "\"nullable_keys\":[\"city\"],"
                + "\"sort_keys\":[]"
                + "}";
        Response r = client().post(path, edgeLabel);
        assertResponseStatus(201, r);

        edgeLabel = "{"
                + "\"name\": \"created\","
                + "\"source_label\": null,"
                + "\"target_label\": null,"
                + "\"frequency\": \"DEFAULT\","
                + "\"properties\":[\"lang\"],"
                + "\"nullable_keys\":[\"lang\"],"
                + "\"sort_keys\":[]"
                + "}";
        Map<String, Object> params = ImmutableMap.of("action", "append");
        r = client().put(path + "created", edgeLabel, params);
        assertResponseStatus(200, r);
    }

    @Test
    public void testGet() {
        String edgeLabel = "{"
                + "\"name\": \"created\","
                + "\"source_label\": \"person\","
                + "\"target_label\": \"software\","
                + "\"frequency\": \"SINGLE\","
                + "\"properties\":[\"date\", \"city\"],"
                + "\"nullable_keys\":[\"city\"],"
                + "\"sort_keys\":[]"
                + "}";
        Response r = client().post(path, edgeLabel);
        assertResponseStatus(201, r);

        String name = "created";
        r = client().get(path, name);
        assertResponseStatus(200, r);
    }

    @Test
    public void testList() {
        String edgeLabel = "{"
                + "\"name\": \"created\","
                + "\"source_label\": \"person\","
                + "\"target_label\": \"software\","
                + "\"frequency\": \"SINGLE\","
                + "\"properties\":[\"date\", \"city\"],"
                + "\"nullable_keys\":[\"city\"],"
                + "\"sort_keys\":[]"
                + "}";
        Response r = client().post(path, edgeLabel);
        assertResponseStatus(201, r);

        r = client().get(path);
        assertResponseStatus(200, r);
    }

    @Test
    public void testDelete() {
        String edgeLabel = "{"
                + "\"name\": \"created\","
                + "\"source_label\": \"person\","
                + "\"target_label\": \"software\","
                + "\"frequency\": \"SINGLE\","
                + "\"properties\":[\"date\", \"city\"],"
                + "\"nullable_keys\":[\"city\"],"
                + "\"sort_keys\":[]"
                + "}";
        Response r = client().post(path, edgeLabel);
        assertResponseStatus(201, r);

        String name = "created";
        r = client().delete(path, name);
        assertResponseStatus(204, r);
    }
}
