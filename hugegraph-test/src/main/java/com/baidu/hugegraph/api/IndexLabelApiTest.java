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

public class IndexLabelApiTest extends BaseApiTest {

    private static String path = "/graphs/hugegraph/schema/indexlabels/";

    @Before
    public void prepareSchema() {
        BaseApiTest.initPropertyKey();
        BaseApiTest.initVertexLabel();
        BaseApiTest.initEdgeLabel();
    }

    @Test
    public void testCreate() {
        String indexLabel = "{"
                + "\"name\": \"personByAge\","
                + "\"base_type\": \"VERTEX_LABEL\","
                + "\"base_value\": \"person\","
                + "\"index_type\": \"RANGE\","
                + "\"fields\":[\"age\"]"
                + "}";
        Response r = client().post(path, indexLabel);
        assertResponseStatus(201, r);
    }

    @Test
    public void testGet() {
        String indexLabel = "{"
                + "\"name\": \"personByAge\","
                + "\"base_type\": \"VERTEX_LABEL\","
                + "\"base_value\": \"person\","
                + "\"index_type\": \"RANGE\","
                + "\"fields\":[\"age\"]"
                + "}";
        Response r = client().post(path, indexLabel);
        assertResponseStatus(201, r);

        String name = "personByAge";
        r = client().get(path, name);
        assertResponseStatus(200, r);
    }

    @Test
    public void testList() {
        String indexLabel = "{"
                + "\"name\": \"personByAge\","
                + "\"base_type\": \"VERTEX_LABEL\","
                + "\"base_value\": \"person\","
                + "\"index_type\": \"RANGE\","
                + "\"fields\":[\"age\"]"
                + "}";
        Response r = client().post(path, indexLabel);
        assertResponseStatus(201, r);

        r = client().get(path);
        assertResponseStatus(200, r);
    }

    @Test
    public void testDelete() {
        String indexLabel = "{"
                + "\"name\": \"personByAge\","
                + "\"base_type\": \"VERTEX_LABEL\","
                + "\"base_value\": \"person\","
                + "\"index_type\": \"RANGE\","
                + "\"fields\":[\"age\"]"
                + "}";
        Response r = client().post(path, indexLabel);
        assertResponseStatus(201, r);

        String name = "personByAge";
        r = client().delete(path, name);
        assertResponseStatus(204, r);
    }
}
