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

import java.io.IOException;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Test;

public class EdgeApiTest extends BaseApiTest {

    private static String path = "/graphs/hugegraph/graph/edges/";

    @Before
    public void prepareSchema() {
        BaseApiTest.initPropertyKey();
        BaseApiTest.initVertexLabel();
        BaseApiTest.initEdgeLabel();
        BaseApiTest.initVertex();
    }

    @Test
    public void testCreate() throws IOException {
        String outVId = getVertexId("person", "name", "peter");
        String inVId = getVertexId("software", "name", "lop");

        String edge = String.format("{"
                + "\"label\": \"created\","
                + "\"outVLabel\": \"person\","
                + "\"inVLabel\": \"software\","
                + "\"outV\": \"%s\","
                + "\"inV\": \"%s\","
                + "\"properties\":{"
                + "\"date\": \"20170324\","
                + "\"weight\": 0.5}"
                + "}", outVId, inVId);
        Response r = client().post(path, edge);
        assertResponseStatus(201, r);
    }

    @Test
    public void testGet() throws IOException {
        String outVId = getVertexId("person", "name", "peter");
        String inVId = getVertexId("software", "name", "lop");

        String edge = String.format("{"
                + "\"label\": \"created\","
                + "\"outVLabel\": \"person\","
                + "\"inVLabel\": \"software\","
                + "\"outV\": \"%s\","
                + "\"inV\": \"%s\","
                + "\"properties\":{"
                + "\"date\": \"20170324\","
                + "\"weight\": 0.5}"
                + "}", outVId, inVId);
        Response r = client().post(path, edge);
        String content = assertResponseStatus(201, r);

        String id = parseId(content);
        r = client().get(path, id);
        assertResponseStatus(200, r);
    }

    @Test
    public void testList() throws IOException {
        String outVId = getVertexId("person", "name", "peter");
        String inVId = getVertexId("software", "name", "lop");

        String edge = String.format("{"
                + "\"label\": \"created\","
                + "\"outVLabel\": \"person\","
                + "\"inVLabel\": \"software\","
                + "\"outV\": \"%s\","
                + "\"inV\": \"%s\","
                + "\"properties\":{"
                + "\"date\": \"20170324\","
                + "\"weight\": 0.5}"
                + "}", outVId, inVId);
        Response r = client().post(path, edge);
        assertResponseStatus(201, r);

        r = client().get(path);
        assertResponseStatus(200, r);
    }

    @Test
    public void testDelete() throws IOException {
        String outVId = getVertexId("person", "name", "peter");
        String inVId = getVertexId("software", "name", "lop");

        String edge = String.format("{"
                + "\"label\": \"created\","
                + "\"outVLabel\": \"person\","
                + "\"inVLabel\": \"software\","
                + "\"outV\": \"%s\","
                + "\"inV\": \"%s\","
                + "\"properties\":{"
                + "\"date\": \"20170324\","
                + "\"weight\": 0.5}"
                + "}", outVId, inVId);
        Response r = client().post(path, edge);
        String content = assertResponseStatus(201, r);

        String id = parseId(content);
        r = client().delete(path, id);
        assertResponseStatus(204, r);
    }
}
