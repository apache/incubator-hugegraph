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

import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;
import com.google.common.collect.ImmutableMap;

import jakarta.ws.rs.core.Response;

public class CypherApiTest extends BaseApiTest {

    private static final String path = "/graphs/hugegraph/cypher";

    @Before
    public void prepareSchema() {
        BaseApiTest.initPropertyKey();
        BaseApiTest.initVertexLabel();
        BaseApiTest.initIndexLabel();
        BaseApiTest.initVertex();
    }

    @Test
    public void testPost() {
        String body = "MATCH (n:person) where n.city ='Beijing' return n";
        assertResponseStatus(200, client().post(path, body));
    }

    @Test
    public void testGet() {
        Map<String, Object> params = ImmutableMap.of("cypher",
                                                     "MATCH (n:person) where n.city ='Beijing' return n");
        Response r = client().get(path, params);
        Assert.assertEquals(r.readEntity(String.class), 200, r.getStatus());
    }
}
