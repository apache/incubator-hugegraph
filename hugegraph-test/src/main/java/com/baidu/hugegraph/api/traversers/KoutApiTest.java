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

package com.baidu.hugegraph.api.traversers;

import java.util.List;
import java.util.Map;

import jakarta.ws.rs.core.Response;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.api.BaseApiTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class KoutApiTest extends BaseApiTest {

    static final String PATH = TRAVERSERS_API + "/kout";

    @Before
    public void prepareSchema() {
        BaseApiTest.initPropertyKey();
        BaseApiTest.initVertexLabel();
        BaseApiTest.initEdgeLabel();
        BaseApiTest.initVertex();
        BaseApiTest.initEdge();
    }

    @Test
    public void testGet() {
        Map<String, String> name2Ids = listAllVertexName2Ids();
        String markoId = name2Ids.get("marko");
        String peterId = name2Ids.get("peter");
        String joshId = name2Ids.get("josh");
        String rippleId = name2Ids.get("ripple");
        // Test for nearest=true
        Response r = client().get(PATH, ImmutableMap.of("source",
                                                        id2Json(markoId),
                                                        "max_depth", 2));
        String content = assertResponseStatus(200, r);
        List<String> vertices = assertJsonContains(content, "vertices");
        Assert.assertEquals(1, vertices.size());
        Assert.assertTrue(vertices.contains(joshId));
        // Test for nearest=false
        r = client().get(PATH, ImmutableMap.of("source", id2Json(markoId),
                                               "max_depth", 2,
                                               "nearest", "false"));
        content = assertResponseStatus(200, r);
        vertices = assertJsonContains(content, "vertices");
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.containsAll(ImmutableList.of(peterId,
                                                                rippleId,
                                                                joshId)));
    }

    @Test
    public void testPost() {
        Map<String, String> name2Ids = listAllVertexName2Ids();
        String markoId = name2Ids.get("marko");
        String reqBody = String.format("{ " +
                                       "\"source\": \"%s\", " +
                                       "\"step\": { " +
                                       " \"direction\": \"BOTH\", " +
                                       " \"labels\": [\"knows\", " +
                                       " \"created\"], " +
                                       "\"properties\": { " +
                                       " \"weight\": \"P.gt(0.1)\"}, " +
                                       " \"degree\": 10000, " +
                                       " \"skip_degree\": 100000}, " +
                                       "\"max_depth\": 1, " +
                                       "\"nearest\": true, " +
                                       "\"limit\": 10000, " +
                                       "\"with_vertex\": true, " +
                                       "\"with_path\": true}", markoId);
        Response resp = client().post(PATH, reqBody);
        String content = assertResponseStatus(200, resp);
        Object size = assertJsonContains(content, "size");
        Assert.assertEquals(2, size);
        assertJsonContains(content, "kout");
        assertJsonContains(content, "paths");
        assertJsonContains(content, "vertices");
    }
}
