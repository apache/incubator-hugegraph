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

import javax.ws.rs.core.Response;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.api.BaseApiTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class KoutApiTest extends BaseApiTest {

    final static String path = TRAVERSERS_API + "/kout";
    final static String postParams = "{ " +
                                     "\"source\": \"%s\", " +
                                     "\"steps\": { " +
                                     " \"direction\": \"BOTH\", " +
                                     "\"edge_steps\": [" +
                                     "{\"label\":\"knows\"," +
                                     "\"properties\": {" +
                                     "\"weight\": \"P.gt(0.1)\"}}," +
                                     "{\"label\":\"created\"," +
                                     "\"properties\": {" +
                                     "\"weight\": \"P.gt(0.1)\"}}]," +
                                     " \"max_degree\": 10000, " +
                                     " \"skip_degree\": 100000}, " +
                                     "\"max_depth\": 1, " +
                                     "\"count_only\": false, " +
                                     "\"nearest\": true, " +
                                     "\"limit\": 10000, " +
                                     "\"with_vertex\": true, " +
                                     "\"with_path\": true, " +
                                     "\"with_edge\": true, " +
                                     "\"algorithm\": \"%s\" }";
    final static String postParamsForCountOnly = "{ " +
            "\"source\": \"%s\", " +
            "\"steps\": { " +
            " \"direction\": \"%s\", " +
            " \"max_degree\": 10000, " +
            " \"skip_degree\": 100000}, " +
            "\"max_depth\": 2, " +
            "\"count_only\": true, " +
            "\"nearest\": true, " +
            "\"limit\": 10000}";

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
        Response r = client().get(path, ImmutableMap.of("source",
                                                        id2Json(markoId),
                                                        "max_depth", 2));
        String content = assertResponseStatus(200, r);
        List<String> vertices = assertJsonContains(content, "vertices");
        Assert.assertEquals(1, vertices.size());
        Assert.assertTrue(vertices.contains(joshId));
        // Test for nearest=false
        r = client().get(path, ImmutableMap.of("source", id2Json(markoId),
                                               "max_depth", 2,
                                               "nearest", "false"));
        content = assertResponseStatus(200, r);
        vertices = assertJsonContains(content, "vertices");
        assertJsonContains(content, "measure");
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.containsAll(ImmutableList.of(peterId,
                                                                rippleId,
                                                                joshId)));
        // Test for algorithm
        r = client().get(path, ImmutableMap.of("source", id2Json(markoId),
                                               "max_depth", 2,
                                               "nearest", "false",
                                               "algorithm", "deep_first"));
        content = assertResponseStatus(200, r);
        vertices = assertJsonContains(content, "vertices");
        assertJsonContains(content, "measure");
        Assert.assertEquals(3, vertices.size());
        Assert.assertTrue(vertices.containsAll(ImmutableList.of(peterId,
                                                                rippleId,
                                                                joshId)));
    }

    @Test
    public void testPost() {
        Map<String, String> name2Ids = listAllVertexName2Ids();
        String markoId = name2Ids.get("marko");
        String reqBody = String.format(postParams, markoId, "breadth_first");
        Response resp = client().post(path, reqBody);
        String content = assertResponseStatus(200, resp);
        Object size = assertJsonContains(content, "size");
        Assert.assertEquals(2, size);
        assertJsonContains(content, "kout");
        assertJsonContains(content, "paths");
        assertJsonContains(content, "vertices");
        assertJsonContains(content, "edges");
        assertJsonContains(content, "measure");

        // for deep-first
        reqBody = String.format(postParams, markoId, "deep_first");
        resp = client().post(path, reqBody);
        content = assertResponseStatus(200, resp);
        size = assertJsonContains(content, "size");
        Assert.assertEquals(2, size);
        assertJsonContains(content, "kout");
        assertJsonContains(content, "paths");
        assertJsonContains(content, "vertices");
        assertJsonContains(content, "edges");
        assertJsonContains(content, "measure");

        // for count-only
        reqBody = String.format(postParamsForCountOnly, markoId, "BOTH");
        resp = client().post(path, reqBody);
        content = assertResponseStatus(200, resp);
        size = assertJsonContains(content, "size");
        Assert.assertEquals(1, size);
        assertJsonContains(content, "kout");
        assertJsonContains(content, "paths");
        assertJsonContains(content, "vertices");
        assertJsonContains(content, "edges");
        assertJsonContains(content, "measure");
    }
}
