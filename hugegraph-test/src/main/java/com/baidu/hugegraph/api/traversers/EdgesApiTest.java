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

import org.junit.Assert;

import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.api.BaseApiTest;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;


public class EdgesApiTest extends BaseApiTest {

    final static String EDGE_PATH = "graphs/hugegraph/traversers/edges";
    final static String SHARES_PATH = "graphs/hugegraph/traversers" +
                                      "/edges/shards";
    final static String SCAN_PATH = "graphs/hugegraph/traversers/edges/scan";

    @Before
    public void prepareSchema() {
        BaseApiTest.initPropertyKey();
        BaseApiTest.initVertexLabel();
        BaseApiTest.initEdgeLabel();
        BaseApiTest.initVertex();
        BaseApiTest.initEdge();
    }

    @Test
    public void testList() {
        Map<String, String> name2Ids = listAllVertexName2Ids();
        final String edgeGetPath = "graphs/hugegraph/graph/edges";
        String vadasId = name2Ids.get("vadas");
        Map params =  ImmutableMap.of("vertex_id",
                                      id2Json(vadasId),
                                      "direction",
                                      "IN");
        Response r = client().get(edgeGetPath, params);
        String respBody = assertResponseStatus(200, r);
        List<Map> edges = assertJsonContains(respBody, "edges");
        Assert.assertNotNull(edges);
        Assert.assertFalse(edges.isEmpty());
        String edgeId = assertMapContains(edges.get(0), "id");
        Assert.assertNotNull(edgeId);

        r = client().get(EDGE_PATH,
                         ImmutableMultimap.of("ids", edgeId));
        assertResponseStatus(200, r);
    }

    @Test
    public void testShareAndScan() {
        Response r = client().get(SHARES_PATH, ImmutableMap.of("split_size",
                                                               1048576));
        String respBody = assertResponseStatus(200, r);
        List<Map> shards = assertJsonContains(respBody, "shards");
        Assert.assertNotNull(shards);
        Assert.assertFalse(shards.isEmpty());
        String start = assertMapContains(shards.get(0), "start");
        String end = assertMapContains(shards.get(0), "end");
        r = client().get(SCAN_PATH, ImmutableMap.of("start", start,
                                                    "end", end));
        respBody = assertResponseStatus(200, r);
        Map<String, Object> entity2 = parseMap(respBody);
        Assert.assertNotNull(entity2);
        Assert.assertFalse(entity2.isEmpty());
    }
}
