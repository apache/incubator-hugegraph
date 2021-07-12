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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.api.BaseApiTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class AllShortestPathsApiTest extends BaseApiTest {

    public static String path = "graphs/hugegraph/traversers/allshortestpaths";

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
        String vadasId = name2Ids.get("vadas");
        String peterId = name2Ids.get("peter");
        String joshId = name2Ids.get("josh");
        Map<String, Object> entities = ImmutableMap.of("source",
                                                       id2Json(markoId),
                                                       "target",
                                                       id2Json(vadasId),
                                                       "max_depth", 100);
        String respJson = assertResponseStatus(200,
                                               client().get(path, entities));
        List paths = assertJsonContains(respJson, "paths");
        Assert.assertEquals(1, paths.size());
        List objects = assertMapContains((Map<?, ?>) paths.get(0), "objects");
        Assert.assertEquals(ImmutableList.of(markoId, peterId, joshId,
                                             vadasId), objects);
    }
}
