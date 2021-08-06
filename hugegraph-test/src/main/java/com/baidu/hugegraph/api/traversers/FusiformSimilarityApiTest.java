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

public class FusiformSimilarityApiTest extends BaseApiTest {

    final static String path = TRAVERSERS_API + "/fusiformsimilarity";

    @Before
    public void prepareSchema() {
        BaseApiTest.initPropertyKey();
        BaseApiTest.initVertexLabel();
        BaseApiTest.initEdgeLabel();
        BaseApiTest.initVertex();
        BaseApiTest.initEdge();
    }

    @Test
    public void testPost() {
        Response r = client().post(path, "{ " +
                                         "\"sources\":{ " +
                                         " \"ids\":[], " +
                                         " \"label\": \"person\", " +
                                         " \"properties\": {}}, " +
                                         "\"label\":\"created\", " +
                                         "\"direction\":\"OUT\", " +
                                         "\"min_neighbors\":1, " +
                                         "\"alpha\":1, " +
                                         "\"min_similars\":1, " +
                                         "\"top\":0, " +
                                         "\"group_property\":\"city\", " +
                                         "\"min_groups\":2, " +
                                         "\"max_degree\": 10000, " +
                                         "\"capacity\": -1, " +
                                         "\"limit\": -1, " +
                                         "\"with_intermediary\": false, " +
                                         "\"with_vertex\":true}");
        String content = assertResponseStatus(200, r);
        Map<String, List> similars = assertJsonContains(content, "similars");
        Map<String, String> name2Ids = listAllVertexName2Ids();
        String markoId = name2Ids.get("marko");
        String peterId = name2Ids.get("peter");
        Assert.assertEquals(2, similars.size());
        assertMapContains(similars, markoId);
        assertMapContains(similars, peterId);
    }
}
