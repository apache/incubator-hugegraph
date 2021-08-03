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

import java.util.Map;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.api.BaseApiTest;

public class FusiformSimilarityApiTest extends BaseApiTest {

    final static String path = "graphs/hugegraph/traversers/fusiformsimilarity";

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
        Response r = client().post(path, "{ "
                                         + "\"sources\":{ "
                                         + "  \"ids\":[], "
                                         + "  \"label\": \"person\", "
                                         + "  \"properties\": {}}, "
                                         + "\"label\":\"created\", "
                                         + "\"direction\":\"OUT\", "
                                         + "\"min_neighbors\":1, "
                                         + "\"alpha\":1, "
                                         + "\"min_similars\":1, "
                                         + "\"top\":0, "
                                         + "\"group_property\":\"city\", "
                                         + "\"min_groups\":2, "
                                         + "\"max_degree\": 10000, "
                                         + "\"capacity\": -1, "
                                         + "\"limit\": -1, "
                                         + "\"with_intermediary\": false, "
                                         + "\"with_vertex\":true}");
        String respBody = assertResponseStatus(200, r);
        Map<String, Object> entity = parseMap(respBody);
        Assert.assertNotNull(entity);
    }
}
