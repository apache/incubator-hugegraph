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

import com.baidu.hugegraph.api.BaseApiTest;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

public class CustomizedPathsApiTest extends BaseApiTest {

    final static String path = TRAVERSERS_API + "/customizedpaths";

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
        Map<String, String> name2Ids = listAllVertexName2Ids();
        String markoId = name2Ids.get("marko");
        String joshId = name2Ids.get("josh");
        String reqBody = String.format("{ " +
                                       "\"sources\": { " +
                                       " \"ids\": [\"%s\"]}, " +
                                       "\"steps\": [{ " +
                                       " \"direction\": \"BOTH\", " +
                                       " \"labels\": [\"knows\"], " +
                                       " \"weight_by\":\"weight\", " +
                                       " \"max_degree\":-1}, {" +
                                       " \"direction\":\"OUT\", " +
                                       " \"labels\": [\"created\"], " +
                                       " \"default_weight\":8, "+
                                       " \"max_degree\":-1, " +
                                       " \"sample\":1 }], " +
                                       "\"sort_by\":\"INCR\", " +
                                       "\"with_vertex\":true}",
                                       markoId);
        Response r = client().post(path, reqBody);
        String content = assertResponseStatus(200, r);
        List<Map<String, Object>> paths = assertJsonContains(content, "paths");
        Assert.assertEquals(1, paths.size());
        List<Map<String, Object>> vertices = assertJsonContains(content, "vertices");
        Assert.assertEquals(3, vertices.size());
    }
}

