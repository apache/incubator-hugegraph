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

public class CustomizedCrosspointsApiTest extends BaseApiTest {

    public static String path = TRAVERSERS_API + "/customizedcrosspoints";

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
        String rippleId = name2Ids.get("ripple");
        String reqBody = String.format("{ " +
                                       "\"sources\":{ " +
                                       " \"ids\":[\"%s\",\"%s\"]}, " +
                                       "\"path_patterns\":[{ " +
                                       " \"steps\":[{ " +
                                       "  \"direction\":\"BOTH\"," +
                                       "  \"labels\":[], " +
                                       "  \"degree\":-1}]}], " +
                                       "\"with_path\":true, " +
                                       "\"with_vertex\":true, " +
                                       "\"capacity\":-1, " +
                                       "\"limit\":-1}", markoId, rippleId);
        Response r = client().post(path, reqBody);
        String content = assertResponseStatus(200, r);
        List<Object> paths = assertJsonContains(content, "paths");
        Assert.assertEquals(2, paths.size());
    }
}
