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

public class TemplatePathsApiTest extends BaseApiTest {

    final static String path = TRAVERSERS_API + "/templatepaths";

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
        String vadasId = name2Ids.get("vadas");
        String joshId = name2Ids.get("josh");
        String peterId = name2Ids.get("peter");
        String rippleId = name2Ids.get("ripple");
        String template = "{" +
                          "\"sources\": {" +
                          " \"ids\": []," +
                          " \"label\": \"person\"," +
                          " \"properties\": {" +
                          "  \"name\": \"vadas\"}}," +
                          "\"targets\": {" +
                          " \"ids\": []," +
                          " \"label\": \"software\"," +
                          " \"properties\": {" +
                          " \"name\": \"ripple\"}}," +
                          "\"steps\": [{" +
                          " \"direction\": \"IN\"," +
                          " \"labels\": [\"knows\"]," +
                          " \"properties\": {}," +
                          " \"max_degree\": 10000," +
                          " \"max_times\": 2," +
                          " \"skip_degree\": 100000},{" +
                          " \"direction\": \"OUT\"," +
                          " \"labels\": [\"created\"]," +
                          " \"properties\": {}," +
                          " \"max_degree\": 10000," +
                          " \"skip_degree\": 100000}]," +
                          " \"capacity\": 10000," +
                          " \"limit\": 10," +
                          " \"with_vertex\": true}";
        Response r = client().post(path, template);
        String content = assertResponseStatus(200, r);
        List<Map<?, ?>> objects = assertJsonContains(content, "paths");
        Assert.assertEquals(1, objects.size());
        List<String> paths = assertMapContains(objects.get(0), "objects");
        Assert.assertEquals(ImmutableList.of(vadasId, joshId,
                                             peterId, rippleId),
                            paths);
    }
}
