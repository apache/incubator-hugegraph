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

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.api.BaseApiTest;
import com.baidu.hugegraph.testutil.Assert;

public class CountApiTest extends BaseApiTest {

    public static String path = TRAVERSERS_API + "/count";

    @Before
    public void prepareSchema() {
        BaseApiTest.initPropertyKey();
        BaseApiTest.initVertexLabel();
        BaseApiTest.initEdgeLabel();
        BaseApiTest.initVertex();
        BaseApiTest.initEdge();
    }

    @Test
    public void testCount() {
        String markoId = listAllVertexName2Ids().get("marko");
        String reqBody = String.format("{ " +
                                       "\"source\": \"%s\", " +
                                       "\"steps\": [{ " +
                                       " \"labels\": []," +
                                       " \"degree\": 100," +
                                       " \"skip_degree\": 100},{ " +
                                       " \"labels\": []," +
                                       " \"degree\": 100," +
                                       " \"skip_degree\": 100},{ " +
                                       " \"labels\": []," +
                                       " \"degree\": 100," +
                                       " \"skip_degree\": 100}]}", markoId);
        Response r = client().post(path, reqBody);
        String content = assertResponseStatus(200, r);
        Integer count = assertJsonContains(content, "count");
        Assert.assertEquals(3, count);
    }
}
