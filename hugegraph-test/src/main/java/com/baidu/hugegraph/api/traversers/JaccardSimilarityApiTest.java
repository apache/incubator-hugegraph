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

import java.util.Map;

import jakarta.ws.rs.core.Response;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.api.BaseApiTest;
import com.google.common.collect.ImmutableMap;

public class JaccardSimilarityApiTest extends BaseApiTest {

    final static String path = TRAVERSERS_API + "/jaccardsimilarity";

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
        Response r = client().get(path, ImmutableMap.of("vertex",
                                                        id2Json(markoId),
                                                        "other",
                                                        id2Json(peterId)));
        String content = assertResponseStatus(200, r);
        Double jaccardSimilarity = assertJsonContains(content,
                                                      "jaccard_similarity");
        Assert.assertEquals(0.25, jaccardSimilarity.doubleValue(), 0.0001);
    }

    @Test
    public void testPost() {
        Map<String, String> name2Ids = listAllVertexName2Ids();
        String markoId = name2Ids.get("marko");
        String rippleId = name2Ids.get("ripple");
        String peterId = name2Ids.get("peter");
        String jsonId = name2Ids.get("josh");
        String reqBody = String.format("{ " +
                                       "\"vertex\": \"%s\", " +
                                       "\"step\": { " +
                                       " \"direction\": \"BOTH\", " +
                                       " \"labels\": [], " +
                                       " \"degree\": 10000, " +
                                       " \"skip_degree\": 100000 }, " +
                                       "\"top\": 3}", markoId);
        Response r = client().post(path, reqBody);
        String content = assertResponseStatus(200, r);
        Double rippleJaccardSimilarity = assertJsonContains(content, rippleId);
        Double peterJaccardSimilarity = assertJsonContains(content, peterId);
        Double jsonJaccardSimilarity = assertJsonContains(content, jsonId);
        Assert.assertEquals(0.3333, rippleJaccardSimilarity.doubleValue(),
                            0.0001);
        Assert.assertEquals(0.25, peterJaccardSimilarity.doubleValue(), 0.0001);
        Assert.assertEquals(0.3333, jsonJaccardSimilarity.doubleValue(),
                            0.0001);
    }
}
