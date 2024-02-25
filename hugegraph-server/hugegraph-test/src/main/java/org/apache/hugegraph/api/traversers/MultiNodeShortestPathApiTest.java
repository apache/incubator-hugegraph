/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.api.traversers;

import java.util.List;
import java.util.Map;

import org.apache.hugegraph.api.BaseApiTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import jakarta.ws.rs.core.Response;

public class MultiNodeShortestPathApiTest extends BaseApiTest {

    final String path = TRAVERSERS_API + "/multinodeshortestpath";

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
        String peterId = name2Ids.get("peter");
        String joshId = name2Ids.get("josh");
        String vadasId = name2Ids.get("vadas");
        String reqBody = String.format("{ " +
                                       "\"vertices\": { " +
                                       " \"ids\": [\"%s\", \"%s\", \"%s\", " +
                                       " \"%s\"]}, " +
                                       "\"step\": { " +
                                       " \"direction\": \"BOTH\", " +
                                       " \"properties\": {}}, " +
                                       "\"max_depth\": 10, " +
                                       "\"capacity\": 100000000, " +
                                       "\"with_vertex\": true}",
                                       markoId, peterId, joshId, vadasId);
        Response r = client().post(this.path, reqBody);
        String content = assertResponseStatus(200, r);
        List<?> paths = assertJsonContains(content, "paths");
        Assert.assertEquals(6, paths.size());
    }
}
