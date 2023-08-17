/*
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

package org.apache.hugegraph.api;

import org.junit.Before;
import org.junit.Test;

import jakarta.ws.rs.core.Response;

public class ArthasApiTest extends BaseApiTest {

    private static final String ARTHAS_START_PATH = "/arthas";
    private static final String ARTHAS_API_BASE_URL = "http://127.0.0.1:8561";
    private static final String ARTHAS_API_PATH = "/api";

    @Before
    public void testArthasStart() {
        Response r = client().get(ARTHAS_START_PATH);
        assertResponseStatus(200, r);
    }

    @Test
    public void testArthasApi() {

        String body = "{\n" +
                      "  \"action\": \"exec\",\n" +
                      "  \"requestId\": \"req112\",\n" +
                      "  \"sessionId\": \"\",\n" +
                      "  \"consumerId\": \"955dbd1325334a84972b0f3ac19de4f7_2\",\n" +
                      "  \"command\": \"version\",\n" +
                      "  \"execTimeout\": \"10000\"\n" +
                      "}";
        RestClient arthasApiClient = new RestClient(ARTHAS_API_BASE_URL);
        Response r = arthasApiClient.post(ARTHAS_API_PATH, body);
        String result = assertResponseStatus(200, r);
        assertJsonContains(result, "state");
        assertJsonContains(result, "requestId");
        assertJsonContains(result, "sessionId");
        assertJsonContains(result, "body");
    }
}
