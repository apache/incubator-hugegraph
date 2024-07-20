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

package org.apache.hugegraph.api;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import jakarta.ws.rs.core.Response;

public class ArthasApiTest extends BaseApiTest {

    private static final String ARTHAS_START_PATH = "/arthas";
    private static final String ARTHAS_API_BASE_URL = "http://127.0.0.1:8561";
    private static final String ARTHAS_API_PATH = "/api";

    @Before
    public void testArthasStart() {
        Response r = client().put(ARTHAS_START_PATH, "", "", ImmutableMap.of());
        assertResponseStatus(200, r);
    }

    @Test
    public void testArthasApi() {
        // command exec
        String execBody = "{\n" +
                          "  \"action\": \"exec\",\n" +
                          "  \"command\": \"version\"\n" +
                          "}";
        RestClient arthasApiClient = new RestClient(ARTHAS_API_BASE_URL, false);
        Response execResponse = arthasApiClient.post(ARTHAS_API_PATH, execBody);
        String result = assertResponseStatus(200, execResponse);
        assertJsonContains(result, "state");
        assertJsonContains(result, "body");

        // command session
        String sessionBody = "{\n" +
                             "  \"action\":\"init_session\"\n" +
                             "}";
        Response sessionResponse = arthasApiClient.post(ARTHAS_API_PATH, sessionBody);
        String sessionResult = assertResponseStatus(200, sessionResponse);
        assertJsonContains(sessionResult, "sessionId");
        assertJsonContains(sessionResult, "consumerId");
        assertJsonContains(sessionResult, "state");

        // join session: using invalid sessionId
        String joinSessionBody = "{\n" +
                                 "  \"action\":\"join_session\",\n" +
                                 "  \"sessionId\" : \"xxx\"\n" +
                                 "}";
        Response joinSessionResponse = arthasApiClient.post(ARTHAS_API_PATH, joinSessionBody);
        String joinSessionResult = assertResponseStatus(200, joinSessionResponse);
        assertJsonContains(joinSessionResult, "message");
        assertJsonContains(joinSessionResult, "state");
    }
}
