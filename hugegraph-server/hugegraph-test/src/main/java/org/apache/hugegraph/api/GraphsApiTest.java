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

import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import jakarta.ws.rs.core.Response;

public class GraphsApiTest extends BaseApiTest {

    private static final String TEMP_SPACE = "graph_test";
    private static final String TEMP_AUTH_SPACE = "graph_auth_test";
    private static final String PATH = "graphspaces/graph_test/graphs";
    private static final String PATH_AUTH = "graphspaces/graph_auth_test" +
                                            "/graphs";

    @BeforeClass
    public static void prepareSpace() {
        createSpace(TEMP_SPACE, false);
        createSpace(TEMP_AUTH_SPACE, true);
    }

    @AfterClass
    public static void tearDown() {
        clearSpaces();
    }

    @Test
    public void testDeleteGraph() {
        Response r = createGraph(TEMP_SPACE, "delete");
        assertResponseStatus(201, r);

        Map<String, Object> params = new HashMap<>();
        params.put("confirm_message", "I'm sure to drop the graph");

        r = client().delete(PATH + "/delete", params);
        assertResponseStatus(204, r);
    }
}
