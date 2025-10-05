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

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.hugegraph.util.JsonUtil;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import jakarta.ws.rs.core.Response;

public class GraphSpaceApiTest extends BaseApiTest {

    private static final String PATH = "graphspaces";

    @Before
    public void removeSpaces() {
        Assume.assumeTrue("skip this test for non-hstore",
                          Objects.equals("hstore", System.getProperty("backend")));
        Response r = this.client().get(PATH);
        String result = r.readEntity(String.class);
        Map<String, Object> resultMap = JsonUtil.fromJson(result, Map.class);
        List<String> spaces = (List<String>) resultMap.get("graphSpaces");
        for (String space : spaces) {
            if (!"DEFAULT".equals(space)) {
                this.client().delete(PATH, space);
            }
        }
    }

    @Test
    public void testAddSpaceNamespace() {
        String body = "{\n" +
                      "  \"name\": \"test_add_no_ns\",\n" +
                      "  \"nickname\":\"Test No Namespace\",\n" +
                      "  \"description\": \"no namespace\",\n" +
                      "  \"cpu_limit\": 1000,\n" +
                      "  \"memory_limit\": 1024,\n" +
                      "  \"storage_limit\": 1000,\n" +
                      "  \"compute_cpu_limit\": 0,\n" +
                      "  \"compute_memory_limit\": 0,\n" +
                      "  \"oltp_namespace\": null,\n" +
                      "  \"olap_namespace\": null,\n" +
                      "  \"storage_namespace\": null,\n" +
                      "  \"operator_image_path\": \"aaa\",\n" +
                      "  \"internal_algorithm_image_url\": \"aaa\",\n" +
                      "  \"max_graph_number\": 100,\n" +
                      "  \"max_role_number\": 100,\n" +
                      "  \"auth\": false,\n" +
                      "  \"configs\": {}\n" +
                      "}";
        Response r = this.client().post(PATH, body);
        assertResponseStatus(201, r);

        String body2 = "{\n" +
                       "  \"name\": \"test_add_has_ns\",\n" +
                       "  \"nickname\":\"Test With Namespace\",\n" +
                       "  \"description\": \"has namespace\",\n" +
                       "  \"cpu_limit\": 1000,\n" +
                       "  \"memory_limit\": 1024,\n" +
                       "  \"storage_limit\": 1000,\n" +
                       "  \"compute_cpu_limit\": 0,\n" +
                       "  \"compute_memory_limit\": 0,\n" +
                       "  \"oltp_namespace\": \"oltp5\",\n" +
                       "  \"olap_namespace\": \"olap5\",\n" +
                       "  \"storage_namespace\": \"st5\",\n" +
                       "  \"operator_image_path\": \"aaa\",\n" +
                       "  \"internal_algorithm_image_url\": \"aaa\",\n" +
                       "  \"max_graph_number\": 100,\n" +
                       "  \"max_role_number\": 100,\n" +
                       "  \"auth\": false,\n" +
                       "  \"configs\": {}\n" +
                       "}";
        r = this.client().post(PATH, body2);
        assertResponseStatus(201, r);
    }

    @Test
    public void testGetSpace() {
        Response r = this.client().get(PATH + "/DEFAULT");
        assertResponseStatus(200, r);
    }

    @Test
    public void testDeleteSpace() {
        String spaceName = "test_delete_space";
        String body = "{"
                      + "\"name\":\"" + spaceName + "\","
                      + "\"nickname\":\"Test Delete Space\","
                      + "\"description\":\"Test delete space\","
                      + "\"cpu_limit\":1000,"
                      + "\"memory_limit\":1024,"
                      + "\"storage_limit\":1000,"
                      + "\"compute_cpu_limit\":0,"
                      + "\"compute_memory_limit\":0,"
                      + "\"oltp_namespace\":null,"
                      + "\"olap_namespace\":null,"
                      + "\"storage_namespace\":null,"
                      + "\"operator_image_path\":\"test\","
                      + "\"internal_algorithm_image_url\":\"test\","
                      + "\"max_graph_number\":100,"
                      + "\"max_role_number\":100,"
                      + "\"auth\":false,"
                      + "\"configs\":{}"
                      + "}";

        // Create graph space
        Response r = this.client().post(PATH, body);
        assertResponseStatus(201, r);

        // Verify graph space exists
        r = this.client().get(PATH, spaceName);
        assertResponseStatus(200, r);

        // Delete graph space
        r = this.client().delete(PATH, spaceName);
        assertResponseStatus(204, r);

        // Verify graph space has been deleted
        r = this.client().get(PATH, spaceName);
        assertResponseStatus(400, r);
    }

    @Test
    public void testCreateSpaceWithSameName() {
        String spaceName = "duplicate_space";
        String body = "{"
                      + "\"name\":\"" + spaceName + "\","
                      + "\"nickname\":\"Duplicate Test Space\","
                      + "\"description\":\"Test duplicate space\","
                      + "\"cpu_limit\":1000,"
                      + "\"memory_limit\":1024,"
                      + "\"storage_limit\":1000,"
                      + "\"compute_cpu_limit\":0,"
                      + "\"compute_memory_limit\":0,"
                      + "\"oltp_namespace\":null,"
                      + "\"olap_namespace\":null,"
                      + "\"storage_namespace\":null,"
                      + "\"operator_image_path\":\"test\","
                      + "\"internal_algorithm_image_url\":\"test\","
                      + "\"max_graph_number\":100,"
                      + "\"max_role_number\":100,"
                      + "\"auth\":false,"
                      + "\"configs\":{}"
                      + "}";

        // First creation should succeed
        Response r = this.client().post(PATH, body);
        assertResponseStatus(201, r);

        // Second creation should fail (duplicate name)
        r = this.client().post(PATH, body);
        assertResponseStatus(400, r);
    }

    @Test
    public void testSpaceResourceLimits() {
        String spaceName = "test_limits_space";

        // Test minimum limits
        String minLimitsBody = "{"
                               + "\"name\":\"" + spaceName + "_min\","
                               + "\"nickname\":\"Minimum Limits Test\","
                               + "\"description\":\"Test minimum limits\","
                               + "\"cpu_limit\":1,"
                               + "\"memory_limit\":1,"
                               + "\"storage_limit\":1,"
                               + "\"compute_cpu_limit\":0,"
                               + "\"compute_memory_limit\":0,"
                               + "\"oltp_namespace\":null,"
                               + "\"olap_namespace\":null,"
                               + "\"storage_namespace\":null,"
                               + "\"operator_image_path\":\"test\","
                               + "\"internal_algorithm_image_url\":\"test\","
                               + "\"max_graph_number\":1,"
                               + "\"max_role_number\":1,"
                               + "\"auth\":false,"
                               + "\"configs\":{}"
                               + "}";

        Response r = this.client().post(PATH, minLimitsBody);
        assertResponseStatus(201, r);

        // Test maximum limits
        String maxLimitsBody = "{"
                               + "\"name\":\"" + spaceName + "_max\","
                               + "\"nickname\":\"Maximum Limits Test\","
                               + "\"description\":\"Test maximum limits\","
                               + "\"cpu_limit\":999999,"
                               + "\"memory_limit\":999999,"
                               + "\"storage_limit\":999999,"
                               + "\"compute_cpu_limit\":999999,"
                               + "\"compute_memory_limit\":999999,"
                               + "\"oltp_namespace\":\"large_oltp\","
                               + "\"olap_namespace\":\"large_olap\","
                               + "\"storage_namespace\":\"large_storage\","
                               + "\"operator_image_path\":\"large_path\","
                               + "\"internal_algorithm_image_url\":\"large_url\","
                               + "\"max_graph_number\":999999,"
                               + "\"max_role_number\":999999,"
                               + "\"auth\":true,"
                               + "\"configs\":{\"large_key\":\"large_value\"}"
                               + "}";

        r = this.client().post(PATH, maxLimitsBody);
        assertResponseStatus(201, r);
    }

    @Test
    public void testInvalidSpaceCreation() {
        // Test invalid space name
        String invalidNameBody = "{"
                                 + "\"name\":\"\","
                                 + "\"nickname\":\"Invalid Name Test\","
                                 + "\"description\":\"Test invalid name\","
                                 + "\"cpu_limit\":1000,"
                                 + "\"memory_limit\":1024,"
                                 + "\"storage_limit\":1000,"
                                 + "\"compute_cpu_limit\":0,"
                                 + "\"compute_memory_limit\":0,"
                                 + "\"oltp_namespace\":null,"
                                 + "\"olap_namespace\":null,"
                                 + "\"storage_namespace\":null,"
                                 + "\"operator_image_path\":\"test\","
                                 + "\"internal_algorithm_image_url\":\"test\","
                                 + "\"max_graph_number\":100,"
                                 + "\"max_role_number\":100,"
                                 + "\"auth\":false,"
                                 + "\"configs\":{}"
                                 + "}";

        Response r = this.client().post(PATH, invalidNameBody);
        assertResponseStatus(400, r);

        // Test negative limits
        String negativeLimitsBody = "{"
                                    + "\"name\":\"test_negative\","
                                    + "\"nickname\":\"Negative Limits Test\","
                                    + "\"description\":\"Test negative limits\","
                                    + "\"cpu_limit\":-1,"
                                    + "\"memory_limit\":-1,"
                                    + "\"storage_limit\":-1,"
                                    + "\"compute_cpu_limit\":0,"
                                    + "\"compute_memory_limit\":0,"
                                    + "\"oltp_namespace\":null,"
                                    + "\"olap_namespace\":null,"
                                    + "\"storage_namespace\":null,"
                                    + "\"operator_image_path\":\"test\","
                                    + "\"internal_algorithm_image_url\":\"test\","
                                    + "\"max_graph_number\":-1,"
                                    + "\"max_role_number\":-1,"
                                    + "\"auth\":false,"
                                    + "\"configs\":{}"
                                    + "}";

        r = this.client().post(PATH, negativeLimitsBody);
        assertResponseStatus(400, r);
    }
}
