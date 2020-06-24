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

package com.baidu.hugegraph.api;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.filter.EncodingFilter;
import org.glassfish.jersey.message.GZipEncoder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.util.JsonUtil;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

public class BaseApiTest {

    private static String BASE_URL = "http://127.0.0.1:8080";
    private static String GRAPH = "hugegraph";

    private static final String URL_PREFIX = "graphs/" + GRAPH;
    private static final String SCHEMA_PKS = "/schema/propertykeys";
    private static final String SCHEMA_VLS = "/schema/vertexlabels";
    private static final String SCHEMA_ELS = "/schema/edgelabels";
    private static final String SCHEMA_ILS = "/schema/indexlabels";
    private static final String GRAPH_VERTEX = "/graph/vertices";
    private static final String GRAPH_EDGE = "/graph/edges";

    private static RestClient client;

    private static final ObjectMapper mapper = new ObjectMapper();

    @BeforeClass
    public static void init() {
        client = newClient();
        BaseApiTest.clearData();
    }

    @AfterClass
    public static void clear() throws Exception {
        client.close();
    }

    @After
    public void teardown() throws Exception {
        BaseApiTest.clearData();
    }

    public RestClient client() {
        return client;
    }

    public static RestClient newClient() {
        return new RestClient(BASE_URL);
    }

    static class RestClient {

        private Client client;
        private WebTarget target;

        public RestClient(String url) {
            this.client = ClientBuilder.newClient();
            this.client.register(EncodingFilter.class);
            this.client.register(GZipEncoder.class);
            this.target = this.client.target(url);
        }

        public void close() {
            this.client.close();
        }

        public WebTarget target() {
            return this.target;
        }

        public WebTarget target(String url) {
            return this.client.target(url);
        }

        public Response get(String path) {
            return this.target.path(path).request().get();
        }

        public Response get(String path, String id) {
            return this.target.path(path).path(id).request().get();
        }

        public Response get(String path, Map<String, Object> params) {
            WebTarget target = this.target.path(path);
            for (Map.Entry<String, Object> i : params.entrySet()) {
                target = target.queryParam(i.getKey(), i.getValue());
            }
            return target.request().get();
        }

        public Response post(String path, String content) {
            return this.post(path, Entity.json(content));
        }

        public Response post(String path, Entity<?> entity) {
            return this.target.path(path).request().post(entity);
        }

        public Response put(String path, String id, String content,
                            Map<String, Object> params) {
            WebTarget target = this.target.path(path).path(id);
            for (Map.Entry<String, Object> i : params.entrySet()) {
                target = target.queryParam(i.getKey(), i.getValue());
            }
            return target.request().put(Entity.json(content));
        }

        public Response delete(String path, String id) {
            return this.target.path(path).path(id).request().delete();
        }

        public Response delete(String path, Map<String, Object> params) {
            WebTarget target = this.target.path(path);
            for (Map.Entry<String, Object> i : params.entrySet()) {
                target = target.queryParam(i.getKey(), i.getValue());
            }
            return target.request().delete();
        }
    }

    /**
     * Utils method to init some properties
     */
    protected static void initPropertyKey() {
        String path = URL_PREFIX + SCHEMA_PKS;

        createAndAssert(path, "{\n"
                + "\"name\": \"name\",\n"
                + "\"data_type\": \"TEXT\",\n"
                + "\"cardinality\": \"SINGLE\",\n"
                + "\"check_exist\": false,\n"
                + "\"properties\":[]\n"
                + "}");
        createAndAssert(path, "{\n"
                + "\"name\": \"age\",\n"
                + "\"data_type\": \"INT\",\n"
                + "\"cardinality\": \"SINGLE\",\n"
                + "\"check_exist\": false,\n"
                + "\"properties\":[]\n"
                + "}");
        createAndAssert(path, "{\n"
                + "\"name\": \"city\",\n"
                + "\"data_type\": \"TEXT\",\n"
                + "\"cardinality\": \"SINGLE\",\n"
                + "\"check_exist\": false,\n"
                + "\"properties\":[]\n"
                + "}");
        createAndAssert(path, "{\n"
                + "\"name\": \"lang\",\n"
                + "\"data_type\": \"TEXT\",\n"
                + "\"cardinality\": \"SINGLE\",\n"
                + "\"check_exist\": false,\n"
                + "\"properties\":[]\n"
                + "}");
        createAndAssert(path, "{\n"
                + "\"name\": \"date\",\n"
                + "\"data_type\": \"TEXT\",\n"
                + "\"cardinality\": \"SINGLE\",\n"
                + "\"check_exist\": false,\n"
                + "\"properties\":[]\n"
                + "}");
        createAndAssert(path, "{\n"
                + "\"name\": \"price\",\n"
                + "\"data_type\": \"INT\",\n"
                + "\"cardinality\": \"SINGLE\",\n"
                + "\"check_exist\": false,\n"
                + "\"properties\":[]\n"
                + "}");
        createAndAssert(path, "{\n"
                + "\"name\": \"weight\",\n"
                + "\"data_type\": \"DOUBLE\",\n"
                + "\"cardinality\": \"SINGLE\",\n"
                + "\"check_exist\": false,\n"
                + "\"properties\":[]\n"
                + "}");
    }

    protected static void initVertexLabel() {
        String path = URL_PREFIX + SCHEMA_VLS;

        createAndAssert(path, "{\n"
                + "\"primary_keys\":[\"name\"],\n"
                + "\"id_strategy\": \"PRIMARY_KEY\",\n"
                + "\"name\": \"person\",\n"
                + "\"properties\":[\"city\", \"name\", \"age\"],\n"
                + "\"check_exist\": false,\n"
                + "\"nullable_keys\":[]\n"
                + "}");

        createAndAssert(path, "{\n"
                + "\"primary_keys\":[\"name\"],\n"
                + "\"id_strategy\": \"PRIMARY_KEY\",\n"
                + "\"name\": \"software\",\n"
                + "\"properties\":[\"price\", \"name\", \"lang\"],\n"
                + "\"check_exist\": false,\n"
                + "\"nullable_keys\":[]\n"
                + "}");
    }

    protected static void initEdgeLabel() {
        String path = URL_PREFIX + SCHEMA_ELS;

        createAndAssert(path, "{\n"
                + "\"name\": \"created\",\n"
                + "\"source_label\": \"person\",\n"
                + "\"target_label\": \"software\",\n"
                + "\"frequency\": \"SINGLE\",\n"
                + "\"properties\":[\"date\", \"weight\"],\n"
                + "\"sort_keys\":[],\n"
                + "\"check_exist\": false,\n"
                + "\"nullable_keys\":[]\n"
                + "}");
        createAndAssert(path, "{\n"
                + "\"name\": \"knows\",\n"
                + "\"source_label\": \"person\",\n"
                + "\"target_label\": \"person\",\n"
                + "\"frequency\": \"MULTIPLE\",\n"
                + "\"properties\":[\"date\", \"weight\"],\n"
                + "\"sort_keys\":[\"date\"],\n"
                + "\"check_exist\": false,\n"
                + "\"nullable_keys\":[]\n"
                + "}");
    }

    protected static void initIndexLabel() {
        String path = URL_PREFIX + SCHEMA_ILS;

        Response r = client.post(path, "{\n"
                + "\"name\": \"personByCity\",\n"
                + "\"base_type\": \"VERTEX_LABEL\",\n"
                + "\"base_value\": \"person\",\n"
                + "\"index_type\": \"SECONDARY\",\n"
                + "\"check_exist\": false,\n"
                + "\"fields\": [\n"
                + "\"city\"\n"
                + "]\n"
                + "}");
        assertResponseStatus(202, r);
    }

    protected static void initVertex() {
        String path = URL_PREFIX + GRAPH_VERTEX;

        createAndAssert(path, "{\n"
                + "\"label\": \"person\",\n"
                + "\"type\": \"vertex\",\n"
                + "\"properties\":{"
                + "\"name\": \"marko\","
                + "\"age\": 29,"
                + "\"city\": \"Beijing\""
                + "}\n"
                + "}");
        createAndAssert(path, "{\n"
                + "\"label\": \"person\",\n"
                + "\"type\": \"vertex\",\n"
                + "\"properties\":{"
                + "\"name\": \"vadas\","
                + "\"age\": 27,"
                + "\"city\": \"HongKong\""
                + "}\n"
                + "}");
        createAndAssert(path, "{\n"
                + "\"label\": \"person\",\n"
                + "\"type\": \"vertex\",\n"
                + "\"properties\":{"
                + "\"name\": \"josh\","
                + "\"age\": 32,"
                + "\"city\": \"Beijing\""
                + "}\n"
                + "}");
        createAndAssert(path, "{\n"
                + "\"label\": \"person\",\n"
                + "\"type\": \"vertex\",\n"
                + "\"properties\":{"
                + "\"name\": \"peter\","
                + "\"age\": 35,"
                + "\"city\": \"Shanghai\""
                + "}\n"
                + "}");
        createAndAssert(path, "{\n"
                + "\"label\": \"software\",\n"
                + "\"type\": \"vertex\",\n"
                + "\"properties\":{"
                + "\"name\": \"ripple\","
                + "\"lang\": \"java\","
                + "\"price\": 199"
                + "}\n"
                + "}");
        createAndAssert(path, "{\n"
                + "\"label\": \"software\",\n"
                + "\"type\": \"vertex\",\n"
                + "\"properties\":{"
                + "\"name\": \"lop\","
                + "\"lang\": \"java\","
                + "\"price\": 328"
                + "}\n"
                + "}");
    }

    protected static Response createAndAssert(String path, String body) {
        Response r = client.post(path, body);
        assertResponseStatus(201, r);
        return r;
    }

    protected static String getVertexId(String label, String key, String value)
                                        throws IOException {
        String props = mapper.writeValueAsString(ImmutableMap.of(key, value));
        Map<String, Object> params = ImmutableMap.of(
                "label", label,
                "properties", URLEncoder.encode(props, "UTF-8")
        );
        Response r = client.get(URL_PREFIX + GRAPH_VERTEX, params);
        String content = r.readEntity(String.class);
        if (r.getStatus() != 200) {
            throw new HugeException("Failed to get vertex id: %s", content);
        }

        @SuppressWarnings("rawtypes")
        List<Map> list = readList(content, "vertices", Map.class);
        if (list.size() != 1) {
            throw new HugeException("Failed to get vertex id: %s", content);
        }
        return (String) list.get(0).get("id");
    }

    protected static void clearGraph() {
        Consumer<String> consumer = (urlSuffix) -> {
            String path = URL_PREFIX + urlSuffix;
            String type = urlSuffix.substring(urlSuffix.lastIndexOf('/') + 1);
            Response r = client.get(path);
            if (r.getStatus() != 200) {
                throw new HugeException("Failed to list " + type);
            }
            String content = r.readEntity(String.class);
            @SuppressWarnings("rawtypes")
            List<Map> list = readList(content, type, Map.class);
            List<Object> ids = list.stream().map(e -> e.get("id"))
                                   .collect(Collectors.toList());
            ids.forEach(id -> {
                client.delete(path, (String) id);
            });
        };

        consumer.accept(GRAPH_EDGE);
        consumer.accept(GRAPH_VERTEX);
    }

    protected static void clearSchema() {
        Consumer<String> consumer = (urlSuffix) -> {
            String path = URL_PREFIX + urlSuffix;
            String type = urlSuffix.substring(urlSuffix.lastIndexOf('/') + 1);
            Response r = client.get(path);
            if (r.getStatus() != 200) {
                throw new HugeException("Failed to list " + type);
            }
            String content = r.readEntity(String.class);
            @SuppressWarnings("rawtypes")
            List<Map> list = readList(content, type, Map.class);
            List<Object> names = list.stream().map(e -> e.get("name"))
                                     .collect(Collectors.toList());
            names.forEach(name -> {
                client.delete(path, (String) name);
            });
        };

        consumer.accept(SCHEMA_ILS);
        consumer.accept(SCHEMA_ELS);
        consumer.accept(SCHEMA_VLS);
        consumer.accept(SCHEMA_PKS);
    }

    protected static String parseId(String content) throws IOException {
        Map<?, ?> map = mapper.readValue(content, Map.class);
        return (String) map.get("id");
    }

    protected static <T> List<T> readList(String content,
                                          String key,
                                          Class<T> clazz) {
        try {
            JsonNode root = mapper.readTree(content);
            JsonNode element = root.get(key);
            if (element == null) {
                throw new HugeException(String.format(
                          "Can't find value of the key: %s in json.", key));
            }
            JavaType type = mapper.getTypeFactory()
                                  .constructParametricType(List.class, clazz);
            return mapper.readValue(element.toString(), type);
        } catch (IOException e) {
            throw new HugeException(String.format(
                      "Failed to deserialize %s", content), e);
        }
    }

    protected static void clearData() {
        clearGraph();
        clearSchema();
    }

    protected static void truncate() {
        String token = "162f7848-0b6d-4faf-b557-3a0797869c55";
        String message = "I'm sure to delete all data";

        Map<String, Object> param = ImmutableMap.of("token", token,
                                                    "confirm_message", message);
        client.delete("graphs/" + GRAPH + "/clear", param);
    }

    protected static String assertResponseStatus(int status,
                                                 Response response) {
        String content = response.readEntity(String.class);
        String message = String.format("Response with status %s and content %s",
                                       response.getStatus(), content);
        Assert.assertEquals(message, status, response.getStatus());
        return content;
    }

    public static <T> T assertJsonContains(String response, String key) {
        Map<?, ?> json = JsonUtil.fromJson(response, Map.class);
        return assertMapContains(json, key);
    }

    @SuppressWarnings("unchecked")
    public static <T> T assertMapContains(Map<?, ?> map, String key) {
        String message = String.format("Expect contains key '%s' in %s",
                                       key, map);
        Assert.assertTrue(message, map.containsKey(key));
        return (T) map.get(key);
    }

    public static Map<?, ?> assertArrayContains(List<Map<?, ?>> list,
                                                String key, Object value) {
        String message = String.format("Expect contains {'%s':'%s'} in list %s",
                                       key, value, list);
        Map<?, ?> found = null;
        for (Map<?, ?> map : list) {
            if (map.get(key).equals(value)) {
                found = map;
                break;
            }
        }
        Assert.assertTrue(message, found != null);
        return found;
    }
}
