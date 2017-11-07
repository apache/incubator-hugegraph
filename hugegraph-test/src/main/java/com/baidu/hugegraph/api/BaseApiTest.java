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

import java.util.Map;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.filter.EncodingFilter;
import org.glassfish.jersey.message.GZipEncoder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.google.common.collect.ImmutableMap;

public class BaseApiTest {

    public static String BASE_URL = "http://127.0.0.1:8080";
    public static String GRAPH = "hugegraph";

    private static final String URL_PREFIX = "graphs/" + GRAPH;
    private static final String SCHEMA_PKS = "/schema/propertykeys";
    private static final String SCHEMA_VLS = "/schema/vertexlabels";
    private static final String SCHEMA_ELS = "/schema/edgelabels";
    private static final String SCHEMA_ILS = "/schema/indexlabels";
    private static final String GRAPH_VERTEX = "/graph/vertices";
    private static final String GRAPH_EDGE = "/graph/edges";

    private static RestClient client;

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

        public Response put(String path, String content,
                            Map<String, Object> params) {
            WebTarget target = this.target.path(path);
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

        client.post(path, "{\n"
                + "\"name\": \"name\",\n"
                + "\"data_type\": \"TEXT\",\n"
                + "\"cardinality\": \"SINGLE\",\n"
                + "\"properties\":[]\n"
                + "}");
        client.post(path, "{\n"
                + "\"name\": \"age\",\n"
                + "\"data_type\": \"INT\",\n"
                + "\"cardinality\": \"SINGLE\",\n"
                + "\"properties\":[]\n"
                + "}");
        client.post(path, "{\n"
                + "\"name\": \"city\",\n"
                + "\"data_type\": \"TEXT\",\n"
                + "\"cardinality\": \"SINGLE\",\n"
                + "\"properties\":[]\n"
                + "}");
        client.post(path, "{\n"
                + "\"name\": \"lang\",\n"
                + "\"data_type\": \"TEXT\",\n"
                + "\"cardinality\": \"SINGLE\",\n"
                + "\"properties\":[]\n"
                + "}");
        client.post(path, "{\n"
                + "\"name\": \"date\",\n"
                + "\"data_type\": \"TEXT\",\n"
                + "\"cardinality\": \"SINGLE\",\n"
                + "\"properties\":[]\n"
                + "}");
        client.post(path, "{\n"
                + "\"name\": \"price\",\n"
                + "\"data_type\": \"INT\",\n"
                + "\"cardinality\": \"SINGLE\",\n"
                + "\"properties\":[]\n"
                + "}");
        client.post(path, "{\n"
                + "\"name\": \"weight\",\n"
                + "\"data_type\": \"DOUBLE\",\n"
                + "\"cardinality\": \"SINGLE\",\n"
                + "\"properties\":[]\n"
                + "}");
    }

    protected static void initVertexLabel() {
        String path = URL_PREFIX + SCHEMA_VLS;

        client.post(path, "{\n"
                + "\"primary_keys\":[\"name\"],\n"
                + "\"id_strategy\": \"PRIMARY_KEY\",\n"
                + "\"name\": \"person\",\n"
                + "\"index_names\":[],\n"
                + "\"properties\":[\"city\", \"name\", \"age\"],\n"
                + "\"nullable_keys\":[]\n"
                + "}");
        client.post(path, "{\n"
                + "\"primary_keys\":[\"name\"],\n"
                + "\"id_strategy\": \"PRIMARY_KEY\",\n"
                + "\"name\": \"software\",\n"
                + "\"index_names\":[],\n"
                + "\"properties\":[\"price\", \"name\", \"lang\"],\n"
                + "\"nullable_keys\":[]\n"
                + "}");
    }

    protected static void initEdgeLabel() {
        String path = URL_PREFIX + SCHEMA_ELS;

        client.post(path, "{\n"
                + "\"name\": \"created\",\n"
                + "\"source_label\": \"person\",\n"
                + "\"target_label\": \"software\",\n"
                + "\"frequency\": \"SINGLE\",\n"
                + "\"properties\":[\"date\", \"weight\"],\n"
                + "\"sort_keys\":[],\n"
                + "\"index_names\":[],\n"
                + "\"nullable_keys\":[]\n"
                + "}");
        client.post(path, "{\n"
                + "\"name\": \"knows\",\n"
                + "\"source_label\": \"person\",\n"
                + "\"target_label\": \"person\",\n"
                + "\"frequency\": \"SINGLE\",\n"
                + "\"properties\":[\"date\", \"weight\"],\n"
                + "\"sort_keys\":[],\n"
                + "\"index_names\":[],\n"
                + "\"nullable_keys\":[]\n"
                + "}");
    }

    protected static void initVertex() {
        String path = URL_PREFIX + GRAPH_VERTEX;

        client.post(path, "{\n"
                + "\"label\": \"person\",\n"
                + "\"type\": \"vertex\",\n"
                + "\"properties\":{"
                + "\"name\": \"marko\","
                + "\"age\": 29,"
                + "\"city\": \"Beijing\""
                + "}\n"
                + "}");
        client.post(path, "{\n"
                + "\"label\": \"person\",\n"
                + "\"type\": \"vertex\",\n"
                + "\"properties\":{"
                + "\"name\": \"vadas\","
                + "\"age\": 27,"
                + "\"city\": \"HongKong\""
                + "}\n"
                + "}");
        client.post(path, "{\n"
                + "\"label\": \"person\",\n"
                + "\"type\": \"vertex\",\n"
                + "\"properties\":{"
                + "\"name\": \"josh\","
                + "\"age\": 32,"
                + "\"city\": \"Beijing\""
                + "}\n"
                + "}");
        client.post(path, "{\n"
                + "\"label\": \"person\",\n"
                + "\"type\": \"vertex\",\n"
                + "\"properties\":{"
                + "\"name\": \"peter\","
                + "\"age\": 35,"
                + "\"city\": \"Shanghai\""
                + "}\n"
                + "}");
        client.post(path, "{\n"
                + "\"label\": \"software\",\n"
                + "\"type\": \"vertex\",\n"
                + "\"properties\":{"
                + "\"name\": \"ripple\","
                + "\"lang\": \"java\","
                + "\"price\": 199"
                + "}\n"
                + "}");
        client.post(path, "{\n"
                + "\"label\": \"software\",\n"
                + "\"type\": \"vertex\",\n"
                + "\"properties\":{"
                + "\"name\": \"lop\","
                + "\"lang\": \"java\","
                + "\"price\": 328"
                + "}\n"
                + "}");
    }

    protected static void clearData() {
        String token = "162f7848-0b6d-4faf-b557-3a0797869c55";
        String message = "I'm sure to delete all data";

        Map<String, Object> param = ImmutableMap.of("token", token,
                                                    "confirm_message", message);
        client.delete("graphs/" + GRAPH + "/clear", param);
    }
}
