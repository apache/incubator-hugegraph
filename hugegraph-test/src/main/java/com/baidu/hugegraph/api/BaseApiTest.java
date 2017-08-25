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

import org.apache.tinkerpop.gremlin.structure.T;
import org.glassfish.jersey.client.filter.EncodingFilter;
import org.glassfish.jersey.message.GZipEncoder;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.baidu.hugegraph.core.BaseCoreTest;
import com.baidu.hugegraph.schema.SchemaManager;

public class BaseApiTest extends BaseCoreTest {

    public static String BASE_URL = "http://127.0.0.1:8080";

    private static RestClient client;

    @BeforeClass
    public static void init() {
        BaseCoreTest.init();
        client = newClient();
    }

    @AfterClass
    public static void clear() throws Exception {
        BaseCoreTest.clear();
        client.close();
    }

    public RestClient client() {
        return this.client;
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
    }

    /**
     * Utils method to init some properties
     */
    protected void initPropertyKey() {
        SchemaManager schema = graph().schema();
        schema.propertyKey("name").asText().ifNotExist().create();
        schema.propertyKey("age").asInt().ifNotExist().create();
        schema.propertyKey("city").asText().ifNotExist().create();
        schema.propertyKey("lang").asText().ifNotExist().create();
        schema.propertyKey("date").asText().ifNotExist().create();
        schema.propertyKey("price").asInt().ifNotExist().create();
    }

    protected void initVertexLabel() {
        SchemaManager schema = graph().schema();

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .ifNotExist()
              .create();

        schema.vertexLabel("software")
              .properties("name", "lang", "price")
              .primaryKeys("name")
              .ifNotExist()
              .create();
    }

    protected void initEdgeLabel() {
        SchemaManager schema = graph().schema();

        schema.edgeLabel("knows")
              .sourceLabel("person")
              .targetLabel("person")
              .properties("date")
              .ifNotExist()
              .create();

        schema.edgeLabel("created")
              .sourceLabel("person").targetLabel("software")
              .properties("date", "city")
              .ifNotExist()
              .create();
    }

    protected void initVertex() {
        graph().tx().open();

        graph().addVertex(T.label, "person", "name", "marko",
                          "age", 29, "city", "Beijing");
        graph().addVertex(T.label, "person", "name", "vadas",
                          "age", 27, "city", "Hongkong");
        graph().addVertex(T.label, "software", "name", "lop",
                          "lang", "java", "price", 328);
        graph().addVertex(T.label, "person", "name", "josh",
                          "age", 32, "city", "Beijing");
        graph().addVertex(T.label, "software", "name", "ripple",
                          "lang", "java", "price", 199);
        graph().addVertex(T.label, "person", "name", "peter",
                          "age", 29, "city", "Shanghai");

        graph().tx().commit();
    }
}
