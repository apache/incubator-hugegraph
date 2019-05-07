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

package com.baidu.hugegraph.unit.rest;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.internal.util.collection.ImmutableMultivaluedMap;
import org.junit.Test;
import org.mockito.Mockito;

import com.baidu.hugegraph.rest.ClientException;
import com.baidu.hugegraph.rest.RestClient;
import com.baidu.hugegraph.rest.RestResult;
import com.baidu.hugegraph.testutil.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class RestClientTest {

    private static class RestClientImpl extends RestClient {

        private final int status;
        private final MultivaluedMap<String, Object> headers;
        private final String content;

        public RestClientImpl(String url, int timeout,
                              int maxTotal, int maxPerRoute, int status) {
            super(url, timeout, maxTotal, maxPerRoute);
            this.status = status;
            this.headers = ImmutableMultivaluedMap.empty();
            this.content = "";
        }

        public RestClientImpl(String url, String user, String password,
                              int timeout, int status) {
            super(url, user, password, timeout);
            this.status = status;
            this.headers = ImmutableMultivaluedMap.empty();
            this.content = "";
        }

        public RestClientImpl(String url, String user, String password,
                              int timeout, int maxTotal, int maxPerRoute,
                              int status) {
            super(url, user, password, timeout, maxTotal, maxPerRoute);
            this.status = status;
            this.headers = ImmutableMultivaluedMap.empty();
            this.content = "";
        }

        public RestClientImpl(String url, int timeout, int status) {
            this(url, timeout, status, ImmutableMultivaluedMap.empty(), "");
        }

        @SuppressWarnings("unused")
        public RestClientImpl(String url, int timeout, int status,
                              MultivaluedMap<String, Object> headers) {
            this(url, timeout, status, headers, "");
        }

        @SuppressWarnings("unused")
        public RestClientImpl(String url, int timeout, int status,
                              String content) {
            this(url, timeout, status, ImmutableMultivaluedMap.empty(), content);
        }

        public RestClientImpl(String url, int timeout, int status,
                              MultivaluedMap<String, Object> headers,
                              String content) {
            super(url, timeout);
            this.status = status;
            this.headers = headers;
            this.content = content;
        }

        @Override
        protected Response request(Callable<Response> method) {
            Response response = Mockito.mock(Response.class);
            Mockito.when(response.getStatus()).thenReturn(this.status);
            Mockito.when(response.getHeaders()).thenReturn(this.headers);
            Mockito.when(response.readEntity(String.class))
                   .thenReturn(this.content);
            return response;
        }

        @Override
        protected void checkStatus(Response response,
                                   Response.Status... statuses) {
            boolean match = false;
            for (Response.Status status : statuses) {
                if (status.getStatusCode() == response.getStatus()) {
                    match = true;
                    break;
                }
            }
            if (!match) {
                throw new ClientException("Invalid response '%s'", response);
            }
        }
    }

    @Test
    public void testPost() {
        RestClient client = new RestClientImpl("/test", 1000, 200);
        RestResult restResult = client.post("path", "body");
        Assert.assertEquals(200, restResult.status());
    }

    @Test
    // TODO: How to verify it?
    public void testPostWithMaxTotalAndPerRoute() {
        RestClient client = new RestClientImpl("/test", 1000, 10, 5, 200);
        RestResult restResult = client.post("path", "body");
        Assert.assertEquals(200, restResult.status());
    }

    @Test
    public void testPostWithUserAndPassword() {
        RestClient client = new RestClientImpl("/test", "user", "", 1000, 200);
        RestResult restResult = client.post("path", "body");
        Assert.assertEquals(200, restResult.status());
    }

    @Test
    public void testPostWithAllParams() {
        RestClient client = new RestClientImpl("/test", "user", "", 1000,
                                               10, 5, 200);
        RestResult restResult = client.post("path", "body");
        Assert.assertEquals(200, restResult.status());
    }

    @Test
    public void testPostWithHeaderAndContent() {
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        headers.add("key1", "value1-1");
        headers.add("key1", "value1-2");
        headers.add("Content-Encoding", "gzip");
        String content = "{\"names\": [\"marko\", \"josh\", \"lop\"]}";
        RestClient client = new RestClientImpl("/test", 1000, 200,
                                               headers, content);
        RestResult restResult = client.post("path", "body");
        Assert.assertEquals(200, restResult.status());
        Assert.assertEquals(headers, restResult.headers());
        Assert.assertEquals(content, restResult.content());
        Assert.assertEquals(ImmutableList.of("marko", "josh", "lop"),
                            restResult.readList("names", String.class));
    }

    @Test
    public void testPostWithException() {
        RestClient client = new RestClientImpl("/test", 1000, 400);
        Assert.assertThrows(ClientException.class, () -> {
            client.post("path", "body");
        });
    }

    @Test
    public void testPostWithParams() {
        RestClient client = new RestClientImpl("/test", 1000, 200);
        MultivaluedMap<String, Object> headers = ImmutableMultivaluedMap.empty();
        Map<String, Object> params = ImmutableMap.of("param1", "value1");
        RestResult restResult = client.post("path", "body", headers,
                                            params);
        Assert.assertEquals(200, restResult.status());
    }

    @Test
    public void testPut() {
        RestClient client = new RestClientImpl("/test", 1000, 200);
        RestResult restResult = client.put("path", "id1", "body");
        Assert.assertEquals(200, restResult.status());
    }

    @Test
    public void testPutWithParams() {
        RestClient client = new RestClientImpl("/test", 1000, 200);
        Map<String, Object> params = ImmutableMap.of("param1", "value1");
        RestResult restResult = client.put("path", "id1", "body", params);
        Assert.assertEquals(200, restResult.status());
    }

    @Test
    public void testPutWithException() {
        RestClient client = new RestClientImpl("/test", 1000, 400);
        Assert.assertThrows(ClientException.class, () -> {
            client.put("path", "id1", "body");
        });
    }

    @Test
    public void testGet() {
        RestClient client = new RestClientImpl("/test", 1000, 200);
        RestResult restResult = client.get("path");
        Assert.assertEquals(200, restResult.status());
    }

    @Test
    public void testGetWithId() {
        RestClient client = new RestClientImpl("/test", 1000, 200);
        RestResult restResult = client.get("path", "id1");
        Assert.assertEquals(200, restResult.status());
    }

    @Test
    public void testGetWithParams() {
        RestClient client = new RestClientImpl("/test", 1000, 200);
        Map<String, Object> params = new HashMap<>();
        params.put("key1", ImmutableList.of("value1-1", "value1-2"));
        params.put("key2", "value2");
        RestResult restResult = client.get("path", params);
        Assert.assertEquals(200, restResult.status());
    }

    @Test
    public void testGetWithException() {
        RestClient client = new RestClientImpl("/test", 1000, 400);
        Assert.assertThrows(ClientException.class, () -> {
            client.get("path", "id1");
        });
    }

    @Test
    public void testDeleteWithId() {
        RestClient client = new RestClientImpl("/test", 1000, 204);
        RestResult restResult = client.delete("path", "id1");
        Assert.assertEquals(204, restResult.status());
    }

    @Test
    public void testDeleteWithParams() {
        RestClient client = new RestClientImpl("/test", 1000, 204);
        Map<String, Object> params = ImmutableMap.of("param1", "value1");
        RestResult restResult = client.delete("path", params);
        Assert.assertEquals(204, restResult.status());
    }

    @Test
    public void testDeleteWithException() {
        RestClient client = new RestClientImpl("/test", 1000, 400);
        Assert.assertThrows(ClientException.class, () -> {
            client.delete("path", "id1");
        });
    }
}
