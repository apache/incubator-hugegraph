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

import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSessionContext;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.http.HttpClientConnection;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.pool.PoolStats;
import org.glassfish.jersey.client.ClientRequest;
import org.glassfish.jersey.internal.util.collection.ImmutableMultivaluedMap;
import org.junit.Test;
import org.mockito.Mockito;

import com.baidu.hugegraph.rest.AbstractRestClient;
import com.baidu.hugegraph.rest.ClientException;
import com.baidu.hugegraph.rest.RestClient;
import com.baidu.hugegraph.rest.RestResult;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.Whitebox;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class RestClientTest {

    private static class RestClientImpl extends AbstractRestClient {

        private final int status;
        private final MultivaluedMap<String, Object> headers;
        private final String content;

        public RestClientImpl(String url, int timeout, int idleTime,
                              int maxTotal, int maxPerRoute, int status) {
            super(url, timeout, idleTime, maxTotal, maxPerRoute);
            this.status = status;
            this.headers = ImmutableMultivaluedMap.empty();
            this.content = "";
        }

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

        public RestClientImpl(String url, String user, String password,
                              int timeout, int maxTotal, int maxPerRoute,
                              String trustStoreFile, String trustStorePassword,
                              int status) {
            super(url, user, password, timeout, maxTotal, maxPerRoute,
                  trustStoreFile, trustStorePassword);
            this.status = status;
            this.headers = ImmutableMultivaluedMap.empty();
            this.content = "";
        }

        public RestClientImpl(String url, int timeout, int status) {
            this(url, timeout, status, ImmutableMultivaluedMap.empty(), "");
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
    public void testCleanExecutor() throws Exception {
        // Modify IDLE_TIME 100ms to speed test
        int newIdleTime = 100;
        int newCheckPeriod = newIdleTime + 20;

        RestClient client = new RestClientImpl("/test", 1000, newIdleTime,
                                               10, 5, 200);

        PoolingHttpClientConnectionManager pool;
        pool = Whitebox.getInternalState(client, "pool");
        pool = Mockito.spy(pool);
        Whitebox.setInternalState(client, "pool", pool);
        HttpRoute route = new HttpRoute(HttpHost.create(
                                        "http://127.0.0.1:8080"));
        // Create a connection manually, it will be put into leased list
        HttpClientConnection conn = pool.requestConnection(route, null)
                                        .get(1L, TimeUnit.SECONDS);
        PoolStats stats = pool.getTotalStats();
        int usingConns = stats.getLeased() + stats.getPending();
        Assert.assertGte(1, usingConns);

        // Sleep more than two check periods for busy connection
        Thread.sleep(newCheckPeriod);
        Mockito.verify(pool, Mockito.never()).closeExpiredConnections();
        stats = pool.getTotalStats();
        usingConns = stats.getLeased() + stats.getPending();
        Assert.assertGte(1, usingConns);

        // The connection will be put into available list
        pool.releaseConnection(conn, null, 0, TimeUnit.SECONDS);

        stats = pool.getTotalStats();
        usingConns = stats.getLeased() + stats.getPending();
        Assert.assertEquals(0, usingConns);
        /*
         * Sleep more than two check periods for free connection,
         * ensure connection has been closed
         */
        Thread.sleep(newCheckPeriod);
        Mockito.verify(pool, Mockito.atLeastOnce())
               .closeExpiredConnections();
        Mockito.verify(pool, Mockito.atLeastOnce())
               .closeIdleConnections(newIdleTime, TimeUnit.MILLISECONDS);
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
    public void testPostHttpsWithAllParams() {
        String trustStoreFile = "src/test/resources/cacerts.jks";
        String trustStorePassword = "changeit";
        RestClient client = new RestClientImpl("/test", "user", "", 1000,
                                               10, 5, trustStoreFile,
                                               trustStorePassword, 200);
        RestResult restResult = client.post("path", "body");
        Assert.assertEquals(200, restResult.status());
    }

    @Test
    public void testHostNameVerifier() {
        BiFunction<String, String, Boolean> verifer = (url, hostname) -> {
            AbstractRestClient.HostNameVerifier verifier;
            SSLSession session;
            try {
                SSLSessionContext sc = SSLContext.getDefault()
                                                 .getClientSessionContext();
                session = sc.getSession(new byte[]{11});
                verifier = new AbstractRestClient.HostNameVerifier(url);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
            return verifier.verify(hostname, session);
        };

        Assert.assertTrue(verifer.apply("http://baidu.com", "baidu.com"));
        Assert.assertTrue(verifer.apply("http://test1.baidu.com", "baidu.com"));
        Assert.assertTrue(verifer.apply("http://test2.baidu.com", "baidu.com"));
        Assert.assertFalse(verifer.apply("http://baidu2.com", "baidu.com"));
        Assert.assertTrue(verifer.apply("http://baidu.com", ""));
        Assert.assertTrue(verifer.apply("baidu.com", "baidu.com"));
        Assert.assertTrue(verifer.apply("http://baidu.com/test", "baidu.com"));
        Assert.assertTrue(verifer.apply("baidu.com/test/abc", "baidu.com"));
        Assert.assertFalse(verifer.apply("baidu.com.sina.com", "baidu.com"));
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
    public void testPutWithHeaders() {
        RestClient client = new RestClientImpl("/test", 1000, 200);
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        headers.add("key1", "value1-1");
        headers.add("key1", "value1-2");
        headers.add("Content-Encoding", "gzip");
        RestResult restResult = client.put("path", "id1", "body", headers);
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

    @Test
    public void testClose() {
        RestClient client = new RestClientImpl("/test", 1000, 10, 5, 200);
        RestResult restResult = client.post("path", "body");
        Assert.assertEquals(200, restResult.status());

        client.close();
        Assert.assertThrows(IllegalStateException.class, () -> {
            client.post("path", "body");
        });

        PoolingHttpClientConnectionManager pool;
        pool = Whitebox.getInternalState(client, "pool");
        Assert.assertNotNull(pool);
        AtomicBoolean isShutDown = Whitebox.getInternalState(pool, "isShutDown");
        Assert.assertTrue(isShutDown.get());

        ScheduledExecutorService cleanExecutor;
        cleanExecutor = Whitebox.getInternalState(client, "cleanExecutor");
        Assert.assertNotNull(cleanExecutor);
        Assert.assertTrue(cleanExecutor.isShutdown());
    }

    @Test
    public void testAuthContext() {
        RestClientImpl client = new RestClientImpl("/test", 1000, 10, 5, 200);
        Assert.assertNull(client.getAuthContext());

        String token = UUID.randomUUID().toString();
        client.setAuthContext(token);
        Assert.assertEquals(token, client.getAuthContext());

        client.resetAuthContext();
        Assert.assertNull(client.getAuthContext());
    }

    private static class MockRestClientImpl extends AbstractRestClient {

        public MockRestClientImpl(String url, int timeout) {
            super(url, timeout);
        }

        @Override
        protected void checkStatus(Response response,
                                   Response.Status... statuses) {
            // pass
        }
    }

    @Test
    public void testRequest() {
        MockRestClientImpl client = new MockRestClientImpl("test", 1000);

        WebTarget target = Mockito.mock(WebTarget.class);
        Builder builder = Mockito.mock(Builder.class);

        Mockito.when(target.path("test")).thenReturn(target);
        Mockito.when(target.path("test")
                           .path(AbstractRestClient.encode("id")))
               .thenReturn(target);
        Mockito.when(target.path("test").request()).thenReturn(builder);
        Mockito.when(target.path("test")
                           .path(AbstractRestClient.encode("id"))
                           .request())
               .thenReturn(builder);

        Response response = Mockito.mock(Response.class);
        Mockito.when(response.getStatus()).thenReturn(200);
        Mockito.when(response.getHeaders())
               .thenReturn(new MultivaluedHashMap<>());
        Mockito.when(response.readEntity(String.class)).thenReturn("content");

        Mockito.when(builder.delete()).thenReturn(response);
        Mockito.when(builder.get()).thenReturn(response);
        Mockito.when(builder.put(Mockito.any())).thenReturn(response);
        Mockito.when(builder.post(Mockito.any())).thenReturn(response);

        Whitebox.setInternalState(client, "target", target);

        RestResult result;

        // Test delete
        client.setAuthContext("token1");
        result = client.delete("test", ImmutableMap.of());
        Assert.assertEquals(200, result.status());
        Mockito.verify(builder).header(HttpHeaders.AUTHORIZATION,
                                       "token1");
        client.resetAuthContext();

        client.setAuthContext("token2");
        result = client.delete("test", "id");
        Assert.assertEquals(200, result.status());
        Mockito.verify(builder).header(HttpHeaders.AUTHORIZATION,
                                       "token2");
        client.resetAuthContext();

        // Test get
        client.setAuthContext("token3");
        result = client.get("test");
        Assert.assertEquals(200, result.status());
        Mockito.verify(builder).header(HttpHeaders.AUTHORIZATION,
                                       "token3");
        client.resetAuthContext();

        client.setAuthContext("token4");
        result = client.get("test", ImmutableMap.of());
        Assert.assertEquals(200, result.status());
        Mockito.verify(builder).header(HttpHeaders.AUTHORIZATION,
                                       "token4");
        client.resetAuthContext();

        client.setAuthContext("token5");
        result = client.get("test", "id");
        Assert.assertEquals(200, result.status());
        Mockito.verify(builder).header(HttpHeaders.AUTHORIZATION,
                                       "token5");
        client.resetAuthContext();

        // Test put
        client.setAuthContext("token6");
        result = client.post("test", new Object());
        Assert.assertEquals(200, result.status());
        Mockito.verify(builder).header(HttpHeaders.AUTHORIZATION,
                                       "token6");
        client.resetAuthContext();

        client.setAuthContext("token7");
        result = client.post("test", new Object(), new MultivaluedHashMap<>());
        Assert.assertEquals(200, result.status());
        Mockito.verify(builder).header(HttpHeaders.AUTHORIZATION,
                                       "token7");
        client.resetAuthContext();

        client.setAuthContext("token8");
        result = client.post("test", new Object(), ImmutableMap.of());
        Assert.assertEquals(200, result.status());
        Mockito.verify(builder).header(HttpHeaders.AUTHORIZATION,
                                       "token8");
        client.resetAuthContext();

        client.setAuthContext("token9");
        result = client.post("test", new Object(), new MultivaluedHashMap<>(),
                             ImmutableMap.of());
        Assert.assertEquals(200, result.status());
        Mockito.verify(builder).header(HttpHeaders.AUTHORIZATION,
                                       "token9");
        client.resetAuthContext();

        // Test post
        client.setAuthContext("token10");
        result = client.post("test", new Object());
        Assert.assertEquals(200, result.status());
        Mockito.verify(builder).header(HttpHeaders.AUTHORIZATION,
                                       "token10");
        client.resetAuthContext();

        client.setAuthContext("token11");
        result = client.post("test", new Object(), new MultivaluedHashMap<>());
        Assert.assertEquals(200, result.status());
        Mockito.verify(builder).header(HttpHeaders.AUTHORIZATION,
                                       "token11");
        client.resetAuthContext();

        client.setAuthContext("token12");
        result = client.post("test", new Object(), ImmutableMap.of());
        Assert.assertEquals(200, result.status());
        Mockito.verify(builder).header(HttpHeaders.AUTHORIZATION,
                                       "token12");
        client.resetAuthContext();

        client.setAuthContext("token13");
        result = client.post("test", new Object(), new MultivaluedHashMap<>(),
                             ImmutableMap.of());
        Assert.assertEquals(200, result.status());
        Mockito.verify(builder).header(HttpHeaders.AUTHORIZATION,
                                       "token13");
        client.resetAuthContext();
    }
}
