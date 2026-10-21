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

package org.apache.hugegraph.auth;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.api.filter.AuthenticationFilter;
import org.apache.hugegraph.api.filter.CompressInterceptor;
import org.apache.hugegraph.api.filter.ExceptionFilter;
import org.apache.hugegraph.api.gremlin.EmbeddedGremlinRestExecutor;
import org.apache.hugegraph.api.gremlin.ForwardGremlinRestExecutor;
import org.apache.hugegraph.api.gremlin.GremlinAPI;
import org.apache.hugegraph.api.gremlin.GremlinClient;
import org.apache.hugegraph.api.gremlin.GremlinRestExecutor;
import org.apache.hugegraph.api.gremlin.GremlinRestExecutorFactory;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.dist.RegisterUtil;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.server.HugeGraphWsAndHttpChannelizer;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.util.Events;
import org.apache.hugegraph.util.JsonUtil;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.channel.WsAndHttpChannelizer;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONUntypedMessageSerializerV1;
import org.apache.tinkerpop.gremlin.util.ser.MessageTextSerializer;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.threadpool.ThreadPoolConfig;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.ServiceUnavailableException;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;

public class EmbeddedGremlinRestExecutorTest {

    private static final String ID = "6a46b56c-5511-491d-8908-aeb1d8185a81";
    private static ContextGremlinServer server;
    private static HugeConfig config;
    private static GremlinClient forward;
    private static EmbeddedGremlinRestExecutor embedded;
    private static Settings settings;
    private static EventHub hub;

    @BeforeClass
    public static void setup() throws Exception {
        OptionSpace.register("server", ServerOptions.instance());
        settings = new Settings();
        settings.host = "127.0.0.1";
        settings.port = 0;
        settings.gremlinPool = 1;
        settings.threadPoolBoss = 1;
        settings.threadPoolWorker = 1;
        settings.evaluationTimeout = 5000;
        settings.channelizer = WsAndHttpChannelizer.class.getName();
        Settings.SerializerSettings json = new Settings.SerializerSettings();
        json.className = GraphSONUntypedMessageSerializerV1.class.getName();
        json.config = Map.of("ioRegistries", Arrays.asList(
                "org.apache.hugegraph.io.HugeGraphIoRegistry",
                "org.apache.hugegraph.io.HugeGraphSONV1MessageIoRegistry"));
        settings.serializers = Collections.singletonList(json);
        hub = new EventHub("embedded-http-test", 1);
        server = new ContextGremlinServer(settings, hub);
        server.getServerGremlinExecutor().getGraphManager().putGraph("known", EmptyGraph.instance());
        server.start().get(30, TimeUnit.SECONDS);
        Channel listening = Whitebox.getInternalState(server, "serverSocketChannel");
        settings.port = ((InetSocketAddress) listening.localAddress()).getPort();
        config = new HugeConfig(Map.of(
                "gremlinserver.url", "http://127.0.0.1:" + settings.port,
                "gremlinserver.rest_execution_mode", "embedded",
                "gremlinserver.timeout", "2", "gremlinserver.max_route", "2"));
        embedded = (EmbeddedGremlinRestExecutor) GremlinRestExecutorFactory.create(config, server, "");
        GremlinRestExecutorFactory.initialize(embedded, server);
        forward = new GremlinClient(config.get(ServerOptions.GREMLIN_SERVER_URL), 2000, 2, 2);
        HugeGraphAuthProxy.resetContext();
    }

    @AfterClass
    public static void teardown() throws Exception {
        if (embedded != null) {
            embedded.close();
        }
        if (forward != null) {
            forward.close();
        }
        if (server != null) {
            server.stop().get(30, TimeUnit.SECONDS);
        }
        HugeGraphAuthProxy.resetContext();
    }

    @Test
    public void testPostWireContract() {
        for (String script : Arrays.asList("1+2", "[]", "[null,true,1,2.5,'中文']",
                                          "[a:1,b:[2,3]]", "'hello'", "null")) {
            comparePost(body(script));
        }
        comparePost("{\"requestId\":\"" + ID + "\",\"gremlin\":\"x\",\"bindings\":" +
                    "{\"x\":{\"a\":[null,true,123,1.25]}}}");
    }

    @Test
    public void testGetEncodingAndStringBindings() {
        for (String value : Arrays.asList("a+b c%20d", "中文&=?#", "%7Bx%7D/[y]", "a\\b\"c")) {
            MultivaluedMap<String, String> params = new MultivaluedHashMap<>();
            params.add("gremlin", "x");
            params.add("bindings.x", value);
            params.add("requestId", ID);
            compare(forward.doGetRequest(null, params), embedded.get(null, params, "test"));
        }
    }

    @Test
    public void testUnresolvedGetTemplateMatchesForward() {
        MultivaluedMap<String, String> params = new MultivaluedHashMap<>();
        params.add("gremlin", "x");
        params.add("bindings.x", "{x}");
        Assert.assertThrows(IllegalStateException.class,
                            () -> forward.doGetRequest(null, params));
        Assert.assertThrows(IllegalStateException.class,
                            () -> embedded.get(null, params, "test"));
    }

    @Test
    public void testErrorsAndAliases() {
        for (String request : Arrays.asList("{}", "{", "null", "", "[]",
                "{\"gremlin\":1}", "{\"gremlin\":null}",
                "{\"gremlin\":\"1\",\"bindings\":[]}",
                "{\"gremlin\":\"1\",\"requestId\":\"bad\"}",
                "{\"requestId\":\"" + ID + "\",\"gremlin\":\"1\",\"aliases\":{\"g\":\"missing\"}}",
                body("throw new IllegalArgumentException('bad')"),
                body("throw new SecurityException('denied')"),
                body("doesNotExist()"))) {
            comparePost(request);
        }
        comparePost(body("42"));
    }

    @Test
    public void testRequestIdGeneratedWhenAbsent() {
        try (Response response = embedded.post(null, "{\"gremlin\":\"1\"}", "test")) {
            Map<?, ?> result = json(response);
            Assert.assertNotNull(UUID.fromString((String) result.get("requestId")));
        }
    }

    @Test
    public void testDuplicateGetParameter() {
        MultivaluedMap<String, String> params = new MultivaluedHashMap<>();
        params.add("gremlin", "1");
        params.add("unrecognized", "a");
        params.add("unrecognized", "b");
        Assert.assertThrows(IllegalArgumentException.class,
                            () -> embedded.get(null, params, "test"));
        Assert.assertThrows(IllegalArgumentException.class,
                            () -> forward.doGetRequest(null, params));
    }

    @Test
    public void testSharedSerializerAndStartupStopGuards() {
        EmbeddedGremlinRestExecutor pending = new EmbeddedGremlinRestExecutor(
                config, server.getServerGremlinExecutor());
        Assert.assertThrows(ServiceUnavailableException.class,
                            () -> pending.post(null, body("1"), "test"));
        GremlinRestExecutorFactory.initialize(pending, server);
        Assert.assertSame(((HugeGraphWsAndHttpChannelizer) server.getChannelizer()).jsonSerializer(),
                          Whitebox.getInternalState(pending, "serializer"));
        pending.beginShutdown();
        Assert.assertThrows(ServiceUnavailableException.class,
                            () -> pending.post(null, body("1"), "test"));
        pending.close();
        pending.close();
        comparePost(body("1"));
    }

    @Test
    public void testFactoryRejectsRemoteAndMismatchedAuthentication() {
        HugeConfig remote = new HugeConfig(Map.of(
                "gremlinserver.url", "http://192.0.2.1:8182",
                "gremlinserver.rest_execution_mode", "embedded"));
        Assert.assertThrows(IllegalArgumentException.class,
                            () -> GremlinRestExecutorFactory.create(remote, server, ""));
        HugeConfig auth = new HugeConfig(Map.of(
                "gremlinserver.url", config.get(ServerOptions.GREMLIN_SERVER_URL),
                "gremlinserver.rest_execution_mode", "embedded",
                "auth.authenticator", StandardAuthenticator.class.getName()));
        Assert.assertThrows(IllegalArgumentException.class,
                            () -> GremlinRestExecutorFactory.create(auth, server, ""));
    }

    @Test
    public void testReadTimeoutDoesNotCancelExecution() throws Exception {
        AtomicInteger writes = new AtomicInteger();
        Whitebox.invoke(server.getServerGremlinExecutor().getGremlinExecutor(), "globalBindings",
                        new Class<?>[]{String.class, Object.class}, "put", "writes", writes);
        HugeConfig shortWait = new HugeConfig(Map.of(
                "gremlinserver.url", config.get(ServerOptions.GREMLIN_SERVER_URL),
                "gremlinserver.timeout", "1", "gremlinserver.max_route", "1"));
        EmbeddedGremlinRestExecutor timed = new EmbeddedGremlinRestExecutor(
                shortWait, server.getServerGremlinExecutor());
        GremlinRestExecutorFactory.initialize(timed, server);
        try {
            Assert.assertThrows(ProcessingException.class, () -> timed.post(null,
                    body("Thread.sleep(1300); writes.incrementAndGet()"), "test"));
            // Queued on the same worker: completion proves the timed-out script ran once.
            comparePost(body("writes.get()"));
            Assert.assertEquals(1, writes.get());
        } finally {
            timed.close();
        }
    }

    @Test
    public void testWorkerUsesSubmittingUserAndClearsItAfterFailure() {
        try {
            for (String name : Arrays.asList("alice", "bob", "alice")) {
                HugeGraphAuthProxy.setContext(new HugeGraphAuthProxy.Context(
                        new HugeAuthenticator.User(name, RolePermission.admin())));
                try (Response response = embedded.post(null, body(
                        "org.apache.hugegraph.auth.HugeGraphAuthProxy.getContext().user().username()"),
                        "test")) {
                    Assert.assertEquals(Collections.singletonList(name),
                                        ((Map<?, ?>) json(response).get("result")).get("data"));
                }
                try (Response ignored = embedded.post(null, body("throw new RuntimeException('x')"),
                                                       "test")) {
                    Assert.assertEquals(500, ignored.getStatus());
                }
            }
            HugeGraphAuthProxy.resetContext();
            try (Response response = embedded.post(null, body(
                    "org.apache.hugegraph.auth.HugeGraphAuthProxy.getContext() == null"), "test")) {
                Assert.assertEquals(Collections.singletonList(true),
                                    ((Map<?, ?>) json(response).get("result")).get("data"));
            }
        } finally {
            HugeGraphAuthProxy.resetContext();
        }
    }

    @Test
    public void testProtocolSizeLimits() {
        for (int size : new int[]{settings.maxContentLength - 256,
                                  settings.maxContentLength + 256}) {
            String request = JsonUtil.toJson(Map.of("gremlin", "1", "padding", "x".repeat(size)));
            try (Response left = forward.doPostRequest(null, request);
                 Response right = embedded.post(null, request, "test")) {
                Assert.assertEquals(left.getStatus(), right.getStatus());
            }
        }
        MultivaluedMap<String, String> params = new MultivaluedHashMap<>();
        params.add("gremlin", "'" + "x".repeat(settings.maxInitialLineLength + 10) + "'");
        try (Response left = forward.doGetRequest(null, params);
             Response right = embedded.get(null, params, "test")) {
            Assert.assertEquals(left.getStatus(), right.getStatus());
        }
    }

    @Test
    public void testHeaderLimitBoundary() throws Exception {
        int low = 0;
        int high = settings.maxHeaderSize;
        while (low + 1 < high) {
            int middle = (low + high) / 2;
            // A decoder-rejected request can leave extra bytes on the old
            // connection. Use a fresh connection to measure the actual limit.
            GremlinClient client = new GremlinClient(config.get(ServerOptions.GREMLIN_SERVER_URL),
                                                    2000, 1, 1);
            try (Response response = client.doPostRequest("x".repeat(middle), body("1"))) {
                if (response.getStatus() == 200) {
                    low = middle;
                } else {
                    high = middle;
                }
            } finally {
                client.close();
            }
        }
        for (int size : new int[]{low, high}) {
            GremlinClient client = new GremlinClient(config.get(ServerOptions.GREMLIN_SERVER_URL),
                                                    2000, 1, 1);
            try {
                compare(client.doPostRequest("x".repeat(size), body("1")),
                        embedded.post("x".repeat(size), body("1"), "test"));
            } finally {
                client.close();
            }
        }
    }

    @Test
    public void testOuterRestResponseContract() throws Exception {
        ForwardGremlinRestExecutor proxy = new ForwardGremlinRestExecutor(config);
        HttpServer left = rest(proxy);
        HttpServer right = rest(embedded);
        try {
            HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
            for (String request : Arrays.asList(body("1+2"), "{}",
                    body("throw new IllegalArgumentException('bad')"),
                    body("throw new SecurityException('denied')"))) {
                HttpResponse<String> expected = client.send(post(left, request),
                                                            HttpResponse.BodyHandlers.ofString());
                HttpResponse<String> actual = client.send(post(right, request),
                                                          HttpResponse.BodyHandlers.ofString());
                Assert.assertEquals(expected.statusCode(), actual.statusCode());
                Assert.assertEquals(expected.headers().firstValue("content-type"),
                                    actual.headers().firstValue("content-type"));
                Assert.assertEquals(expected.body(), actual.body());
            }
        } finally {
            left.shutdownNow();
            right.shutdownNow();
            proxy.close();
        }
    }

    @Test
    public void testRestAuthenticatesBasicAndBearerOnce() throws Exception {
        GraphManager manager =
                Mockito.mock(GraphManager.class);
        Mockito.when(manager.requireAuthentication()).thenReturn(true);
        AtomicInteger authentications = new AtomicInteger();
        Mockito.when(manager.authenticate(Mockito.anyMap())).thenAnswer(call -> {
            authentications.incrementAndGet();
            Map<String, String> credentials = call.getArgument(0);
            String name = credentials.containsKey(HugeAuthenticator.KEY_TOKEN) ? "bearer" : "basic";
            HugeAuthenticator.User user = new HugeAuthenticator.User(name, RolePermission.admin());
            HugeGraphAuthProxy.setContext(new HugeGraphAuthProxy.Context(user));
            return user;
        });
        HttpServer rest = rest(embedded, manager);
        try {
            HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
            int port = rest.getListeners().iterator().next().getPort();
            for (String credential : Arrays.asList("Basic dXNlcjpwYXNz", "Bearer test-token")) {
                HttpRequest request = HttpRequest.newBuilder(URI.create(
                        "http://127.0.0.1:" + port + "/gremlin"))
                        .timeout(Duration.ofSeconds(10))
                        .header("Content-Type", "application/json")
                        .header("Authorization", credential)
                        .POST(HttpRequest.BodyPublishers.ofString(body(
                                "org.apache.hugegraph.auth.HugeGraphAuthProxy.getContext().user().username()")))
                        .build();
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                Assert.assertEquals(response.body(), 200, response.statusCode());
                Map<?, ?> result = JsonUtil.fromJson(response.body(), Map.class);
                Assert.assertEquals(Collections.singletonList(
                        credential.startsWith("Basic") ? "basic" : "bearer"),
                        ((Map<?, ?>) result.get("result")).get("data"));
            }
            Assert.assertEquals(2, authentications.get());
        } finally {
            rest.shutdownNow();
        }
    }

    private static HttpRequest post(HttpServer server, String body) {
        int port = server.getListeners().iterator().next().getPort();
        return HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/gremlin"))
                          .timeout(Duration.ofSeconds(10))
                          .header("Content-Type", "application/json")
                          .POST(HttpRequest.BodyPublishers.ofString(body)).build();
    }

    private static HttpServer rest(GremlinRestExecutor executor) throws Exception {
        return rest(executor, null);
    }

    private static HttpServer rest(GremlinRestExecutor executor,
                                   GraphManager manager) throws Exception {
        ResourceConfig resources = new ResourceConfig(GremlinAPI.class,
                ExceptionFilter.HugeGremlinExceptionMapper.class,
                ExceptionFilter.UnknownExceptionMapper.class,
                ExceptionFilter.IllegalArgumentExceptionMapper.class,
                CompressInterceptor.class);
        if (manager != null) {
            resources.register(AuthenticationFilter.class);
        }
        resources.register(new AbstractBinder() {
            @Override
            protected void configure() {
                bind(executor).to(GremlinRestExecutor.class);
                bind(config).to(HugeConfig.class);
                if (manager != null) {
                    bind(manager).to(GraphManager.class);
                }
            }
        });
        HttpServer server = GrizzlyHttpServerFactory.createHttpServer(
                URI.create("http://127.0.0.1:0"), resources, false);
        NetworkListener listener = server.getListeners().iterator().next();
        listener.getTransport().setSelectorRunnersCount(1);
        listener.getTransport().setWorkerThreadPoolConfig(
                ThreadPoolConfig.defaultConfig().setCorePoolSize(2).setMaxPoolSize(2));
        server.start();
        return server;
    }

    @Test
    public void testForwardHeaderShape() throws Exception {
        try (ServerSocket listener = new ServerSocket(0)) {
            CompletableFuture<String> captured = CompletableFuture.supplyAsync(() -> {
                try (Socket connection = listener.accept()) {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(
                            connection.getInputStream(), StandardCharsets.ISO_8859_1));
                    StringBuilder headers = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null && !line.isEmpty()) {
                        headers.append(line).append("\r\n");
                    }
                    connection.getOutputStream().write(("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n" +
                            "Content-Type: application/json\r\nConnection: close\r\n\r\n{}")
                            .getBytes(StandardCharsets.US_ASCII));
                    return headers.toString();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            GremlinClient client = new GremlinClient("http://127.0.0.1:" + listener.getLocalPort(),
                                                    2000, 1, 1);
            try (Response response = client.doPostRequest("Bearer example", body("1"))) {
                Assert.assertEquals(200, response.getStatus());
                String headers = captured.get(5, TimeUnit.SECONDS);
                Assert.assertTrue(headers.contains("Transfer-Encoding: chunked"));
                Assert.assertTrue(headers.contains("Content-Type: application/json"));
                Assert.assertTrue(headers.contains("Accept-Encoding: gzip"));
                Assert.assertTrue(headers.contains("User-Agent: Jersey/"));
            } finally {
                client.close();
            }
        }
    }

    @Test
    public void testHugeGraphTransactionsSerializationAndDynamicAliases() throws Exception {
        checkHugeGraphContract("memory");
    }

    @Test
    public void testRocksDBTransactionsSerializationAndDynamicAliases() throws Exception {
        checkHugeGraphContract("rocksdb");
    }

    private static void checkHugeGraphContract(String backend) throws Exception {
        RegisterUtil.registerBackends();
        Path data = Files.createTempDirectory("gremlin-rest-" + backend);
        HugeGraph graph = HugeFactory.open(new HugeConfig(Map.of(
                "backend", backend, "serializer", "rocksdb".equals(backend) ? "binary" : "text",
                "store", "embedded_contract_" + backend,
                "rocksdb.data_path", data.toString(), "rocksdb.wal_path", data.toString())));
        try {
            graph.initBackend();
            graph.schema().propertyKey("name").asText().ifNotExist().create();
            graph.schema().vertexLabel("person").useCustomizeNumberId().properties("name")
                 .ifNotExist().create();
            graph.tx().commit();
            hub.notify(Events.GRAPH_CREATE, graph).get(5, TimeUnit.SECONDS);
            String name = graph.spaceGraphName();
            String add = "g.addVertex(org.apache.tinkerpop.gremlin.structure.T.label,'person'," +
                         "org.apache.tinkerpop.gremlin.structure.T.id,1L,'name','alice')";
            String request = graphBody(add, name);
            byte[] expected;
            try (Response response = forward.doPostRequest(null, request)) {
                Assert.assertEquals(200, response.getStatus());
                expected = response.readEntity(byte[].class);
            }
            try (Response response = embedded.post(null, graphBody(
                    "g.traversal().V().drop().iterate()", name), "test")) {
                Assert.assertEquals(200, response.getStatus());
            }
            try (Response response = embedded.post(null, request, "test")) {
                Assert.assertEquals(200, response.getStatus());
                Assert.assertArrayEquals(expected, (byte[]) response.getEntity());
            }
            for (GremlinRestExecutor path : Arrays.asList(
                    new ForwardGremlinRestExecutor(config), embedded)) {
                try (Response response = path.post(null, graphBody(
                        "g.addVertex(org.apache.tinkerpop.gremlin.structure.T.label,'person'," +
                        "org.apache.tinkerpop.gremlin.structure.T.id,2L,'name','bob');" +
                        "throw new IllegalArgumentException('rollback')", name), "test")) {
                    Assert.assertEquals(500, response.getStatus());
                }
                try (Response response = path.post(null, graphBody(
                        "g.traversal().V().count()", name), "test")) {
                    Assert.assertEquals(Collections.singletonList(1),
                                        ((Map<?, ?>) json(response).get("result")).get("data"));
                }
                if (path != embedded) {
                    path.close();
                }
            }
            EmbeddedGremlinRestExecutor brokenSerializer = new EmbeddedGremlinRestExecutor(
                    config, server.getServerGremlinExecutor());
            MessageTextSerializer<?> failing =
                    Mockito.mock(MessageTextSerializer.class);
            Mockito.when(failing.serializeResponseAsBinary(
                    Mockito.any(ResponseMessage.class),
                    Mockito.any(ByteBufAllocator.class)))
                    .thenThrow(new SerializationException("intentional failure"));
            brokenSerializer.initialize(failing);
            try (Response response = brokenSerializer.post(null,
                    graphBody(add.replace("1L", "3L"), name), "test")) {
                Assert.assertEquals(500, response.getStatus());
                Assert.assertEquals(SerializationException.class.getName(),
                                    json(response).get("Exception-Class"));
            } finally {
                brokenSerializer.close();
            }
            // The HTTP contract commits before serialization, even if serialization fails.
            try (Response response = embedded.post(null, graphBody(
                    "g.traversal().V().count()", name), "test")) {
                Assert.assertEquals(Collections.singletonList(2),
                                    ((Map<?, ?>) json(response).get("result")).get("data"));
            }
            hub.notify(Events.GRAPH_DROP, graph).get(5, TimeUnit.SECONDS);
            comparePost(graphBody("1", name));
        } finally {
            hub.notify(Events.GRAPH_DROP, graph).get(5, TimeUnit.SECONDS);
            graph.close();
            org.apache.commons.io.FileUtils.deleteDirectory(data.toFile());
        }
    }

    @Test
    public void testEvalTimeoutUsesGremlinEnvelope() {
        String script = "org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource" +
                        ".traversal().withEmbedded(org.apache.tinkerpop.gremlin.tinkergraph.structure." +
                        "TinkerGraph.open()).with('evaluationTimeout',50)" +
                        ".inject(1).map{Thread.sleep(300);1}";
        comparePost(body(script));
        try (Response response = embedded.post(null, body(script), "test")) {
            Assert.assertEquals(500, response.getStatus());
            Assert.assertEquals("java.util.concurrent.TimeoutException", json(response).get("Exception-Class"));
        }
    }

    @Test
    public void testConcurrentUserContexts() throws Exception {
        ExecutorService callers = Executors.newFixedThreadPool(2);
        try {
            List<Future<?>> results = new ArrayList<>();
            for (String name : Arrays.asList("alice", "bob")) {
                results.add(callers.submit(() -> {
                    try {
                        HugeGraphAuthProxy.setContext(new HugeGraphAuthProxy.Context(
                                new HugeAuthenticator.User(name, RolePermission.admin())));
                        for (int i = 0; i < 5; i++) {
                            try (Response response = embedded.post(null, body(
                                    "org.apache.hugegraph.auth.HugeGraphAuthProxy.getContext().user().username()"),
                                    "test")) {
                                Assert.assertEquals(Collections.singletonList(name),
                                        ((Map<?, ?>) json(response).get("result")).get("data"));
                            }
                        }
                    } finally {
                        HugeGraphAuthProxy.resetContext();
                    }
                }));
            }
            for (Future<?> result : results) {
                result.get(10, TimeUnit.SECONDS);
            }
        } finally {
            callers.shutdownNow();
        }
    }

    @Test
    public void testAdmissionWaitsAndDoesNotRejectThirdRequest() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Object engine = server.getServerGremlinExecutor().getGremlinExecutor();
        Whitebox.invoke(engine, "globalBindings", new Class<?>[]{String.class, Object.class},
                        "put", "entered", entered);
        Whitebox.invoke(engine, "globalBindings", new Class<?>[]{String.class, Object.class},
                        "put", "release", release);
        ExecutorService callers = Executors.newFixedThreadPool(3);
        try {
            List<Future<Integer>> results = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                results.add(callers.submit(() -> {
                    try (Response response = embedded.post(null, body(
                            "entered.countDown(); release.await(); 1"), "test")) {
                        return response.getStatus();
                    }
                }));
            }
            Assert.assertTrue(entered.await(1, TimeUnit.SECONDS));
            Semaphore admission = Whitebox.getInternalState(embedded, "admission");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
            while (admission.getQueueLength() == 0 && System.nanoTime() < deadline) {
                Thread.sleep(5);
            }
            Assert.assertEquals(0, admission.availablePermits());
            Assert.assertEquals(1, admission.getQueueLength());
            release.countDown();
            for (Future<Integer> result : results) {
                Assert.assertEquals(Integer.valueOf(200), result.get(5, TimeUnit.SECONDS));
            }
        } finally {
            release.countDown();
            callers.shutdownNow();
        }
    }

    /** Manual transport microbenchmark; not part of the correctness suite. */
    public static void main(String[] args) throws Exception {
        setup();
        ForwardGremlinRestExecutor proxy = new ForwardGremlinRestExecutor(config);
        HttpServer forwardRest = rest(proxy);
        HttpServer embeddedRest = rest(embedded);
        List<Map<String, Object>> measurements = new ArrayList<>();
        try {
            HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
            for (String script : Arrays.asList("1+2", "(1..100).toList()", "Thread.sleep(10);1")) {
                String request = body(script);
                String expected = client.send(post(forwardRest, request),
                                               HttpResponse.BodyHandlers.ofString()).body();
                for (int i = 0; i < 100; i++) {
                    client.send(post(forwardRest, request), HttpResponse.BodyHandlers.ofString());
                    client.send(post(embeddedRest, request), HttpResponse.BodyHandlers.ofString());
                }
                for (int round = 0; round < 4; round++) {
                    for (int turn = 0; turn < 2; turn++) {
                        boolean local = (round + turn) % 2 == 0;
                        long[] elapsed = new long[200];
                        for (int i = 0; i < elapsed.length; i++) {
                            long start = System.nanoTime();
                            HttpResponse<String> response = client.send(
                                    post(local ? embeddedRest : forwardRest, request),
                                    HttpResponse.BodyHandlers.ofString());
                            elapsed[i] = System.nanoTime() - start;
                            Assert.assertEquals(response.body(), 200, response.statusCode());
                            Assert.assertEquals(expected, response.body());
                        }
                        long total = Arrays.stream(elapsed).sum();
                        Arrays.sort(elapsed);
                        Map<String, Object> sample = new LinkedHashMap<>();
                        sample.put("script", script);
                        sample.put("mode", local ? "embedded" : "forward");
                        sample.put("round", round);
                        sample.put("samples", elapsed.length);
                        sample.put("p50_us", elapsed[elapsed.length / 2] / 1000.0);
                        sample.put("p95_us", elapsed[(int) (elapsed.length * 0.95)] / 1000.0);
                        sample.put("throughput_rps", elapsed.length * 1e9 / total);
                        sample.put("sorted_latency_ns", elapsed);
                        measurements.add(sample);
                    }
                }
            }
            Files.writeString(Path.of(args[0]), JsonUtil.toJson(Map.of(
                    "java", System.getProperty("java.version"), "os", System.getProperty("os.name"),
                    "scope", "single-client script-only HTTP transport microbenchmark",
                    "measurements", measurements)));
        } finally {
            forwardRest.shutdownNow();
            embeddedRest.shutdownNow();
            proxy.close();
            teardown();
        }
    }

    public static class ProtocolAuthenticator extends StandardAuthenticator {

        @Override
        public void setup(Map<String, Object> ignored) {
        }

        @Override
        public HugeAuthenticator.User authenticate(Map<String, String> credentials) {
            return HugeAuthenticator.User.ADMIN;
        }
    }

    @Test
    public void testAuthenticatedProtocolLimits() throws Exception {
        Settings limited = new Settings();
        limited.host = "127.0.0.1";
        limited.port = 0;
        limited.gremlinPool = 1;
        limited.threadPoolBoss = 1;
        limited.threadPoolWorker = 1;
        limited.maxInitialLineLength = 128;
        limited.maxHeaderSize = 512;
        limited.channelizer = WsAndHttpChannelizer.class.getName();
        limited.serializers = settings.serializers;
        limited.authentication.authenticator = ProtocolAuthenticator.class.getName();
        limited.authentication.authenticationHandler = WsAndHttpBasicAuthHandler.class.getName();
        ContextGremlinServer authenticated = new ContextGremlinServer(limited,
                new EventHub("embedded-auth-protocol-test", 1));
        try {
            authenticated.start().get(30, TimeUnit.SECONDS);
            Channel socket = Whitebox.getInternalState(authenticated, "serverSocketChannel");
            String url = "http://127.0.0.1:" + ((InetSocketAddress) socket.localAddress()).getPort();
            HugeConfig config = new HugeConfig(Map.of("gremlinserver.url", url));
            EmbeddedGremlinRestExecutor adapter = new EmbeddedGremlinRestExecutor(
                    config, authenticated.getServerGremlinExecutor());
            GremlinRestExecutorFactory.initialize(adapter, authenticated);
            GremlinClient client = new GremlinClient(url, 2000, 1, 1);
            try {
                MultivaluedMap<String, String> params = new MultivaluedHashMap<>();
                params.add("gremlin", "1/*" + "x".repeat(200) + "*/");
                try (Response left = client.doGetRequest("Basic dXNlcjpwYXNz", params);
                     Response right = adapter.get("Basic dXNlcjpwYXNz", params, "test")) {
                    Assert.assertEquals(left.getStatus(), right.getStatus());
                }
            } finally {
                client.close();
                adapter.close();
            }
        } finally {
            authenticated.stop().get(30, TimeUnit.SECONDS);
            HugeGraphAuthProxy.resetContext();
        }
    }

    @Test
    public void testAnonymousAuditUsesRestPeerWithoutCredentials() {
        org.apache.logging.log4j.core.Logger logger = (org.apache.logging.log4j.core.Logger)
                org.apache.logging.log4j.LogManager.getLogger(
                        org.apache.tinkerpop.gremlin.server.GremlinServer.AUDIT_LOGGER_NAME);
        org.apache.logging.log4j.Level oldLevel = logger.getLevel();
        org.apache.logging.log4j.core.Appender appender =
                Mockito.mock(org.apache.logging.log4j.core.Appender.class);
        List<String> messages = new ArrayList<>();
        Mockito.when(appender.getName()).thenReturn("embedded-audit-test");
        Mockito.when(appender.isStarted()).thenReturn(true);
        Mockito.doAnswer(call -> {
            org.apache.logging.log4j.core.LogEvent event = call.getArgument(0);
            messages.add(event.getMessage().getFormattedMessage());
            return null;
        }).when(appender).append(Mockito.any(org.apache.logging.log4j.core.LogEvent.class));
        logger.addAppender(appender);
        logger.setLevel(org.apache.logging.log4j.Level.INFO);
        boolean oldAudit = settings.enableAuditLog;
        try {
            settings.enableAuditLog = true;
            HugeGraphAuthProxy.setContext(HugeGraphAuthProxy.Context.admin());
            try (Response response = embedded.post("Bearer audit-secret-sentinel", body("1"), "peer:123")) {
                Assert.assertEquals(200, response.getStatus());
            }
            Assert.assertEquals(Collections.singletonList(
                    "User anonymous with address peer:123 requested: 1"), messages);
        } finally {
            settings.enableAuditLog = oldAudit;
            HugeGraphAuthProxy.resetContext();
            logger.removeAppender(appender);
            logger.setLevel(oldLevel);
        }
    }

    private static String graphBody(String script, String graph) {
        return JsonUtil.toJson(Map.of("gremlin", script, "requestId", ID,
                                     "aliases", Map.of("g", graph)));
    }

    private static String body(String script) {
        return JsonUtil.toJson(Map.of("gremlin", script, "requestId", ID));
    }

    private static void comparePost(String body) {
        compare(forward.doPostRequest(null, body), embedded.post(null, body, "test"));
    }

    private static Map<?, ?> json(Response response) {
        String text = response.getEntity() instanceof byte[] ?
                      new String((byte[]) response.getEntity(), StandardCharsets.UTF_8) :
                      response.readEntity(String.class);
        return JsonUtil.fromJson(text, Map.class);
    }

    private static void compare(Response expected, Response actual) {
        try (expected; actual) {
            Assert.assertEquals(expected.getStatus(), actual.getStatus());
            Map<?, ?> left = json(expected);
            Map<?, ?> right = json(actual);
            if (!ID.equals(left.get("requestId"))) {
                left.remove("requestId");
                right.remove("requestId");
            }
            left.remove("stackTrace");
            right.remove("stackTrace");
            Assert.assertEquals(left, right);
        }
    }
}
