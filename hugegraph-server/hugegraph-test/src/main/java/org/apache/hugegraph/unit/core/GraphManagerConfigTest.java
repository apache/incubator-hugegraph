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

package org.apache.hugegraph.unit.core;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.script.ScriptException;
import javax.script.SimpleBindings;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.StandardHugeGraph;
import org.apache.hugegraph.auth.AuthContext;
import org.apache.hugegraph.auth.HugeAuthenticator;
import org.apache.hugegraph.auth.HugeGraphAuthProxy;
import org.apache.hugegraph.auth.HugePermission;
import org.apache.hugegraph.auth.HugeResource;
import org.apache.hugegraph.auth.RolePermission;
import org.apache.hugegraph.backend.store.BackendFeatures;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.config.RpcOptions;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.event.EventListener;
import org.apache.hugegraph.exception.HugeException;
import org.apache.hugegraph.rpc.RpcClientProvider;
import org.apache.hugegraph.rpc.RpcServer;
import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.task.TaskManager;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.Events;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.util.DefaultGraphManager;
import org.mockito.Mockito;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class GraphManagerConfigTest {

    private static ExecutorService previousEventExecutor;
    private static ExecutorService previousTaskDbExecutor;
    private static ExecutorService taskDbExecutor;

    @BeforeClass
    public static void initEventExecutor() {
        // Other suite classes may have destroyed the global EventHub executor.
        previousEventExecutor = Whitebox.getInternalState(EventHub.class, "executor");
        Whitebox.setInternalState(EventHub.class, "executor", null);
        EventHub.init(2);
        previousTaskDbExecutor = Whitebox.getInternalState(TaskManager.instance(), "taskDbExecutor");
        taskDbExecutor = Executors.newSingleThreadExecutor(
                runnable -> new Thread(runnable, "task-db-worker-rpc-config-test"));
        Whitebox.setInternalState(TaskManager.instance(), "taskDbExecutor", taskDbExecutor);
    }

    @AfterClass
    public static void restoreEventExecutor() throws InterruptedException {
        try {
            Assert.assertTrue(EventHub.destroy(5));
        } finally {
            Whitebox.setInternalState(EventHub.class, "executor", previousEventExecutor);
            taskDbExecutor.shutdown();
            try {
                Assert.assertTrue(taskDbExecutor.awaitTermination(5, TimeUnit.SECONDS));
            } finally {
                Whitebox.setInternalState(TaskManager.instance(), "taskDbExecutor", previousTaskDbExecutor);
            }
        }
    }

    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void testAuthenticatedRpcCleanupPreservesOwnerUntilAdmin() throws Exception {
        OptionSpace.register("core", CoreOptions.instance());
        OptionSpace.register("rpc", RpcOptions.instance());
        OptionSpace.register("server", ServerOptions.instance());
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty(ServerOptions.GRAPHS.name(), this.temporary.newFolder("auth").getPath());
        properties.setProperty(ServerOptions.USE_PD.name(), false);
        properties.setProperty(ServerOptions.GRAPH_LOAD_FROM_LOCAL_CONFIG.name(), false);
        properties.setProperty(ServerOptions.ENABLE_DYNAMIC_CREATE_DROP.name(), true);
        properties.setProperty(RpcOptions.RPC_SERVER_HOST.name(), "127.0.0.1");
        properties.setProperty(RpcOptions.RPC_SERVER_PORT.name(), 0);
        try (ServerSocket reservation = new ServerSocket(0)) {
            properties.setProperty(RpcOptions.RPC_REMOTE_URL.name(), "127.0.0.1:" + reservation.getLocalPort());
        }
        EventHub hub = new EventHub("authenticated-rpc-cleanup");
        GraphManager manager = new GraphManager(new HugeConfig(properties), hub);
        String originalThreadName = Thread.currentThread().getName();
        AuthContext.setContext(HugeAuthenticator.User.ADMIN.toJson());
        TaskManager.setContext(HugeAuthenticator.User.ADMIN.toJson());
        try {
            String factory = "gremlin.graph=" + AuthSharedMemoryGraph.class.getName() + "\nbackend=memory\nstore=";
            manager.createGraphLocal("auth_startup", factory + "auth_startup\n");
            Whitebox.invoke(GraphManager.class, "startRpcServer", manager);
            HugeGraph owner = manager.createGraphLocal("auth_late", factory + "auth_late\n");
            RpcServer server = Whitebox.getInternalState(manager, "rpcServer");
            RpcClientProvider client = Whitebox.getInternalState(manager, "rpcClient");
            assertRpcResources(server, client, 4);
            EventListener rejection = event -> {
                throw new IllegalStateException("reject authenticated candidate");
            };
            hub.listen(Events.GRAPH_CREATE, rejection);
            try {
                Assert.assertThrows(HugeException.class, () -> manager.createGraphLocal(
                        "auth_rejected", factory + "auth_rejected\n"));
            } finally {
                hub.unlisten(Events.GRAPH_CREATE, rejection);
            }
            assertRpcResources(server, client, 4);
            TaskManager.resetContext();
            RolePermission execute = RolePermission.role(owner.graphSpace(), owner.name(), HugePermission.EXECUTE);
            Assert.assertFalse(RolePermission.isAdmin(execute));
            AuthContext.setContext(new HugeAuthenticator.User("reader", execute).toJson());
            SimpleBindings bindings = new SimpleBindings();
            bindings.put("graph", owner);
            GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
            Assert.assertThrows(ScriptException.class, () -> engine.eval("graph.unregisterRpcServices()", bindings),
                                error -> Assert.assertContains("Permission denied", error.getMessage()));
            assertRpcResources(server, client, 4);
            AuthContext.resetContext();
            Assert.assertThrows(IllegalStateException.class, owner::unregisterRpcServices,
                                error -> Assert.assertContains("Missing authentication context", error.getMessage()));
            assertRpcResources(server, client, 4);
            HugeGraphAuthProxy.runAsAdmin(owner::unregisterRpcServices);
            assertRpcResources(server, client, 2);
            AuthContext.setContext(HugeAuthenticator.User.ADMIN.toJson());
            // Use the normal PD-drop branch with explicit admin identity and no placement metadata writes.
            Whitebox.setInternalState(manager, "PDExist", true);
            Map<String, GraphSpace> spaces = Whitebox.getInternalState(manager, "graphSpaces");
            spaces.put("DEFAULT", new GraphSpace("DEFAULT"));
            Thread.currentThread().setName("grpc-auth-cleanup-test");
            manager.dropGraph("DEFAULT", "auth_late", false);
            Assert.assertTrue(owner.closed());
            assertRpcResources(server, client, 2);
        } finally {
            Thread.currentThread().setName(originalThreadName);
            try {
                HugeGraphAuthProxy.runAsAdmin(manager::close);
            } finally {
                HugeGraphAuthProxy.resetContext();
                TaskManager.resetContext();
            }
        }
    }

    @Test
    public void testAuthenticatedPublicationPreservesCallerScope() throws Exception {
        OptionSpace.register("core", CoreOptions.instance());
        OptionSpace.register("rpc", RpcOptions.instance());
        OptionSpace.register("server", ServerOptions.instance());
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty(ServerOptions.GRAPHS.name(), this.temporary.newFolder("lazy").getPath());
        properties.setProperty(ServerOptions.USE_PD.name(), false);
        properties.setProperty(ServerOptions.GRAPH_LOAD_FROM_LOCAL_CONFIG.name(), false);
        properties.setProperty(RpcOptions.RPC_SERVER_HOST.name(), "127.0.0.1");
        properties.setProperty(RpcOptions.RPC_SERVER_PORT.name(), 0);
        try (ServerSocket reservation = new ServerSocket(0)) {
            properties.setProperty(RpcOptions.RPC_REMOTE_URL.name(), "127.0.0.1:" + reservation.getLocalPort());
        }
        GraphManager manager = new GraphManager(new HugeConfig(properties), new EventHub("lazy-auth-rpc"));
        SharedMemoryGraph warm = null;
        try {
            Whitebox.invoke(GraphManager.class, "startRpcServer", manager);
            String space = "rpcscope";
            String name = "lazy_auth";
            PropertiesConfiguration config = new PropertiesConfiguration();
            config.setProperty(CoreOptions.BACKEND.name(), "memory");
            config.setProperty(CoreOptions.STORE.name(), name);
            config.setProperty(CoreOptions.GRAPH_SPACE.name(), space);
            warm = new SharedMemoryGraph(new HugeConfig(config));
            warm.initBackend();
            HugeGraph owner = new HugeGraphAuthProxy(warm);
            RolePermission role = new WriterRole(space, name);
            Assert.assertFalse(RolePermission.isAdmin(role));
            String caller = new HugeAuthenticator.User("writer", role).toJson();
            AuthContext.setContext(caller);
            TaskManager.setContext(caller);
            HugeGraphAuthProxy.setRequestGraphSpace(space);
            Whitebox.invoke(GraphManager.class, new Class<?>[]{String.class, HugeGraph.class},
                            "publishGraph", manager, owner.spaceGraphName(), owner);
            Assert.assertSame(owner, manager.graph(space + "-" + name));
            Assert.assertEquals(caller, AuthContext.getContext());
            Assert.assertEquals(caller, TaskManager.getContext());
            Assert.assertEquals(space, HugeGraphAuthProxy.getRequestGraphSpace());
            Assert.assertEquals("writer", HugeGraphAuthProxy.username());
            RpcServer server = Whitebox.getInternalState(manager, "rpcServer");
            RpcClientProvider client = Whitebox.getInternalState(manager, "rpcClient");
            assertRpcResources(server, client, 2);
            Assert.assertThrows(jakarta.ws.rs.ForbiddenException.class, owner::unregisterRpcServices);
            assertRpcResources(server, client, 2);
        } finally {
            HugeGraph retained = warm;
            HugeGraphAuthProxy.runAsAdmin(() -> {
                manager.close();
                if (retained != null) {
                    try {
                        retained.close();
                    } catch (Exception failure) {
                        throw new IllegalStateException(failure);
                    } finally {
                        HugeFactory.remove(retained);
                    }
                }
            });
            HugeGraphAuthProxy.resetContext();
            TaskManager.resetContext();
        }
    }

    private static class WriterRole extends RolePermission {

        WriterRole(String space, String graph) {
            this.add(space, graph, HugePermission.READ, HugeResource.ALL_RES);
            this.add(space, graph, HugePermission.WRITE, HugeResource.ALL_RES);
        }
    }

    public static class AuthSharedMemoryGraph {

        public static HugeGraph open(Configuration config) {
            return new HugeGraphAuthProxy(new SharedMemoryGraph(new HugeConfig(config)));
        }
    }

    @Test
    public void testLegacyDefaultCleanupDoesNotAbortDropWithoutRpc() throws Exception {
        this.checkLegacyDrop(false, false);
    }

    @Test
    public void testActualCleanupFailureStillPropagates() throws Exception {
        this.checkLegacyDrop(true, false);
    }

    @Test
    public void testClearCleanupFailurePreservesOwnerForRetry() throws Exception {
        this.checkLegacyDrop(true, true);
    }

    @Test
    public void testQuotaUpdateFailurePreservesOwnerAndCountForRetry() throws Exception {
        this.checkLegacyDrop(false, false, true);
    }

    private void checkLegacyDrop(boolean failCleanup, boolean clear) throws Exception {
        this.checkLegacyDrop(failCleanup, clear, false);
    }

    private void checkLegacyDrop(boolean failCleanup, boolean clear, boolean failQuota) throws Exception {
        OptionSpace.register("core", CoreOptions.instance());
        OptionSpace.register("rpc", RpcOptions.instance());
        OptionSpace.register("server", ServerOptions.instance());
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty(ServerOptions.GRAPHS.name(), this.temporary.newFolder("legacy").getPath());
        properties.setProperty(ServerOptions.GRAPH_LOAD_FROM_LOCAL_CONFIG.name(), false);
        properties.setProperty(ServerOptions.USE_PD.name(), false);
        properties.setProperty(ServerOptions.ENABLE_DYNAMIC_CREATE_DROP.name(), true);
        properties.setProperty(RpcOptions.RPC_SERVER_HOST.name(), "");
        properties.setProperty(RpcOptions.RPC_REMOTE_URL.name(), "");
        EventHub hub = new EventHub("legacy-rpc-cleanup-test");
        GraphManager manager = new GraphManager(new HugeConfig(properties), hub);
        HugeGraph delegate = null;
        String originalThreadName = Thread.currentThread().getName();
        try {
            delegate = manager.createGraphLocal("legacy_cleanup", "gremlin.graph=" + HugeFactory.class.getName() +
                                                                  "\nbackend=memory\nstore=legacy_cleanup\n");
            HugeGraph owner = delegate;
            IllegalStateException failure = new IllegalStateException("actual owner cleanup failed");
            AtomicBoolean failing = new AtomicBoolean(failCleanup);
            HugeGraph legacy = (HugeGraph) Proxy.newProxyInstance(HugeGraph.class.getClassLoader(),
                    new Class<?>[]{HugeGraph.class}, (proxy, method, args) -> {
                        if ("unregisterRpcServices".equals(method.getName())) {
                            if (failing.get()) {
                                throw failure;
                            }
                            return InvocationHandler.invokeDefault(proxy, method, args);
                        }
                        try {
                            return method.invoke(owner, args);
                        } catch (InvocationTargetException error) {
                            throw error.getCause();
                        }
                    });
            Map<String, HugeGraph> graphs = Whitebox.getInternalState(manager, "graphs");
            graphs.put(legacy.spaceGraphName(), legacy);
            DefaultGraphManager bindings = new DefaultGraphManager(new Settings());
            bindings.putGraph(legacy.spaceGraphName(), legacy);
            CountDownLatch dropped = new CountDownLatch(1);
            hub.listen(Events.GRAPH_DROP, event -> {
                if (bindings.getGraph(legacy.spaceGraphName()) == legacy) {
                    try {
                        bindings.removeGraph(legacy.spaceGraphName());
                    } catch (Exception failureToUnbind) {
                        throw new IllegalStateException(failureToUnbind);
                    }
                }
                dropped.countDown();
                return true;
            });
            MetaManager meta = Mockito.mock(MetaManager.class);
            Whitebox.setInternalState(manager, "metaManager", meta);
            RpcServer server = Whitebox.getInternalState(manager, "rpcServer");
            RpcClientProvider client = Whitebox.getInternalState(manager, "rpcClient");
            Assert.assertFalse(server.enabled());
            Assert.assertFalse(client.enabled());
            // Reach the normal PD-drop branch without booting PD or writing placement metadata.
            Whitebox.setInternalState(manager, "PDExist", true);
            Map<String, GraphSpace> spaces = Whitebox.getInternalState(manager, "graphSpaces");
            GraphSpace space = new GraphSpace("DEFAULT");
            space.graphNumberUsed(5);
            spaces.put("DEFAULT", space);
            if (failQuota) {
                Mockito.doThrow(failure).doNothing().when(meta).updateGraphSpaceConfig("DEFAULT", space);
            } else {
                Thread.currentThread().setName("grpc-legacy-cleanup-test");
            }
            if (failCleanup || failQuota) {
                Assert.assertThrows(IllegalStateException.class, () -> manager.dropGraph(
                        "DEFAULT", "legacy_cleanup", clear), caught -> Assert.assertSame(failure, caught));
                Assert.assertEquals(!clear, owner.closed());
                Assert.assertSame(legacy, manager.graph(legacy.spaceGraphName()));
                Assert.assertSame(legacy, bindings.getGraph(legacy.spaceGraphName()));
                Map<?, ?> factory = Whitebox.getInternalState(HugeFactory.class, "GRAPHS");
                Assert.assertSame(owner, factory.get(legacy.spaceGraphName()));
                Assert.assertEquals(1L, dropped.getCount());
                if (failCleanup) {
                    Mockito.verifyNoInteractions(meta);
                }
                Assert.assertEquals(5, space.graphNumberUsed());
                failing.set(false);
            }
            manager.dropGraph("DEFAULT", "legacy_cleanup", clear);
            Assert.assertTrue(dropped.await(5, TimeUnit.SECONDS));
            Assert.assertTrue(owner.closed());
            Map<?, ?> factory = Whitebox.getInternalState(HugeFactory.class, "GRAPHS");
            Assert.assertFalse(factory.containsKey(legacy.spaceGraphName()));
            Assert.assertNull(bindings.getGraph(legacy.spaceGraphName()));
            Assert.assertFalse(manager.graphs().contains(legacy.spaceGraphName()));
            if (failQuota) {
                Assert.assertEquals(4, space.graphNumberUsed());
                Mockito.verify(meta, Mockito.times(2)).updateGraphSpaceConfig("DEFAULT", space);
            }
        } finally {
            Thread.currentThread().setName(originalThreadName);
            manager.close();
            if (delegate != null) {
                delegate.close();
                HugeFactory.remove(delegate);
            }
        }
    }

    @Test
    public void testLateGraphRpcPublicationRollbackAndReplacement() throws Exception {
        OptionSpace.register("core", CoreOptions.instance());
        OptionSpace.register("rpc", RpcOptions.instance());
        OptionSpace.register("server", ServerOptions.instance());
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty(ServerOptions.GRAPHS.name(), this.temporary.newFolder("graphs").getPath());
        properties.setProperty(ServerOptions.USE_PD.name(), false);
        properties.setProperty(ServerOptions.GRAPH_LOAD_FROM_LOCAL_CONFIG.name(), false);
        properties.setProperty(ServerOptions.ENABLE_DYNAMIC_CREATE_DROP.name(), true);
        properties.setProperty(RpcOptions.RPC_SERVER_HOST.name(), "127.0.0.1");
        properties.setProperty(RpcOptions.RPC_SERVER_PORT.name(), 0);
        try (ServerSocket reservation = new ServerSocket(0)) {
            properties.setProperty(RpcOptions.RPC_REMOTE_URL.name(), "127.0.0.1:" + reservation.getLocalPort());
        }
        EventHub hub = new EventHub("rpc-publication-test");
        GraphManager manager = new GraphManager(new HugeConfig(properties), hub);
        BarrierRpcServer server = new BarrierRpcServer(new HugeConfig(properties));
        Whitebox.setInternalState(manager, "rpcServer", server);
        RpcClientProvider client = Whitebox.getInternalState(manager, "rpcClient");
        ExecutorService initializer = Executors.newFixedThreadPool(2);
        HugeGraph startup = null;
        HugeGraph replacement = null;
        try {
            startup = manager.createGraphLocal("rpc_startup", sharedMemoryConfig("rpc_startup"));
            Assert.assertEquals(0, server.config().configs().size());
            HugeGraph startupOwner = startup;
            AtomicReference<Thread> initThread = new AtomicReference<>();
            Future<?> initialization = initializer.submit(() -> {
                initThread.set(Thread.currentThread());
                try {
                    Whitebox.invoke(manager.getClass(), "startRpcServer", manager);
                } finally {
                    Whitebox.invoke(StandardHugeGraph.class, "closeCurrentThreadTransaction", startupOwner);
                }
            });
            Assert.assertTrue(server.entered.await(5, TimeUnit.SECONDS));
            Assert.assertEquals(2, server.config().configs().size());
            AtomicReference<Thread> publisher = new AtomicReference<>();
            Future<HugeGraph> publication = initializer.submit(() -> {
                publisher.set(Thread.currentThread());
                HugeGraph graph = manager.createGraphLocal("rpc_late", sharedMemoryConfig("rpc_late"));
                Whitebox.invoke(StandardHugeGraph.class, "closeCurrentThreadTransaction", graph);
                return graph;
            });
            awaitManagerGate(publisher, initThread.get());
            Assert.assertFalse(manager.graphs().contains("DEFAULT-rpc_late"));
            assertRpcResources(server, client, 2);
            server.release.countDown();
            initialization.get(5, TimeUnit.SECONDS);
            HugeGraph original = publication.get(5, TimeUnit.SECONDS);
            Assert.assertEquals(2, server.exports.get());
            assertRpcResources(server, client, 4);

            EventListener failure = event -> {
                throw new IllegalStateException("reject candidate after RPC registration");
            };
            hub.listen(Events.GRAPH_CREATE, failure);
            try {
                Assert.assertThrows(HugeException.class, () -> manager.createGraphLocal(
                        "rpc_rejected", sharedMemoryConfig("rpc_rejected")));
            } finally {
                hub.unlisten(Events.GRAPH_CREATE, failure);
            }
            Assert.assertFalse(manager.graphs().contains("DEFAULT-rpc_rejected"));
            assertRpcResources(server, client, 4);

            manager.dropGraphLocal("rpc_late");
            assertRpcResources(server, client, 2);
            replacement = manager.createGraphLocal("rpc_late", sharedMemoryConfig("rpc_late"));
            hub.notifySync(Events.GRAPH_DROP, original);
            Assert.assertSame(replacement, manager.graph("DEFAULT-rpc_late"));
            assertRpcResources(server, client, 4);
            manager.close();
            List<?> bootstraps = Whitebox.getInternalState(client.config(), "bootstraps");
            Assert.assertEquals(0, server.config().configs().size());
            Assert.assertEquals(0, bootstraps.size());
        } finally {
            server.release.countDown();
            initializer.shutdownNow();
            initializer.awaitTermination(5, TimeUnit.SECONDS);
            manager.close();
            if (startup != null) {
                HugeFactory.remove(startup);
            }
            if (replacement != null) {
                HugeFactory.remove(replacement);
            }
        }
    }

    private static void awaitManagerGate(AtomicReference<Thread> publisher, Thread initializer) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            Thread thread = publisher.get();
            ThreadInfo info = thread == null ? null : ManagementFactory.getThreadMXBean().getThreadInfo(thread.getId());
            if (info != null && info.getThreadState() == Thread.State.BLOCKED &&
                info.getLockOwnerId() == initializer.getId()) {
                Assert.assertContains(GraphManager.class.getName(), info.getLockName());
                return;
            }
            Thread.sleep(5);
        }
        Assert.fail("Graph publisher did not block on the startup manager gate");
    }

    private static class BarrierRpcServer extends RpcServer {

        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicInteger exports = new AtomicInteger();

        BarrierRpcServer(HugeConfig config) {
            super(config);
        }

        @Override
        public void exportAll() {
            if (this.exports.incrementAndGet() == 1) {
                this.entered.countDown();
                try {
                    E.checkState(this.release.await(5, TimeUnit.SECONDS), "RPC export barrier timed out");
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(failure);
                }
            }
            super.exportAll();
        }
    }

    private static void assertRpcResources(RpcServer server, RpcClientProvider client, int count) {
        Assert.assertEquals(count, server.config().configs().size());
        Map<?, ?> configs = Whitebox.getInternalState(client.config(), "configs");
        List<?> bootstraps = Whitebox.getInternalState(client.config(), "bootstraps");
        Assert.assertEquals(count, configs.size());
        Assert.assertEquals(count, bootstraps.size());
    }

    private static String sharedMemoryConfig(String name) {
        return "gremlin.graph=" + SharedMemoryGraph.class.getName() + "\nbackend=memory\nstore=" + name + "\n";
    }

    // Exercises cache RPC registration, not shared-backend DML or consistency.
    public static class SharedMemoryGraph extends StandardHugeGraph {

        public SharedMemoryGraph(HugeConfig config) {
            super(config);
        }

        public static SharedMemoryGraph open(Configuration config) {
            return new SharedMemoryGraph(new HugeConfig(config));
        }

        @Override
        public BackendFeatures backendStoreFeatures() {
            BackendFeatures features = super.backendStoreFeatures();
            return (BackendFeatures) Proxy.newProxyInstance(BackendFeatures.class.getClassLoader(),
                    new Class<?>[]{BackendFeatures.class}, (proxy, method, args) ->
                    "supportsSharedStorage".equals(method.getName()) ? true : method.invoke(features, args));
        }
    }

    @Test
    public void testAttachExplicitLocalResourceLimits() {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty(CoreOptions.SCHEMA_CACHE_CAPACITY.name(), 1000L);
        properties.setProperty(
                CoreOptions.SERIALIZER_BUFFER_MAX_CAPACITY.name(), 8388608);
        GraphManager manager = newManager(properties);

        try {
            Map<String, Object> attached = attach(manager, new HashMap<>());

            Assert.assertEquals(1000L, attached.get(
                    CoreOptions.SCHEMA_CACHE_CAPACITY.name()));
            Assert.assertEquals(8388608, attached.get(
                    CoreOptions.SERIALIZER_BUFFER_MAX_CAPACITY.name()));
        } finally {
            manager.close();
        }
    }

    @Test
    public void testKeepIncomingResourceLimits() {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty(CoreOptions.SCHEMA_CACHE_CAPACITY.name(), 1000L);
        properties.setProperty(
                CoreOptions.SERIALIZER_BUFFER_MAX_CAPACITY.name(), 8388608);
        GraphManager manager = newManager(properties);
        Map<String, Object> configs = new HashMap<>();
        configs.put(CoreOptions.SCHEMA_CACHE_CAPACITY.name(), 2000L);
        configs.put(CoreOptions.SERIALIZER_BUFFER_MAX_CAPACITY.name(),
                    16777216);

        try {
            Map<String, Object> attached = attach(manager, configs);

            Assert.assertEquals(2000L, attached.get(
                    CoreOptions.SCHEMA_CACHE_CAPACITY.name()));
            Assert.assertEquals(16777216, attached.get(
                    CoreOptions.SERIALIZER_BUFFER_MAX_CAPACITY.name()));
        } finally {
            manager.close();
        }
    }

    @Test
    public void testAttachDoesNotMutateSourceOrCopySensitiveOptions() {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty(CoreOptions.SCHEMA_CACHE_CAPACITY.name(), 1000L);
        properties.setProperty(
                CoreOptions.SERIALIZER_BUFFER_MAX_CAPACITY.name(), 8388608);
        properties.setProperty(CoreOptions.BACKEND.name(), "memory");
        properties.setProperty(CoreOptions.STORE.name(), "local_store");
        properties.setProperty(ServerOptions.ADMIN_PA.name(), "local_secret");
        GraphManager manager = newManager(properties);
        Map<String, Object> configs = new HashMap<>();
        configs.put("marker", "source");
        Map<String, Object> source = new HashMap<>(configs);

        try {
            Map<String, Object> attached = attach(manager, configs);

            Assert.assertEquals(source, configs);
            Assert.assertEquals("source", attached.get("marker"));
            Assert.assertFalse(attached.containsKey(CoreOptions.BACKEND.name()));
            Assert.assertFalse(attached.containsKey(CoreOptions.STORE.name()));
            Assert.assertFalse(attached.containsKey(ServerOptions.ADMIN_PA.name()));
        } finally {
            manager.close();
        }
    }

    @Test
    public void testDoNotAttachImplicitLocalDefaults() {
        GraphManager manager = newManager(new PropertiesConfiguration());

        try {
            Map<String, Object> attached = attach(manager, new HashMap<>());

            Assert.assertFalse(attached.containsKey(
                    CoreOptions.SCHEMA_CACHE_CAPACITY.name()));
            Assert.assertFalse(attached.containsKey(
                    CoreOptions.SERIALIZER_BUFFER_MAX_CAPACITY.name()));
        } finally {
            manager.close();
        }
    }

    @Test
    public void testDoNotAttachResourceLimitsToAliasGraph() {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty(CoreOptions.SCHEMA_CACHE_CAPACITY.name(), 1000L);
        properties.setProperty(
                CoreOptions.SERIALIZER_BUFFER_MAX_CAPACITY.name(), 8388608);
        GraphManager manager = newManager(properties);
        Map<String, Object> configs = new HashMap<>();
        configs.put(CoreOptions.ALIAS_NAME.name(), "source_graph");
        Map<String, Object> source = new HashMap<>(configs);

        try {
            Map<String, Object> attached = attach(manager, configs);

            Assert.assertEquals(source, attached);
            Assert.assertEquals(source, configs);
        } finally {
            manager.close();
        }
    }

    private static GraphManager newManager(PropertiesConfiguration properties) {
        return new GraphManager(new HugeConfig(properties),
                                new EventHub("config-test"));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> attach(GraphManager manager,
                                               Map<String, Object> configs) {
        return Whitebox.invoke(manager.getClass(), new Class<?>[]{Map.class},
                               "attachLocalCacheConfig", manager, configs);
    }
}
