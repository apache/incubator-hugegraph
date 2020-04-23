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

package com.baidu.hugegraph.auth;

import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.ws.rs.ForbiddenException;

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.slf4j.Logger;

import com.baidu.hugegraph.GremlinGraph;
import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.auth.HugeAuthenticator.RoleAction;
import com.baidu.hugegraph.auth.HugeAuthenticator.RolePerm;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeFeatures;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Action;
import com.baidu.hugegraph.type.define.GraphMode;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public class HugeGraphAuthProxy implements GremlinGraph {

    static {
        HugeGraph.registerTraversalStrategies(HugeGraphAuthProxy.class);
    }

    private static final Logger LOG = Log.logger(HugeGraph.class);

    private final HugeGraph hugegraph;
    private final Transaction tx;

    public HugeGraphAuthProxy(HugeGraph hugegraph) {
        LOG.info("Wrap graph '{}' with HugeGraphAuthProxy", hugegraph.name());
        this.hugegraph = hugegraph;
        this.tx = new TransactionProxy(hugegraph.tx());
    }

    @Override
    public HugeGraph hugegraph(String action) {
        this.verifyPermissionAction(action);
        return this.hugegraph;
    }

    @Override
    public HugeGraph hugegraph() {
        this.verifyPermission(HugeAuthenticator.ROLE_ADMIN);
        return this.hugegraph;
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> clazz)
                                               throws IllegalArgumentException {
        this.verifyPermission();
        return this.hugegraph.compute(clazz);
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        this.verifyPermission();
        return this.hugegraph.compute();
    }

    @Override
    public GraphTraversalSource traversal() {
        verifyPermissionAction(HugePermission.GREMLIN);
        return new GraphTraversalSourceProxy(this);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public <I extends Io> I io(final Io.Builder<I> builder) {
        this.verifyPermission();
        return this.hugegraph.io(builder);
    }

    @Override
    public SchemaManager schema() {
        this.verifyPermissionAction(HugePermission.SCHEMA_READ);
        this.verifyPermissionAction(HugePermission.SCHEMA_WRITE);
        this.verifyPermissionAction(HugePermission.SCHEMA_DELETE);
        return this.hugegraph.schema();
    }

    @Override
    public PropertyKey propertyKey(String key) {
        this.verifyPermissionAction(HugePermission.SCHEMA_READ);
        // TODO: fix call PropertyKey.graph().remove()
        return this.hugegraph.propertyKey(key);
    }

    @Override
    public PropertyKey propertyKey(Id key) {
        this.verifyPermissionAction(HugePermission.SCHEMA_READ);
        return this.hugegraph.propertyKey(key);
    }

    @Override
    public VertexLabel vertexLabel(String label) {
        this.verifyPermissionAction(HugePermission.SCHEMA_READ);
        return this.hugegraph.vertexLabel(label);
    }

    @Override
    public VertexLabel vertexLabel(Id label) {
        this.verifyPermissionAction(HugePermission.SCHEMA_READ);
        return this.hugegraph.vertexLabel(label);
    }

    @Override
    public EdgeLabel edgeLabel(String label) {
        this.verifyPermissionAction(HugePermission.SCHEMA_READ);
        return this.hugegraph.edgeLabel(label);
    }

    @Override
    public EdgeLabel edgeLabel(Id label) {
        this.verifyPermissionAction(HugePermission.SCHEMA_READ);
        return this.hugegraph.edgeLabel(label);
    }

    @Override
    public IndexLabel indexLabel(String label) {
        this.verifyPermissionAction(HugePermission.SCHEMA_READ);
        return this.hugegraph.indexLabel(label);
    }

    @Override
    public IndexLabel indexLabel(Id label) {
        this.verifyPermissionAction(HugePermission.SCHEMA_READ);
        return this.hugegraph.indexLabel(label);
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        this.verifyPermissionAction(HugePermission.VERTEX_WRITE);
        return this.hugegraph.addVertex(keyValues);
    }

    @Override
    public Iterator<Vertex> vertices(Query query) {
        this.verifyPermissionAction(HugePermission.VERTEX_READ);
        return this.hugegraph.vertices(query);
    }

    @Override
    public Iterator<Vertex> vertices(Object... objects) {
        this.verifyPermissionAction(HugePermission.VERTEX_READ);
        return this.hugegraph.vertices(objects);
    }

    @Override
    public Iterator<Edge> edges(Query query) {
        this.verifyPermissionAction(HugePermission.EDGE_READ);
        return this.hugegraph.edges(query);
    }

    @Override
    public Iterator<Edge> edges(Object... objects) {
        this.verifyPermissionAction(HugePermission.EDGE_READ);
        return this.hugegraph.edges(objects);
    }

    @Override
    public Iterator<Vertex> adjacentVertices(Iterator<Edge> edges) {
        this.verifyPermissionAction(HugePermission.VERTEX_READ);
        return this.hugegraph.adjacentVertices(edges);
    }

    @Override
    public Iterator<Edge> adjacentEdges(Id vertexId) {
        this.verifyPermissionAction(HugePermission.EDGE_READ);
        return this.hugegraph.adjacentEdges(vertexId);
    }

    @Override
    public Number queryNumber(Query query) {
        if (query.resultType().isVertex()) {
            this.verifyPermissionAction(HugePermission.VERTEX_READ);
        } else {
            assert query.resultType().isEdge();
            this.verifyPermissionAction(HugePermission.EDGE_READ);
        }
        return this.hugegraph.queryNumber(query);

    }

    @Override
    public Transaction tx() {
        // Can't verifyPermission() here, will be called by rollbackAll()
        return this.tx;
    }

    @Override
    public void close() throws HugeException {
        this.verifyPermission(HugeAuthenticator.ROLE_ADMIN);
        this.hugegraph.close();
    }

    @Override
    public HugeFeatures features() {
        // Can't verifyPermission() here, will be called by rollbackAll()
        return this.hugegraph.features();
    }

    @Override
    public Variables variables() {
        this.verifyPermission();
        return new VariablesProxy(this.hugegraph.variables());
    }

    @Override
    public HugeConfig configuration() {
        throw new NotSupportException("Graph.configuration()");
    }

    @Override
    public String toString() {
        this.verifyPermission();
        return this.hugegraph.toString();
    }

    @Override
    public String name() {
        this.verifyPermission();
        return this.hugegraph.name();
    }

    @Override
    public GraphMode mode() {
        this.verifyPermission();
        return this.hugegraph.mode();
    }

    @Override
    public <R> R metadata(HugeType type, String meta, Object... args) {
        // TODO: deal with META_WRITE
        this.verifyPermissionAction(HugePermission.META_READ);
        return this.hugegraph.metadata(type, meta, args);
    }

    @Override
    public String matchUser(String username, String password) {
        // Can't verifyPermission() here, login first
        return this.hugegraph.matchUser(username, password);
    }

    @Override
    public UserManager userManager() {
        this.verifyPermission();
        return this.hugegraph.userManager();
    }

    @Override
    public String backend() {
        this.verifyPermission();
        return this.hugegraph.backend();
    }

    @Override
    public void initBackend() {
        this.verifyPermission(HugeAuthenticator.ROLE_ADMIN);
        this.hugegraph.initBackend();
    }

    @Override
    public void clearBackend() {
        this.verifyPermission(HugeAuthenticator.ROLE_ADMIN);
        this.hugegraph.clearBackend();
    }

    @Override
    public void truncateBackend() {
        this.verifyPermission(HugeAuthenticator.ROLE_ADMIN);
        this.hugegraph.truncateBackend();
    }

    private void verifyPermission() {
        /*
         * The owner role should match the graph name
         * NOTE: the graph names in gremlin-server.yaml/graphs and
         * hugegraph.properties/store must be the same if enable auth.
         */
        String owner = this.hugegraph.name();
        this.verifyPermission(owner);
    }

    private void verifyPermissionAction(HugePermission permission) {
        this.verifyPermissionAction(permission.string());
    }

    private void verifyPermissionAction(String action) {
        String owner = this.hugegraph.name();
        this.verifyPermission(RoleAction.ownerFor(owner, action));
    }

    private void verifyPermission(String permission) {
        Context context = getContext();
        E.checkState(context != null,
                     "Missing authentication context " +
                     "when accessing a Graph with permission control");
        String role = context.user().role();
        if (!RolePerm.match(role, permission)) {
            throw new ForbiddenException("Permission denied");
        }
    }

    private class TransactionProxy implements Transaction {

        private final Transaction origin;

        public TransactionProxy(Transaction origin) {
            E.checkNotNull(origin, "origin");
            this.origin = origin;
        }

        @Override
        public void open() {
            this.origin.open();
        }

        @Override
        public void commit() {
            if (this.hasUpdate(HugeType.EDGE, Action.INSERT)) {
                verifyPermissionAction(HugePermission.EDGE_WRITE);
            }
            if (this.hasUpdate(HugeType.EDGE, Action.DELETE)) {
                verifyPermissionAction(HugePermission.EDGE_WRITE);
            }
            if (this.hasUpdate(HugeType.VERTEX, Action.INSERT)) {
                verifyPermissionAction(HugePermission.VERTEX_WRITE);
            }
            if (this.hasUpdate(HugeType.VERTEX, Action.DELETE)) {
                verifyPermissionAction(HugePermission.VERTEX_DELETE);
            }
            this.origin.commit();
        }

        private boolean hasUpdate(HugeType type, Action action) {
            Object txs = Whitebox.invoke(this, "origin", "getOrNewTransaction");
            return (boolean) Whitebox.invoke(txs, "graphTx", "hasUpdate",
                                             type, action);
        }

        @Override
        public void rollback() {
            this.origin.rollback();
        }

        @Override
        public <G extends Graph> G createThreadedTx() {
            return this.origin.createThreadedTx();
        }

        @Override
        public boolean isOpen() {
            return this.origin.isOpen();
        }

        @Override
        public void readWrite() {
            this.origin.readWrite();
        }

        @Override
        public void close() {
            this.origin.close();
        }

        @Override
        public Transaction onReadWrite(Consumer<Transaction> consumer) {
            return this.origin.onReadWrite(consumer);
        }

        @Override
        public Transaction onClose(Consumer<Transaction> consumer) {
            return this.origin.onClose(consumer);
        }

        @Override
        public void addTransactionListener(Consumer<Status> listener) {
            this.origin.addTransactionListener(listener);
        }

        @Override
        public void removeTransactionListener(Consumer<Status> listener) {
            this.origin.removeTransactionListener(listener);
        }

        @Override
        public void clearTransactionListeners() {
            this.origin.clearTransactionListeners();
        }
    }

    private class GraphTraversalSourceProxy extends GraphTraversalSource {

        public GraphTraversalSourceProxy(Graph graph) {
            super(graph);
        }

        @Override
        public Graph getGraph() {
            // Be called by GraphTraversalSource clone
            verifyPermissionAction(HugePermission.GREMLIN);
            return this.graph;
        }
    }

    private class VariablesProxy implements Variables {

        private final Variables variables;

        public VariablesProxy(Variables variables) {
            this.variables = variables;
        }

        @Override
        public <R> Optional<R> get(String key) {
            verifyPermissionAction(HugePermission.VAR_READ);
            return this.variables.get(key);
        }

        @Override
        public Set<String> keys() {
            verifyPermissionAction(HugePermission.VAR_READ);
            return this.variables.keys();
        }

        @Override
        public void set(String key, Object value) {
            verifyPermissionAction(HugePermission.VAR_WRITE);
            this.variables.set(key, value);
        }

        @Override
        public void remove(String key) {
            verifyPermissionAction(HugePermission.VAR_DELETE);
            this.variables.remove(key);
        }
    }

    private static final ThreadLocal<Context> contexts = new ThreadLocal<>();

    public static void setContext(Context context) {
        contexts.set(context);
    }

    public static Context getContext() {
        return contexts.get();
    }

    public static void resetContext() {
        contexts.remove();
    }

    public static class Context {

        private static final Context ADMIN =
                             new Context(HugeAuthenticator.User.ADMIN);

        private final HugeAuthenticator.User user;

        public Context(HugeAuthenticator.User user) {
            this.user = user;
        }

        public HugeAuthenticator.User user() {
            return this.user;
        }

        public static Context admin() {
            return ADMIN;
        }
    }

    public static class ContextThreadPoolExecutor extends ThreadPoolExecutor {

        public ContextThreadPoolExecutor(int corePoolSize, int maxPoolSize,
                                         ThreadFactory threadFactory) {
            super(corePoolSize, maxPoolSize, 0L, TimeUnit.MILLISECONDS,
                  new LinkedBlockingQueue<>(), threadFactory);
        }

        @Override
        public void execute(Runnable command) {
            super.execute(new ContextTask(command));
        }
    }

    public static class ContextTask implements Runnable {

        private final Runnable runner;
        private final Context context;

        public ContextTask(Runnable runner) {
            this.context = HugeGraphAuthProxy.getContext();
            this.runner = runner;
        }

        @Override
        public void run() {
            HugeGraphAuthProxy.setContext(this.context);
            try {
                this.runner.run();
            } finally {
                HugeGraphAuthProxy.resetContext();
            }
        }
    }
}
