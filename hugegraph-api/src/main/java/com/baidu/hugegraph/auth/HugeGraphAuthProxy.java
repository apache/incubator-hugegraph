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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.ForbiddenException;

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.structure.HugeFeatures;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public class HugeGraphAuthProxy implements Graph {

    private static final Logger LOG = Log.logger(HugeGraph.class);

    private final HugeGraph hugegraph;

    public HugeGraphAuthProxy(HugeGraph hugegraph) {
        LOG.info("Wrap graph '{}' with HugeGraphAuthProxy", hugegraph.name());
        this.hugegraph = hugegraph;
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        this.verifyPermission();
        return this.hugegraph.addVertex(keyValues);
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
        this.verifyPermission();
        return new GraphTraversalSourceProxy(this.hugegraph);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public <I extends Io> I io(final Io.Builder<I> builder) {
        this.verifyPermission();
        return this.hugegraph.io(builder);
    }

    @Override
    public Iterator<Vertex> vertices(Object... objects) {
        this.verifyPermission();
        return this.hugegraph.vertices(objects);
    }

    @Override
    public Iterator<Edge> edges(Object... objects) {
        this.verifyPermission();
        return this.hugegraph.edges(objects);
    }

    @Override
    public Transaction tx() {
        // Can't verifyPermission() here, will be called by rollbackAll()
        return this.hugegraph.tx();
    }

    @Override
    public void close() throws HugeException {
        this.verifyPermission("admin");
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
        return this.hugegraph.variables();
    }

    @Override
    public HugeConfig configuration() {
        throw new NotSupportException("Graph.configuration()");
    }

    @Override
    public String toString() {
        this.verifyPermission();
        return this.hugegraph.name();
    }

    public HugeGraph graph() {
        this.verifyPermission();
        return this.hugegraph;
    }

    public HugeGraph hugegraph() {
        this.verifyPermission(StandardAuthenticator.ROLE_ADMIN);
        return this.hugegraph;
    }

    public SchemaManager schema() {
        this.verifyPermission();
        return this.hugegraph.schema();
    }

    public void initBackend() {
        this.verifyPermission(StandardAuthenticator.ROLE_ADMIN);
        this.hugegraph.initBackend();
    }

    public void clearBackend() {
        this.verifyPermission(StandardAuthenticator.ROLE_ADMIN);
        this.hugegraph.clearBackend();
    }

    private void verifyPermission() {
        // Should match owner role
        this.verifyPermission(this.hugegraph.name());
    }

    private void verifyPermission(String permission) {
        Context context = getContext();
        E.checkState(context != null,
                     "Missing authentication context " +
                     "when accessing a Graph with permission control");
        if (!context.role().equals(StandardAuthenticator.ROLE_ADMIN) &&
            !context.role().equals(permission)) {
            throw new ForbiddenException("Permission denied");
        }
    }

    private class GraphTraversalSourceProxy extends GraphTraversalSource {

        public GraphTraversalSourceProxy(Graph graph) {
            super(graph);
        }

        @Override
        public Graph getGraph() {
            verifyPermission();
            return this.graph;
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

        private final String username;
        private final String role;

        public Context(String username, String role) {
            this.username = username;
            this.role = role;
        }

        public String username() {
            return this.username;
        }

        public String role() {
            return this.role;
        }

        private static final Context ADMIN;

        static {
            ADMIN = new Context("admin", StandardAuthenticator.ROLE_ADMIN);
        }

        public static Context admin() {
            return ADMIN;
        }
    }

    public static class ContextThreadPoolExecutor extends ThreadPoolExecutor {

        public ContextThreadPoolExecutor(int corePoolSize, int maxPoolSize,
                                         ThreadFactory threadFactory) {
            super(corePoolSize, maxPoolSize, 0L, TimeUnit.MILLISECONDS,
                  new LinkedBlockingQueue<Runnable>(), threadFactory);
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
