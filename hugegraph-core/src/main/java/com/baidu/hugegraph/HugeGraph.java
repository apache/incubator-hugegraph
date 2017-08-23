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

package com.baidu.hugegraph;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadedTransaction;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.cache.CachedGraphTransaction;
import com.baidu.hugegraph.backend.cache.CachedSchemaTransaction;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.serializer.AbstractSerializer;
import com.baidu.hugegraph.backend.serializer.SerializerFactory;
import com.baidu.hugegraph.backend.store.BackendProviderFactory;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.BackendStoreProvider;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.event.EventHub;
import com.baidu.hugegraph.io.HugeGraphIoRegistry;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeFeatures;
import com.baidu.hugegraph.traversal.optimize.HugeGraphStepStrategy;
import com.baidu.hugegraph.traversal.optimize.HugeVertexStepStrategy;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.LockUtil;
import com.baidu.hugegraph.util.Log;

/**
 * HugeGraph is the entrance of the graph system, you can modify or query
 * the schema/vertex/edge data through this class.
 */
public class HugeGraph implements Graph {

    private static final Logger LOG = Log.logger(HugeGraph.class);

    static {
        TraversalStrategies strategies = null;
        strategies = TraversalStrategies.GlobalCache.getStrategies(Graph.class)
                                        .clone();
        strategies.addStrategies(HugeVertexStepStrategy.instance(),
                                 HugeGraphStepStrategy.instance());
        TraversalStrategies.GlobalCache.registerStrategies(HugeGraph.class,
                                                           strategies);
    }

    static {
        LockUtil.init();
    }

    private String name;
    private EventHub schemaEventHub;
    private HugeFeatures features;
    private HugeConfig configuration;

    // Store provider like Cassandra
    private BackendStoreProvider storeProvider;

    // Default transactions
    private ThreadLocal<GraphTransaction> graphTransaction;
    private ThreadLocal<SchemaTransaction> schemaTransaction;

    public HugeGraph(HugeConfig configuration) {
        this.configuration = configuration;

        this.schemaEventHub = new EventHub("schema");
        this.features = new HugeFeatures(this, true);

        this.name = configuration.get(CoreOptions.STORE);

        this.graphTransaction = new ThreadLocal<>();
        this.schemaTransaction = new ThreadLocal<>();

        this.storeProvider = null;

        try {
            this.initTransaction();
        } catch (BackendException e) {
            String message = "Failed to init backend store";
            LOG.error("{}: {}", message, e.getMessage());
            throw new HugeException(message);
        }
    }

    private synchronized void initTransaction() throws HugeException {
        if (this.storeProvider == null) {
            String backend = this.configuration.get(CoreOptions.BACKEND);
            LOG.info("Opening backend store: '{}'", backend);
            this.storeProvider = BackendProviderFactory.open(backend,
                                                             this.name);
        }

        SchemaTransaction schemaTx = this.openSchemaTransaction();
        GraphTransaction graphTx = this.openGraphTransaction();

        schemaTx.autoCommit(true);
        graphTx.autoCommit(true);

        this.schemaTransaction.set(schemaTx);
        this.graphTransaction.set(graphTx);
    }

    private void destroyTransaction() {
        GraphTransaction graphTx = this.graphTransaction.get();
        if (graphTx != null) {
            try {
                graphTx.close();
            } catch (Exception e) {
                LOG.error("Failed to close GraphTransaction", e);
            }
        }

        SchemaTransaction schemaTx = this.schemaTransaction.get();
        if (schemaTx != null) {
            try {
                schemaTx.close();
            } catch (Exception e) {
                LOG.error("Failed to close SchemaTransaction", e);
            }
        }

        this.graphTransaction.remove();
        this.schemaTransaction.remove();
    }

    public String name() {
        return this.name;
    }

    public EventHub schemaEventHub() {
        return this.schemaEventHub;
    }

    public void initBackend() {
        this.storeProvider.init();
    }

    public void clearBackend() {
        this.storeProvider.clear();
    }

    private SchemaTransaction openSchemaTransaction() throws HugeException {
        try {
            String name = this.configuration.get(CoreOptions.STORE_SCHEMA);
            BackendStore store = this.storeProvider.loadSchemaStore(name);
            return new CachedSchemaTransaction(this, store);
        } catch (BackendException e) {
            String message = "Failed to open schema transaction";
            LOG.error("{}: {}", message, e.getMessage());
            throw new HugeException(message);
        }
    }

    private GraphTransaction openGraphTransaction() throws HugeException {
        try {
            String graph = this.configuration.get(CoreOptions.STORE_GRAPH);
            BackendStore store = this.storeProvider.loadGraphStore(graph);

            String index = this.configuration.get(CoreOptions.STORE_INDEX);
            BackendStore indexStore = this.storeProvider.loadIndexStore(index);

            return new CachedGraphTransaction(this, store, indexStore);
        } catch (BackendException e) {
            String message = "Failed to open graph transaction";
            LOG.error("{}: {}", message, e.getMessage());
            throw new HugeException(message);
        }
    }

    public SchemaTransaction schemaTransaction() {
        SchemaTransaction schemaTx = this.schemaTransaction.get();
        if (schemaTx == null) {
            this.initTransaction();
            schemaTx = this.schemaTransaction.get();
        }
        return schemaTx;
    }

    public GraphTransaction graphTransaction() {
        GraphTransaction graphTx = this.graphTransaction.get();
        if (graphTx == null) {
            this.initTransaction();
            graphTx = this.graphTransaction.get();
        }
        return graphTx;
    }

    public SchemaManager schema() {
        return new SchemaManager(this.schemaTransaction());
    }

    public GraphTransaction openTransaction() {
        return this.openGraphTransaction();
    }

    public AbstractSerializer serializer() {
        String name = this.configuration.get(CoreOptions.SERIALIZER);
        AbstractSerializer serializer = SerializerFactory.serializer(name);
        if (serializer == null) {
            throw new HugeException("Can't load serializer with name " + name);
        }
        return serializer;
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        return this.graphTransaction().addVertex(keyValues);
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> aClass)
            throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public <I extends Io> I io(final Io.Builder<I> builder) {
        return (I) builder.graph(this).onMapper(mapper ->
                mapper.addRegistry(HugeGraphIoRegistry.getInstance()))
                .create();
    }

    @Override
    public Iterator<Vertex> vertices(Object... objects) {
        if (objects.length == 0) {
            return this.graphTransaction().queryVertices().iterator();
        }
        return this.graphTransaction().queryVertices(objects).iterator();
    }

    public Iterator<Vertex> vertices(Query query) {
        return this.graphTransaction().queryVertices(query).iterator();
    }

    public Iterator<Vertex> adjacentVertices(Iterator<Edge> edges) {
        return this.graphTransaction().queryAdjacentVertices(edges).iterator();
    }

    @Override
    public Iterator<Edge> edges(Object... objects) {
        if (objects.length == 0) {
            return this.graphTransaction().queryEdges().iterator();
        }
        return this.graphTransaction().queryEdges(objects).iterator();
    }

    public Iterator<Edge> edges(Query query) {
        return this.graphTransaction().queryEdges(query).iterator();
    }

    public PropertyKey propertyKey(String name) {
        PropertyKey pk = this.schemaTransaction().getPropertyKey(name);
        E.checkArgument(pk != null, "Undefined property key:'%s'", name);
        return pk;
    }

    public VertexLabel vertexLabel(String name) {
        VertexLabel vl = this.schemaTransaction().getVertexLabel(name);
        E.checkArgument(vl != null, "Undefined vertex label: '%s'", name);
        return vl;
    }

    public EdgeLabel edgeLabel(String name) {
        EdgeLabel el = this.schemaTransaction().getEdgeLabel(name);
        E.checkArgument(el != null, "Undefined edge label: '%s'", name);
        return el;
    }

    public IndexLabel indexLabel(String name) {
        IndexLabel il = this.schemaTransaction().getIndexLabel(name);
        E.checkArgument(il != null, "Undefined index label: '%s'", name);
        return il;
    }

    @Override
    public Transaction tx() {
        return this.tx;
    }

    @Override
    public void close() throws HugeException {
        try {
            if (this.tx.isOpen()) {
                this.tx.close();
            }
        } finally {
            this.destroyTransaction();
            this.storeProvider.close();
        }
    }

    @Override
    public HugeFeatures features() {
        return this.features;
    }

    @Override
    public Variables variables() {
        throw Graph.Exceptions.variablesNotSupported();
    }

    @Override
    public HugeConfig configuration() {
        return this.configuration;
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, this.name());
    }

    private Transaction tx = new AbstractThreadedTransaction(this) {

        private ThreadLocal<GraphTransaction> backendTx = new ThreadLocal<>();

        @Override
        public void open() {
            if (isOpen()) {
                close();
            }
            doOpen();
        }

        @Override
        protected void doOpen() {
            this.backendTx.set(graphTransaction());
            this.backendTx().autoCommit(false);
        }

        @Override
        public void doCommit() {
            this.verifyOpened();
            this.backendTx().commit();
        }

        @Override
        public void doRollback() {
            this.verifyOpened();
            this.backendTx().rollback();
        }

        @Override
        public <R> Workload<R> submit(Function<Graph, R> graphRFunction) {
            throw new UnsupportedOperationException(
                      "HugeGraph transaction does not support submit.");
        }

        @Override
        public <G extends Graph> G createThreadedTx() {
            throw new UnsupportedOperationException(
                      "HugeGraph does not support threaded transactions.");
        }

        @Override
        public boolean isOpen() {
            return this.backendTx() != null;
        }

        @Override
        public void doClose() {
            this.verifyOpened();

            // Calling super will clear listeners
            super.doClose();

            this.backendTx().autoCommit(true);
            try {
                // Would commit() if there is changes
                // TODO: maybe we should call commit() directly
                this.backendTx().afterWrite();
            } finally {
                this.backendTx.remove();
            }
        }

        @Override
        public synchronized Transaction onReadWrite(
                final Consumer<Transaction> consumer) {
            consumer.accept(this);
            return this;
        }

        private GraphTransaction backendTx() {
            return this.backendTx.get();
        }

        private void verifyOpened() {
            if (!this.isOpen()) {
                throw new HugeException("Transaction has not been opened");
            }
        }
    };
}
