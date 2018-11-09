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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;

import com.baidu.hugegraph.analyzer.Analyzer;
import com.baidu.hugegraph.analyzer.AnalyzerFactory;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.cache.CachedGraphTransaction;
import com.baidu.hugegraph.backend.cache.CachedSchemaTransaction;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.serializer.AbstractSerializer;
import com.baidu.hugegraph.backend.serializer.SerializerFactory;
import com.baidu.hugegraph.backend.store.BackendProviderFactory;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.BackendStoreInfo;
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
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeFeatures;
import com.baidu.hugegraph.task.TaskManager;
import com.baidu.hugegraph.task.TaskScheduler;
import com.baidu.hugegraph.traversal.optimize.HugeGraphStepStrategy;
import com.baidu.hugegraph.traversal.optimize.HugeVertexStepStrategy;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.LockUtil;
import com.baidu.hugegraph.util.Log;
import com.baidu.hugegraph.variables.HugeVariables;
import com.google.common.util.concurrent.RateLimiter;

/**
 * HugeGraph is the entrance of the graph system, you can modify or query
 * the schema/vertex/edge data through this class.
 */
public class HugeGraph implements Graph {

    private static final Logger LOG = Log.logger(HugeGraph.class);

    static {
        TraversalStrategies strategies = null;
        strategies = TraversalStrategies.GlobalCache
                                        .getStrategies(Graph.class)
                                        .clone();
        strategies.addStrategies(HugeVertexStepStrategy.instance(),
                                 HugeGraphStepStrategy.instance());
        TraversalStrategies.GlobalCache.registerStrategies(HugeGraph.class,
                                                           strategies);
    }

    static {
        LockUtil.init();
    }

    private final String name;
    private volatile boolean closed;
    private volatile boolean restoring;

    private final HugeConfig configuration;

    private final EventHub schemaEventHub;
    private final EventHub indexEventHub;
    private final RateLimiter rateLimiter;
    private final TaskManager taskManager;

    private final HugeFeatures features;

    private final BackendStoreProvider storeProvider;
    private final TinkerpopTransaction tx;

    private HugeVariables variables;

    public HugeGraph(HugeConfig configuration) {
        this.configuration = configuration;

        this.schemaEventHub = new EventHub("schema");
        this.indexEventHub = new EventHub("index");

        final int limit = configuration.get(CoreOptions.RATE_LIMIT);
        this.rateLimiter = limit > 0 ? RateLimiter.create(limit) : null;

        this.taskManager = TaskManager.instance();

        this.features = new HugeFeatures(this, true);

        this.name = configuration.get(CoreOptions.STORE);
        this.closed = false;
        this.restoring = false;

        try {
            this.storeProvider = this.loadStoreProvider();
        } catch (BackendException e) {
            String message = "Failed to init backend store";
            LOG.error("{}: {}", message, e.getMessage());
            throw new HugeException(message);
        }

        this.tx = new TinkerpopTransaction(this);

        this.taskManager.addScheduler(this);

        this.variables = null;
    }

    public String name() {
        return this.name;
    }

    public String backend() {
        return this.storeProvider.type();
    }

    public String backendVersion() {
        return this.storeProvider.version();
    }

    public boolean closed() {
        return this.closed && this.tx.closed();
    }

    public boolean restoring() {
        return this.restoring;
    }

    public void restoring(boolean restoring) {
        this.restoring = restoring;
    }

    public EventHub schemaEventHub() {
        return this.schemaEventHub;
    }

    public EventHub indexEventHub() {
        return this.indexEventHub;
    }

    public RateLimiter rateLimiter() {
        return this.rateLimiter;
    }

    public void initBackend() {
        this.loadSchemaStore().open(this.configuration);
        this.loadSystemStore().open(this.configuration);
        this.loadGraphStore().open(this.configuration);
        try {
            this.storeProvider.init();
            this.initBackendStoreInfo();
        } finally {
            this.loadGraphStore().close();
            this.loadSystemStore().close();
            this.loadSchemaStore().close();
        }
    }

    public void clearBackend() {
        this.waitUntilAllTasksCompleted();

        this.loadSchemaStore().open(this.configuration);
        this.loadSystemStore().open(this.configuration);
        this.loadGraphStore().open(this.configuration);
        try {
            this.storeProvider.clear();
        } finally {
            this.loadGraphStore().close();
            this.loadSystemStore().close();
            this.loadSchemaStore().close();
        }
    }

    public void truncateBackend() {
        this.waitUntilAllTasksCompleted();

        this.storeProvider.truncate();
        this.initBackendStoreInfo();
    }

    private void waitUntilAllTasksCompleted() {
        Long timeout = this.configuration.get(CoreOptions.TASK_WAIT_TIMEOUT);
        try {
            this.taskScheduler().waitUntilAllTasksCompleted(timeout);
        } catch (TimeoutException e) {
            throw new HugeException("Failed to wait all tasks to complete", e);
        }
    }

    private void initBackendStoreInfo() {
        new BackendStoreInfo(this).init();
    }

    private SchemaTransaction openSchemaTransaction() throws HugeException {
        this.checkGraphNotClosed();
        try {
            return new CachedSchemaTransaction(this, this.loadSchemaStore());
        } catch (BackendException e) {
            String message = "Failed to open schema transaction";
            LOG.error("{}", message, e);
            throw new HugeException(message);
        }
    }

    private GraphTransaction openGraphTransaction() throws HugeException {
        this.checkGraphNotClosed();
        try {
            return new CachedGraphTransaction(this, this.loadGraphStore());
        } catch (BackendException e) {
            String message = "Failed to open graph transaction";
            LOG.error("{}", message, e);
            throw new HugeException(message);
        }
    }

    private BackendStoreProvider loadStoreProvider() {
        String backend = this.configuration.get(CoreOptions.BACKEND);
        LOG.info("Opening backend store '{}' for graph '{}'",
                 backend, this.name);
        return BackendProviderFactory.open(backend, this.name);
    }

    private void checkGraphNotClosed() {
        E.checkState(!this.closed, "Graph '%s' has been closed", this);
    }

    public BackendStore loadSchemaStore() {
        String name = this.configuration.get(CoreOptions.STORE_SCHEMA);
        return this.storeProvider.loadSchemaStore(name);
    }

    public BackendStore loadGraphStore() {
        String graph = this.configuration.get(CoreOptions.STORE_GRAPH);
        return this.storeProvider.loadGraphStore(graph);
    }

    public BackendStore loadSystemStore() {
        String name = this.configuration.get(CoreOptions.STORE_SYSTEM);
        return this.storeProvider.loadSystemStore(name);
    }

    public SchemaTransaction schemaTransaction() {
        this.checkGraphNotClosed();
        /*
         * NOTE: each schema operation will be auto committed,
         * Don't need to open tinkerpop tx by readWrite() and commit manually.
         */
        return this.tx.schemaTransaction();
    }

    public GraphTransaction graphTransaction() {
        this.checkGraphNotClosed();
        /*
         * NOTE: graph operations must be committed manually,
         * Maybe users need to auto open tinkerpop tx by readWrite().
         */
        this.tx.readWrite();
        return this.tx.graphTransaction();
    }

    public SchemaManager schema() {
        return new SchemaManager(this.schemaTransaction());
    }

    public GraphTransaction openTransaction() {
        // Open a new one
        return this.openGraphTransaction();
    }

    public AbstractSerializer serializer() {
        String name = this.configuration.get(CoreOptions.SERIALIZER);
        LOG.debug("Loading serializer '{}' for graph '{}'", name, this.name);
        AbstractSerializer serializer = SerializerFactory.serializer(name);
        if (serializer == null) {
            throw new HugeException("Can't load serializer with name " + name);
        }
        return serializer;
    }

    public Analyzer analyzer() {
        String name = this.configuration.get(CoreOptions.TEXT_ANALYZER);
        String mode = this.configuration.get(CoreOptions.TEXT_ANALYZER_MODE);
        LOG.debug("Loading text analyzer '{}' with mode '{}' for graph '{}'",
                  name, mode, this.name);
        return AnalyzerFactory.analyzer(name, mode);
    }

    public TaskScheduler taskScheduler() {
        TaskScheduler scheduler = this.taskManager.getScheduler(this);
        E.checkState(scheduler != null,
                     "Can't find task scheduler for graph '%s'", this);
        return scheduler;
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        return this.graphTransaction().addVertex(keyValues);
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> clazz)
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
            mapper.addRegistry(HugeGraphIoRegistry.instance())
        ).create();
    }

    @Override
    public Iterator<Vertex> vertices(Object... objects) {
        if (objects.length == 0) {
            return this.graphTransaction().queryVertices();
        }
        return this.graphTransaction().queryVertices(objects);
    }

    public Iterator<Vertex> vertices(Query query) {
        return this.graphTransaction().queryVertices(query);
    }

    public Iterator<Vertex> adjacentVertices(Iterator<Edge> edges) {
        return this.graphTransaction().queryAdjacentVertices(edges);
    }

    @Override
    public Iterator<Edge> edges(Object... objects) {
        if (objects.length == 0) {
            return this.graphTransaction().queryEdges();
        }
        return this.graphTransaction().queryEdges(objects);
    }

    public Iterator<Edge> edges(Query query) {
        return this.graphTransaction().queryEdges(query);
    }

    public PropertyKey propertyKey(Id id) {
        PropertyKey pk = this.schemaTransaction().getPropertyKey(id);
        E.checkArgument(pk != null, "Undefined property key id: '%s'", id);
        return pk;
    }

    public PropertyKey propertyKey(String name) {
        PropertyKey pk = this.schemaTransaction().getPropertyKey(name);
        E.checkArgument(pk != null, "Undefined property key: '%s'", name);
        return pk;
    }

    public VertexLabel vertexLabel(Id id) {
        VertexLabel vl = this.schemaTransaction().getVertexLabel(id);
        E.checkArgument(vl != null, "Undefined vertex label id: '%s'", id);
        return vl;
    }

    public VertexLabel vertexLabel(String name) {
        VertexLabel vl = this.schemaTransaction().getVertexLabel(name);
        E.checkArgument(vl != null, "Undefined vertex label: '%s'", name);
        return vl;
    }

    public EdgeLabel edgeLabel(Id id) {
        EdgeLabel el = this.schemaTransaction().getEdgeLabel(id);
        E.checkArgument(el != null, "Undefined edge label id: '%s'", id);
        return el;
    }

    public EdgeLabel edgeLabel(String name) {
        EdgeLabel el = this.schemaTransaction().getEdgeLabel(name);
        E.checkArgument(el != null, "Undefined edge label: '%s'", name);
        return el;
    }

    public IndexLabel indexLabel(Id id) {
        IndexLabel il = this.schemaTransaction().getIndexLabel(id);
        E.checkArgument(il != null, "Undefined index label id: '%s'", id);
        return il;
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
        this.closed = true;
        try {
            this.closeTx();
        } finally {
            this.storeProvider.close();
            this.taskManager.closeScheduler(this);
        }
    }

    public void closeTx() {
        try {
            if (this.tx.isOpen()) {
                this.tx.close();
            }
        } finally {
            this.tx.destroyTransaction();
        }
    }

    @Override
    public HugeFeatures features() {
        return this.features;
    }

    @Override
    public synchronized Variables variables() {
        if (this.variables == null) {
            this.variables = new HugeVariables(this);
        }
        // Ensure variables() work after variables schema was cleared
        this.variables.initSchema();
        return this.variables;
    }

    @Override
    public HugeConfig configuration() {
        return this.configuration;
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, this.name());
    }

    public List<String> mapPkId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.propertyKey(id);
            names.add(schema.name());
        }
        return names;
    }

    public List<String> mapVlId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.vertexLabel(id);
            names.add(schema.name());
        }
        return names;
    }

    public List<String> mapElId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.edgeLabel(id);
            names.add(schema.name());
        }
        return names;
    }

    public List<String> mapIlId2Name(Collection<Id> ids) {
        List<String> names = new ArrayList<>(ids.size());
        for (Id id : ids) {
            SchemaElement schema = this.indexLabel(id);
            names.add(schema.name());
        }
        return names;
    }

    public List<Id> mapPkName2Id(Collection<String> pkeys) {
        List<Id> ids = new ArrayList<>(pkeys.size());
        for (String pkey : pkeys) {
            PropertyKey propertyKey = this.propertyKey(pkey);
            ids.add(propertyKey.id());
        }
        return ids;
    }

    public Id[] mapElName2Id(String[] edgeLabels) {
        Id[] ids = new Id[edgeLabels.length];
        for (int i = 0; i < edgeLabels.length; i++) {
            EdgeLabel edgeLabel = this.edgeLabel(edgeLabels[i]);
            ids[i] = edgeLabel.id();
        }
        return ids;
    }

    public Id[] mapVlName2Id(String[] vertexLabels) {
        Id[] ids = new Id[vertexLabels.length];
        for (int i = 0; i < vertexLabels.length; i++) {
            VertexLabel vertexLabel = this.vertexLabel(vertexLabels[i]);
            ids[i] = vertexLabel.id();
        }
        return ids;
    }

    /**
     * Stop all the daemon threads
     * @param timout seconds
     * @throws InterruptedException when be interrupted
     */
    public static void shutdown(long timout) throws InterruptedException {
        EventHub.destroy(timout);
        TaskManager.instance().shutdown(timout);
    }

    private class TinkerpopTransaction extends AbstractThreadLocalTransaction {

        private AtomicInteger refs;

        private ThreadLocal<Boolean> opened;

        // Backend transactions
        private ThreadLocal<GraphTransaction> graphTransaction;
        private ThreadLocal<SchemaTransaction> schemaTransaction;

        public TinkerpopTransaction(Graph graph) {
            super(graph);

            this.refs = new AtomicInteger(0);

            this.opened = ThreadLocal.withInitial(() -> false);
            this.graphTransaction = ThreadLocal.withInitial(() -> null);
            this.schemaTransaction = ThreadLocal.withInitial(() -> null);
        }

        public boolean closed() {
            assert this.refs.get() >= 0 : this.refs.get();
            return this.refs.get() == 0;
        }

        public void commitIfGtSize(int size) {
            // Only committing graph transaction data if reaching batch size
            // is OK, bacause schema transaction is auto committed.
            this.graphTransaction().commitIfGtSize(size);
        }

        @Override
        public void commit() {
            try {
                super.commit();
            } finally {
                this.setClosed();
            }
        }

        @Override
        public void rollback() {
            try {
                super.rollback();
            } finally {
                this.setClosed();
            }
        }

        @Override
        public <G extends Graph> G createThreadedTx() {
            throw Transaction.Exceptions.threadedTransactionsNotSupported();
        }

        @Override
        public boolean isOpen() {
            return this.opened.get();
        }

        @Override
        protected void doOpen() {
            this.schemaTransaction();
            this.graphTransaction();

            this.setOpened();
        }

        @Override
        protected void doCommit() {
            this.verifyOpened();

            this.schemaTransaction().commit();
            this.graphTransaction().commit();
        }

        @Override
        protected void doRollback() {
            this.verifyOpened();

            try {
                this.graphTransaction().rollback();
            } finally {
                this.schemaTransaction().rollback();
            }
        }

        @Override
        protected void doClose() {
            this.verifyOpened();

            try {
                // Calling super will clear listeners
                super.doClose();
            } finally {
                this.resetState();
            }
        }

        @Override
        public String toString() {
            return String.format("TinkerpopTransaction{opened=%s, " +
                                 "graphTx=%s, schemaTx=%s}",
                                 this.opened.get(),
                                 this.graphTransaction.get(),
                                 this.schemaTransaction.get());
        }

        private void verifyOpened() {
            if (!this.isOpen()) {
                throw new HugeException("Transaction has not been opened");
            }
        }

        private void resetState() {
            this.setClosed();
            this.readWriteConsumerInternal.set(READ_WRITE_BEHAVIOR.AUTO);
            this.closeConsumerInternal.set(CLOSE_BEHAVIOR.ROLLBACK);
        }

        private void setOpened() {
            // The backend tx may be reused, here just set a flag
            assert this.opened.get() == false;
            this.opened.set(true);
            this.refs.incrementAndGet();
        }

        private void setClosed() {
            // Just set flag opened=false to reuse the backend tx
            if (this.opened.get()) {
                this.opened.set(false);
                this.refs.decrementAndGet();
            }
        }

        private SchemaTransaction schemaTransaction() {
            /*
             * NOTE: this method may be called even tx is not opened,
             * the reason is for reusing backend tx.
             * so we don't call this.verifyOpened() here.
             */

            SchemaTransaction schemaTx = this.schemaTransaction.get();
            if (schemaTx == null) {
                schemaTx = openSchemaTransaction();
                this.schemaTransaction.set(schemaTx);
            }
            return schemaTx;
        }

        private GraphTransaction graphTransaction() {
            /*
             * NOTE: this method may be called even tx is not opened,
             * the reason is for reusing backend tx.
             * so we don't call this.verifyOpened() here.
             */

            GraphTransaction graphTx = this.graphTransaction.get();
            if (graphTx == null) {
                graphTx = openGraphTransaction();
                this.graphTransaction.set(graphTx);
            }
            return graphTx;
        }

        private void destroyTransaction() {
            if (this.isOpen()) {
                throw new HugeException(
                          "Transaction should be closed before destroying");
            }

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
    }
}
