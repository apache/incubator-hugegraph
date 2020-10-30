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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;

import com.baidu.hugegraph.analyzer.Analyzer;
import com.baidu.hugegraph.analyzer.AnalyzerFactory;
import com.baidu.hugegraph.auth.StandardUserManager;
import com.baidu.hugegraph.auth.UserManager;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.cache.CachedGraphTransaction;
import com.baidu.hugegraph.backend.cache.CachedSchemaTransaction;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.SnowflakeIdGenerator;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.serializer.AbstractSerializer;
import com.baidu.hugegraph.backend.serializer.SerializerFactory;
import com.baidu.hugegraph.backend.store.BackendFeatures;
import com.baidu.hugegraph.backend.store.BackendProviderFactory;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.BackendStoreProvider;
import com.baidu.hugegraph.backend.store.BackendStoreSystemInfo;
import com.baidu.hugegraph.backend.store.raft.RaftBackendStoreProvider;
import com.baidu.hugegraph.backend.store.raft.RaftGroupManager;
import com.baidu.hugegraph.backend.store.ram.RamTable;
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
import com.baidu.hugegraph.schema.SchemaLabel;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeEdgeProperty;
import com.baidu.hugegraph.structure.HugeFeatures;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.structure.HugeVertexProperty;
import com.baidu.hugegraph.task.ServerInfoManager;
import com.baidu.hugegraph.task.TaskManager;
import com.baidu.hugegraph.task.TaskScheduler;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.GraphMode;
import com.baidu.hugegraph.type.define.NodeRole;
import com.baidu.hugegraph.util.DateUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.LockUtil;
import com.baidu.hugegraph.util.Log;
import com.baidu.hugegraph.variables.HugeVariables;
import com.google.common.util.concurrent.RateLimiter;

/**
 * StandardHugeGraph is the entrance of the graph system, you can modify or
 * query the schema/vertex/edge data through this class.
 */
public class StandardHugeGraph implements HugeGraph {

    public static final Class<?>[] PROTECT_CLASSES = {
           StandardHugeGraph.class,
           StandardHugeGraph.StandardHugeGraphParams.class,
           StandardHugeGraph.TinkerpopTransaction.class,
           StandardHugeGraph.Txs.class,
           StandardHugeGraph.SysTransaction.class
    };

    private static final Logger LOG = Log.logger(HugeGraph.class);

    private volatile boolean started;
    private volatile boolean closed;
    private volatile GraphMode mode;
    private volatile HugeVariables variables;

    private final String name;

    private final StandardHugeGraphParams params;

    private final HugeConfig configuration;

    private final EventHub schemaEventHub;
    private final EventHub graphEventHub;
    private final EventHub indexEventHub;

    private final RateLimiter writeRateLimiter;
    private final RateLimiter readRateLimiter;
    private final TaskManager taskManager;
    private final UserManager userManager;

    private final HugeFeatures features;

    private final BackendStoreProvider storeProvider;
    private final TinkerpopTransaction tx;

    private final RamTable ramtable;

    public StandardHugeGraph(HugeConfig config) {
        this.params = new StandardHugeGraphParams();
        this.configuration = config;

        this.schemaEventHub = new EventHub("schema");
        this.graphEventHub = new EventHub("graph");
        this.indexEventHub = new EventHub("index");

        final int writeLimit = config.get(CoreOptions.RATE_LIMIT_WRITE);
        this.writeRateLimiter = writeLimit > 0 ?
                                RateLimiter.create(writeLimit) : null;
        final int readLimit = config.get(CoreOptions.RATE_LIMIT_READ);
        this.readRateLimiter = readLimit > 0 ?
                               RateLimiter.create(readLimit) : null;

        boolean ramtableEnable = config.get(CoreOptions.QUERY_RAMTABLE_ENABLE);
        if (ramtableEnable) {
            long vc = config.get(CoreOptions.QUERY_RAMTABLE_VERTICES_CAPACITY);
            int ec = config.get(CoreOptions.QUERY_RAMTABLE_EDGES_CAPACITY);
            this.ramtable = new RamTable(this, vc, ec);
        } else {
            this.ramtable = null;
        }

        this.taskManager = TaskManager.instance();

        this.features = new HugeFeatures(this, true);

        this.name = config.get(CoreOptions.STORE);
        this.started = false;
        this.closed = false;
        this.mode = GraphMode.NONE;

        LockUtil.init(this.name);

        try {
            this.storeProvider = this.loadStoreProvider();
        } catch (BackendException e) {
            LockUtil.destroy(this.name);
            String message = "Failed to load backend store provider";
            LOG.error("{}: {}", message, e.getMessage());
            throw new HugeException(message);
        }

        this.tx = new TinkerpopTransaction(this);

        SnowflakeIdGenerator.init(this.params);

        this.taskManager.addScheduler(this.params);
        this.userManager = new StandardUserManager(this.params);
        this.variables = null;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public HugeGraph hugegraph() {
        return this;
    }

    @Override
    public String backend() {
        return this.storeProvider.type();
    }

    @Override
    public String backendVersion() {
        return this.storeProvider.version();
    }

    @Override
    public BackendStoreSystemInfo backendStoreSystemInfo() {
        return new BackendStoreSystemInfo(this.schemaTransaction());
    }

    @Override
    public BackendFeatures backendStoreFeatures() {
        return this.graphTransaction().storeFeatures();
    }

    @Override
    public void serverStarted(Id serverId, NodeRole serverRole) {
        LOG.info("Init server info for graph '{}'...", this.name);
        this.serverInfoManager().initServerInfo(serverId, serverRole);

        LOG.info("Restoring incomplete tasks for graph '{}'...", this.name);
        this.taskScheduler().restoreTasks();

        this.started = true;
    }

    @Override
    public boolean started() {
        return this.started;
    }

    @Override
    public boolean closed() {
        if (this.closed && !this.tx.closed()) {
            LOG.warn("The tx is not closed while graph '{}' is closed", this);
        }
        return this.closed;
    }

    @Override
    public GraphMode mode() {
        return this.mode;
    }

    @Override
    public void mode(GraphMode mode) {
        LOG.info("Graph {} will work in {} mode", this, mode);
        this.mode = mode;
    }

    @Override
    public void waitStarted() {
        // Just for trigger Tx.getOrNewTransaction, then load 3 stores
        this.schemaTransaction();
        this.storeProvider.waitStoreStarted();
    }

    @Override
    public void initBackend() {
        this.loadSchemaStore().open(this.configuration);
        this.loadSystemStore().open(this.configuration);
        this.loadGraphStore().open(this.configuration);

        LockUtil.lock(this.name, LockUtil.GRAPH_LOCK);
        try {
            this.storeProvider.init();
            this.storeProvider.initSystemInfo(this);
        } finally {
            LockUtil.unlock(this.name, LockUtil.GRAPH_LOCK);
            this.loadGraphStore().close();
            this.loadSystemStore().close();
            this.loadSchemaStore().close();
        }

        LOG.info("Graph '{}' has been initialized", this.name);
    }

    @Override
    public void clearBackend() {
        this.waitUntilAllTasksCompleted();

        this.loadSchemaStore().open(this.configuration);
        this.loadSystemStore().open(this.configuration);
        this.loadGraphStore().open(this.configuration);

        LockUtil.lock(this.name, LockUtil.GRAPH_LOCK);
        try {
            this.storeProvider.clear();
        } finally {
            LockUtil.unlock(this.name, LockUtil.GRAPH_LOCK);
            this.loadGraphStore().close();
            this.loadSystemStore().close();
            this.loadSchemaStore().close();
        }

        LOG.info("Graph '{}' has been cleared", this.name);
    }

    @Override
    public void truncateBackend() {
        this.waitUntilAllTasksCompleted();

        LockUtil.lock(this.name, LockUtil.GRAPH_LOCK);
        try {
            this.storeProvider.truncate();
            this.storeProvider.initSystemInfo(this);
            this.serverStarted(this.serverInfoManager().selfServerId(),
                               this.serverInfoManager().selfServerRole());
        } finally {
            LockUtil.unlock(this.name, LockUtil.GRAPH_LOCK);
        }

        LOG.info("Graph '{}' has been truncated", this.name);
    }

    private SchemaTransaction openSchemaTransaction() throws HugeException {
        this.checkGraphNotClosed();
        try {
            return new CachedSchemaTransaction(this.params, loadSchemaStore());
        } catch (BackendException e) {
            String message = "Failed to open schema transaction";
            LOG.error("{}", message, e);
            throw new HugeException(message);
        }
    }

    private SysTransaction openSystemTransaction() throws HugeException {
        this.checkGraphNotClosed();
        try {
            return new SysTransaction(this.params, loadSystemStore());
        } catch (BackendException e) {
            String message = "Failed to open system transaction";
            LOG.error("{}", message, e);
            throw new HugeException(message);
        }
    }

    private GraphTransaction openGraphTransaction() throws HugeException {
        // Open a new one
        this.checkGraphNotClosed();
        try {
            return new CachedGraphTransaction(this.params, loadGraphStore());
        } catch (BackendException e) {
            String message = "Failed to open graph transaction";
            LOG.error("{}", message, e);
            throw new HugeException(message);
        }
    }

    private void checkGraphNotClosed() {
        E.checkState(!this.closed, "Graph '%s' has been closed", this);
    }

    private BackendStore loadSchemaStore() {
        String name = this.configuration.get(CoreOptions.STORE_SCHEMA);
        return this.storeProvider.loadSchemaStore(name);
    }

    private BackendStore loadGraphStore() {
        String graph = this.configuration.get(CoreOptions.STORE_GRAPH);
        return this.storeProvider.loadGraphStore(graph);
    }

    private BackendStore loadSystemStore() {
        String name = this.configuration.get(CoreOptions.STORE_SYSTEM);
        return this.storeProvider.loadSystemStore(name);
    }

    private SchemaTransaction schemaTransaction() {
        this.checkGraphNotClosed();
        /*
         * NOTE: each schema operation will be auto committed,
         * Don't need to open tinkerpop tx by readWrite() and commit manually.
         */
        return this.tx.schemaTransaction();
    }

    private SysTransaction systemTransaction() {
        this.checkGraphNotClosed();
        /*
         * NOTE: system operations must be committed manually,
         * Maybe users need to auto open tinkerpop tx by readWrite().
         */
        this.tx.readWrite();
        return this.tx.systemTransaction();
    }

    private GraphTransaction graphTransaction() {
        this.checkGraphNotClosed();
        /*
         * NOTE: graph operations must be committed manually,
         * Maybe users need to auto open tinkerpop tx by readWrite().
         */
        this.tx.readWrite();
        return this.tx.graphTransaction();
    }

    private BackendStoreProvider loadStoreProvider() {
        return BackendProviderFactory.open(this.params);
    }

    private AbstractSerializer serializer() {
        String name = this.configuration.get(CoreOptions.SERIALIZER);
        LOG.debug("Loading serializer '{}' for graph '{}'", name, this.name);
        AbstractSerializer serializer = SerializerFactory.serializer(name);
        if (serializer == null) {
            throw new HugeException("Can't load serializer with name " + name);
        }
        return serializer;
    }

    private Analyzer analyzer() {
        String name = this.configuration.get(CoreOptions.TEXT_ANALYZER);
        String mode = this.configuration.get(CoreOptions.TEXT_ANALYZER_MODE);
        LOG.debug("Loading text analyzer '{}' with mode '{}' for graph '{}'",
                  name, mode, this.name);
        return AnalyzerFactory.analyzer(name, mode);
    }

    protected void reloadRamtable() {
        this.reloadRamtable(false);
    }

    protected void reloadRamtable(boolean loadFromFile) {
        // Expect triggered manually, like gremlin job
        if (this.ramtable != null) {
            this.ramtable.reload(loadFromFile, this.name);
        } else {
            LOG.warn("The ramtable feature is not enabled for graph {}", this);
        }
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
    public Vertex addVertex(Object... keyValues) {
        return this.graphTransaction().addVertex(keyValues);
    }

    @Override
    public void removeVertex(Vertex vertex) {
        this.graphTransaction().removeVertex((HugeVertex) vertex);
    }

    @Override
    public void removeVertex(String label, Object id) {
        if (label != null) {
            VertexLabel vl = this.vertexLabel(label);
            // It's OK even if exist adjacent edges `vl.existsLinkLabel()`
            if (!vl.existsIndexLabel()) {
                // Improve perf by removeVertex(id)
                Id idValue = HugeVertex.getIdValue(id);
                HugeVertex vertex = new HugeVertex(this, idValue, vl);
                this.removeVertex(vertex);
                return;
            }
        }

        this.vertex(id).remove();
    }

    @Override
    public <V> void addVertexProperty(VertexProperty<V> p) {
        this.graphTransaction().addVertexProperty((HugeVertexProperty<V>) p);
    }

    @Override
    public <V> void removeVertexProperty(VertexProperty<V> p) {
        this.graphTransaction().removeVertexProperty((HugeVertexProperty<V>) p);
    }

    @Override
    public Edge addEdge(Edge edge) {
        return this.graphTransaction().addEdge((HugeEdge) edge);
    }

    @Override
    public void canAddEdge(Edge edge) {
        // pass
    }

    @Override
    public void removeEdge(Edge edge) {
        this.graphTransaction().removeEdge((HugeEdge) edge);
    }

    @Override
    public void removeEdge(String label, Object id) {
        if (label != null) {
            EdgeLabel el = this.edgeLabel(label);
            if (!el.existsIndexLabel()) {
                // Improve perf by removeEdge(id)
                Id idValue = HugeEdge.getIdValue(id, false);
                HugeEdge edge = new HugeEdge(this, idValue, el);
                this.removeEdge(edge);
                return;
            }
        }

        this.edge(id).remove();
    }

    @Override
    public <V> void addEdgeProperty(Property<V> p) {
        this.graphTransaction().addEdgeProperty((HugeEdgeProperty<V>) p);
    }

    @Override
    public <V> void removeEdgeProperty(Property<V> p) {
        this.graphTransaction().removeEdgeProperty((HugeEdgeProperty<V>) p);
    }

    @Override
    public Vertex vertex(Object object) {
        return this.graphTransaction().queryVertex(object);
    }

    @Override
    public Iterator<Vertex> vertices(Object... objects) {
        if (objects.length == 0) {
            return this.graphTransaction().queryVertices();
        }
        return this.graphTransaction().queryVertices(objects);
    }

    @Override
    public Iterator<Vertex> vertices(Query query) {
        return this.graphTransaction().queryVertices(query);
    }

    @Override
    public Iterator<Vertex> adjacentVertex(Object id) {
        return this.graphTransaction().queryAdjacentVertices(id);
    }

    @Override
    public boolean checkAdjacentVertexExist() {
        return this.graphTransaction().checkAdjacentVertexExist();
    }

    @Override
    public Edge edge(Object object) {
        return this.graphTransaction().queryEdge(object);
    }

    @Override
    public Iterator<Edge> edges(Object... objects) {
        if (objects.length == 0) {
            return this.graphTransaction().queryEdges();
        }
        return this.graphTransaction().queryEdges(objects);
    }

    @Override
    public Iterator<Edge> edges(Query query) {
        return this.graphTransaction().queryEdges(query);
    }

    @Override
    public Iterator<Vertex> adjacentVertices(Iterator<Edge> edges) {
        return this.graphTransaction().queryAdjacentVertices(edges);
    }

    @Override
    public Iterator<Edge> adjacentEdges(Id vertexId) {
        return this.graphTransaction().queryEdgesByVertex(vertexId);
    }

    @Override
    public Number queryNumber(Query query) {
        return this.graphTransaction().queryNumber(query);
    }

    @Override
    public void addPropertyKey(PropertyKey pkey) {
        assert this.name.equals(pkey.graph().name());
        this.schemaTransaction().addPropertyKey(pkey);
    }

    @Override
    public void removePropertyKey(Id pkey) {
        this.schemaTransaction().removePropertyKey(pkey);
    }

    @Override
    public Collection<PropertyKey> propertyKeys() {
        return this.schemaTransaction().getPropertyKeys();
    }

    @Override
    public PropertyKey propertyKey(Id id) {
        PropertyKey pk = this.schemaTransaction().getPropertyKey(id);
        E.checkArgument(pk != null, "Undefined property key with id: '%s'", id);
        return pk;
    }

    @Override
    public PropertyKey propertyKey(String name) {
        PropertyKey pk = this.schemaTransaction().getPropertyKey(name);
        E.checkArgument(pk != null, "Undefined property key: '%s'", name);
        return pk;
    }

    @Override
    public boolean existsPropertyKey(String name) {
        return this.schemaTransaction().getPropertyKey(name) != null;
    }

    @Override
    public void addVertexLabel(VertexLabel vertexLabel) {
        assert this.name.equals(vertexLabel.graph().name());
        this.schemaTransaction().addVertexLabel(vertexLabel);
    }

    @Override
    public Id removeVertexLabel(Id label) {
        return this.schemaTransaction().removeVertexLabel(label);
    }

    @Override
    public Collection<VertexLabel> vertexLabels() {
        return this.schemaTransaction().getVertexLabels();
    }

    @Override
    public VertexLabel vertexLabelOrNone(Id id) {
        VertexLabel vl = this.schemaTransaction().getVertexLabel(id);
        if (vl == null) {
            vl = VertexLabel.undefined(this, id);
        }
        return vl;
    }

    @Override
    public VertexLabel vertexLabel(Id id) {
        VertexLabel vl = this.schemaTransaction().getVertexLabel(id);
        E.checkArgument(vl != null, "Undefined vertex label with id: '%s'", id);
        return vl;
    }

    @Override
    public VertexLabel vertexLabel(String name) {
        VertexLabel vl = this.schemaTransaction().getVertexLabel(name);
        E.checkArgument(vl != null, "Undefined vertex label: '%s'", name);
        return vl;
    }

    @Override
    public boolean existsVertexLabel(String name) {
        return this.schemaTransaction().getVertexLabel(name) != null;
    }

    @Override
    public boolean existsLinkLabel(Id vertexLabel) {
        List<EdgeLabel> edgeLabels = this.schemaTransaction().getEdgeLabels();
        for (EdgeLabel edgeLabel : edgeLabels) {
            if (edgeLabel.linkWithLabel(vertexLabel)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void addEdgeLabel(EdgeLabel edgeLabel) {
        assert this.name.equals(edgeLabel.graph().name());
        this.schemaTransaction().addEdgeLabel(edgeLabel);
    }

    @Override
    public Id removeEdgeLabel(Id id) {
        return this.schemaTransaction().removeEdgeLabel(id);
    }

    @Override
    public Collection<EdgeLabel> edgeLabels() {
        return this.schemaTransaction().getEdgeLabels();
    }

    @Override
    public EdgeLabel edgeLabelOrNone(Id id) {
        EdgeLabel el = this.schemaTransaction().getEdgeLabel(id);
        if (el == null) {
            el = EdgeLabel.undefined(this, id);
        }
        return el;
    }

    @Override
    public EdgeLabel edgeLabel(Id id) {
        EdgeLabel el = this.schemaTransaction().getEdgeLabel(id);
        E.checkArgument(el != null, "Undefined edge label with id: '%s'", id);
        return el;
    }

    @Override
    public EdgeLabel edgeLabel(String name) {
        EdgeLabel el = this.schemaTransaction().getEdgeLabel(name);
        E.checkArgument(el != null, "Undefined edge label: '%s'", name);
        return el;
    }

    @Override
    public boolean existsEdgeLabel(String name) {
        return this.schemaTransaction().getEdgeLabel(name) != null;
    }

    @Override
    public void addIndexLabel(SchemaLabel schemaLabel, IndexLabel indexLabel) {
        assert this.name.equals(schemaLabel.graph().name());
        assert this.name.equals(indexLabel.graph().name());
        this.schemaTransaction().addIndexLabel(schemaLabel, indexLabel);
    }

    @Override
    public Id removeIndexLabel(Id id) {
        return this.schemaTransaction().removeIndexLabel(id);
    }

    @Override
    public Id rebuildIndex(SchemaElement schema) {
        return this.schemaTransaction().rebuildIndex(schema);
    }

    @Override
    public Collection<IndexLabel> indexLabels() {
        return this.schemaTransaction().getIndexLabels();
    }

    @Override
    public IndexLabel indexLabel(Id id) {
        IndexLabel il = this.schemaTransaction().getIndexLabel(id);
        E.checkArgument(il != null, "Undefined index label with id: '%s'", id);
        return il;
    }

    @Override
    public IndexLabel indexLabel(String name) {
        IndexLabel il = this.schemaTransaction().getIndexLabel(name);
        E.checkArgument(il != null, "Undefined index label: '%s'", name);
        return il;
    }

    @Override
    public boolean existsIndexLabel(String name) {
        return this.schemaTransaction().getIndexLabel(name) != null;
    }

    @Override
    public Transaction tx() {
        return this.tx;
    }

    @Override
    public synchronized void close() throws Exception {
        if (this.closed()) {
            return;
        }

        LOG.info("Close graph {}", this);
        this.userManager.close();
        this.taskManager.closeScheduler(this.params);
        try {
            this.closeTx();
        } finally {
            this.closed = true;
            this.storeProvider.close();
            LockUtil.destroy(this.name);
        }
        // Make sure that all transactions are closed in all threads
        E.checkState(this.tx.closed(),
                     "Ensure tx closed in all threads when closing graph '%s'",
                     this.name);
    }

    @Override
    public HugeFeatures features() {
        return this.features;
    }

    @Override
    public synchronized Variables variables() {
        if (this.variables == null) {
            this.variables = new HugeVariables(this.params);
        }
        // Ensure variables() work after variables schema was cleared
        this.variables.initSchemaIfNeeded();
        return this.variables;
    }

    @Override
    public SchemaManager schema() {
        return new SchemaManager(this.schemaTransaction(), this);
    }

    @Override
    public Id getNextId(HugeType type) {
        return this.schemaTransaction().getNextId(type);
    }

    @Override
    public <T> T metadata(HugeType type, String meta, Object... args) {
        return this.graphTransaction().metadata(type, meta, args);
    }

    @Override
    public TaskScheduler taskScheduler() {
        TaskScheduler scheduler = this.taskManager.getScheduler(this.params);
        E.checkState(scheduler != null,
                     "Can't find task scheduler for graph '%s'", this);
        return scheduler;
    }

    private ServerInfoManager serverInfoManager() {
        ServerInfoManager manager = this.taskManager
                                        .getServerInfoManager(this.params);
        E.checkState(manager != null,
                     "Can't find server info manager for graph '%s'", this);
        return manager;
    }

    @Override
    public UserManager userManager() {
        // this.userManager.initSchemaIfNeeded();
        return this.userManager;
    }

    @Override
    public RaftGroupManager raftGroupManager(String group) {
        if (!(this.storeProvider instanceof RaftBackendStoreProvider)) {
            return null;
        }
        RaftBackendStoreProvider provider =
                ((RaftBackendStoreProvider) this.storeProvider);
        return provider.raftNodeManager(group);
    }

    @Override
    public HugeConfig configuration() {
        return this.configuration;
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, this.name());
    }

    @Override
    public final void proxy(HugeGraph graph) {
        this.params.graph(graph);
    }

    @Override
    public boolean sameAs(HugeGraph graph) {
        return this == graph;
    }

    @Override
    public long now() {
        return ((TinkerpopTransaction) this.tx()).openedTime();
    }

    private void closeTx() {
        try {
            if (this.tx.isOpen()) {
                this.tx.close();
            }
        } finally {
            this.tx.destroyTransaction();
        }
    }

    private void waitUntilAllTasksCompleted() {
        long timeout = this.configuration.get(CoreOptions.TASK_WAIT_TIMEOUT);
        try {
            this.taskScheduler().waitUntilAllTasksCompleted(timeout);
        } catch (TimeoutException e) {
            throw new HugeException("Failed to wait all tasks to complete", e);
        }
    }

    private class StandardHugeGraphParams implements HugeGraphParams {

        private HugeGraph graph = StandardHugeGraph.this;

        private void graph(HugeGraph graph) {
            this.graph = graph;
        }

        @Override
        public HugeGraph graph() {
            return this.graph;
        }

        @Override
        public String name() {
            return StandardHugeGraph.this.name();
        }

        @Override
        public GraphMode mode() {
            return StandardHugeGraph.this.mode();
        }

        @Override
        public SchemaTransaction schemaTransaction() {
            return StandardHugeGraph.this.schemaTransaction();
        }

        @Override
        public GraphTransaction systemTransaction() {
            return StandardHugeGraph.this.systemTransaction();
        }

        @Override
        public GraphTransaction graphTransaction() {
            return StandardHugeGraph.this.graphTransaction();
        }

        @Override
        public GraphTransaction openTransaction() {
            // Open a new one
            return StandardHugeGraph.this.openGraphTransaction();
        }

        @Override
        public void closeTx() {
            StandardHugeGraph.this.closeTx();
        }

        @Override
        public boolean started() {
            return StandardHugeGraph.this.started();
        }

        @Override
        public boolean closed() {
            return StandardHugeGraph.this.closed();
        }

        @Override
        public boolean initialized() {
            return StandardHugeGraph.this.graphTransaction().storeInitialized();
        }

        @Override
        public BackendFeatures backendStoreFeatures() {
            return StandardHugeGraph.this.backendStoreFeatures();
        }

        @Override
        public BackendStore loadSchemaStore() {
            return StandardHugeGraph.this.loadSchemaStore();
        }

        @Override
        public BackendStore loadGraphStore() {
            return StandardHugeGraph.this.loadGraphStore();
        }

        @Override
        public BackendStore loadSystemStore() {
            return StandardHugeGraph.this.loadSystemStore();
        }

        @Override
        public EventHub schemaEventHub() {
            return StandardHugeGraph.this.schemaEventHub;
        }

        @Override
        public EventHub graphEventHub() {
            return StandardHugeGraph.this.graphEventHub;
        }

        @Override
        public EventHub indexEventHub() {
            return StandardHugeGraph.this.indexEventHub;
        }

        @Override
        public HugeConfig configuration() {
            return StandardHugeGraph.this.configuration();
        }

        @Override
        public ServerInfoManager serverManager() {
            // this.serverManager.initSchemaIfNeeded();
            return StandardHugeGraph.this.serverInfoManager();
        }

        @Override
        public AbstractSerializer serializer() {
            return StandardHugeGraph.this.serializer();
        }

        @Override
        public Analyzer analyzer() {
            return StandardHugeGraph.this.analyzer();
        }

        @Override
        public RateLimiter writeRateLimiter() {
            return StandardHugeGraph.this.writeRateLimiter;
        }

        @Override
        public RateLimiter readRateLimiter() {
            return StandardHugeGraph.this.readRateLimiter;
        }

        @Override
        public RamTable ramtable() {
            return StandardHugeGraph.this.ramtable;
        }
    }

    private class TinkerpopTransaction extends AbstractThreadLocalTransaction {

        // Times opened from upper layer
        private final AtomicInteger refs;
        // Flag opened of each thread
        private final ThreadLocal<Boolean> opened;
        // Backend transactions
        private final ThreadLocal<Txs> transactions;

        public TinkerpopTransaction(Graph graph) {
            super(graph);

            this.refs = new AtomicInteger();
            this.opened = ThreadLocal.withInitial(() -> false);
            this.transactions = ThreadLocal.withInitial(() -> null);
        }

        public boolean closed() {
            int refs = this.refs.get();
            assert refs >= 0 : refs;
            return refs == 0;
        }

        /**
         * Commit tx if batch size reaches the specified value,
         * it may be used by Gremlin
         */
        @SuppressWarnings("unused")
        public void commitIfGtSize(int size) {
            // Only commit graph transaction data (schema auto committed)
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
            this.getOrNewTransaction();
            this.setOpened();
        }

        @Override
        protected void doCommit() {
            this.verifyOpened();
            this.getOrNewTransaction().commit();
        }

        @Override
        protected void doRollback() {
            this.verifyOpened();
            this.getOrNewTransaction().rollback();
        }

        @Override
        protected void doClose() {
            this.verifyOpened();

            try {
                // Calling super.doClose() will clear listeners
                super.doClose();
            } finally {
                this.resetState();
            }
        }

        @Override
        public String toString() {
            return String.format("TinkerpopTransaction{opened=%s, txs=%s}",
                                 this.opened.get(), this.transactions.get());
        }

        public long openedTime() {
            return this.transactions.get().openedTime();
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
            this.transactions.get().openedTime(DateUtil.now().getTime());
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
            return this.getOrNewTransaction().schemaTx;
        }

        private SysTransaction systemTransaction() {
            return this.getOrNewTransaction().systemTx;
        }

        private GraphTransaction graphTransaction() {
            return this.getOrNewTransaction().graphTx;
        }

        private Txs getOrNewTransaction() {
            /*
             * NOTE: this method may be called even tx is not opened,
             * the reason is for reusing backend tx.
             * so we don't call this.verifyOpened() here.
             */

            Txs txs = this.transactions.get();
            if (txs == null) {
                // TODO: close SchemaTransaction if GraphTransaction is error
                txs = new Txs(openSchemaTransaction(), openSystemTransaction(),
                              openGraphTransaction());
                this.transactions.set(txs);
            }
            return txs;
        }

        private void destroyTransaction() {
            if (this.isOpen()) {
                throw new HugeException(
                          "Transaction should be closed before destroying");
            }

            // Do close if needed, then remove the reference
            Txs txs = this.transactions.get();
            if (txs != null) {
                txs.close();
            }
            this.transactions.remove();
        }
    }

    private static final class Txs {

        private final SchemaTransaction schemaTx;
        private final SysTransaction systemTx;
        private final GraphTransaction graphTx;
        private long openedTime;

        public Txs(SchemaTransaction schemaTx, SysTransaction systemTx,
                   GraphTransaction graphTx) {
            assert schemaTx != null && systemTx != null && graphTx != null;
            this.schemaTx = schemaTx;
            this.systemTx = systemTx;
            this.graphTx = graphTx;
            this.openedTime = DateUtil.now().getTime();
        }

        public void commit() {
            this.graphTx.commit();
        }

        public void rollback() {
            this.graphTx.rollback();
        }

        public void close() {
            try {
                this.graphTx.close();
            } catch (Exception e) {
                LOG.error("Failed to close GraphTransaction", e);
            }

            try {
                this.systemTx.close();
            } catch (Exception e) {
                LOG.error("Failed to close SystemTransaction", e);
            }

            try {
                this.schemaTx.close();
            } catch (Exception e) {
                LOG.error("Failed to close SchemaTransaction", e);
            }
        }

        public void openedTime(long time) {
            this.openedTime = time;
        }

        public long openedTime() {
            return this.openedTime;
        }

        @Override
        public String toString() {
            return String.format("{schemaTx=%s,systemTx=%s,graphTx=%s}",
                                 this.schemaTx, this.systemTx, this.graphTx);
        }
    }

    private static class SysTransaction extends GraphTransaction {

        public SysTransaction(HugeGraphParams graph, BackendStore store) {
            super(graph, store);
            this.autoCommit(true);
        }
    }
}
