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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import javax.ws.rs.ForbiddenException;

import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyTranslator;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode.Instruction;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.Io;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.auth.HugeAuthenticator.RolePerm;
import com.baidu.hugegraph.auth.HugeAuthenticator.User;
import com.baidu.hugegraph.auth.SchemaDefine.AuthElement;
import com.baidu.hugegraph.backend.cache.Cache;
import com.baidu.hugegraph.backend.cache.CacheManager;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendFeatures;
import com.baidu.hugegraph.backend.store.BackendStoreSystemInfo;
import com.baidu.hugegraph.backend.store.raft.RaftGroupManager;
import com.baidu.hugegraph.config.AuthOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.TypedOption;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.iterator.FilterIterator;
import com.baidu.hugegraph.logger.HugeGraphLogger;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.schema.SchemaLabel;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeFeatures;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.task.HugeTask;
import com.baidu.hugegraph.task.ServerInfoManager;
import com.baidu.hugegraph.task.TaskManager;
import com.baidu.hugegraph.task.TaskScheduler;
import com.baidu.hugegraph.task.TaskStatus;
import com.baidu.hugegraph.traversal.optimize.HugeScriptTraversal;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.Namifiable;
import com.baidu.hugegraph.type.define.GraphMode;
import com.baidu.hugegraph.type.define.GraphReadMode;
import com.baidu.hugegraph.type.define.NodeRole;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.baidu.hugegraph.util.RateLimiter;

public final class HugeGraphAuthProxy implements HugeGraph {

    static {
        HugeGraph.registerTraversalStrategies(HugeGraphAuthProxy.class);
    }

    private static final HugeGraphLogger LOGGER
            = Log.getLogger(HugeGraphAuthProxy.class);
    private final Cache<Id, UserWithRole> usersRoleCache;
    private final Cache<Id, RateLimiter> auditLimiters;
    private final double auditLogMaxRate;

    private final HugeGraph hugegraph;
    private final TaskSchedulerProxy taskScheduler;
    private AuthManager authManager;

    public HugeGraphAuthProxy(HugeGraph hugegraph) {
        HugeConfig config = (HugeConfig) hugegraph.configuration();
        long expired = config.get(AuthOptions.AUTH_PROXY_CACHE_EXPIRE);
        long capacity = config.get(AuthOptions.AUTH_CACHE_CAPACITY);

        this.hugegraph = hugegraph;
        this.taskScheduler = new TaskSchedulerProxy(hugegraph.taskScheduler());
        this.auditLimiters = this.cache("audit-log-limiter", capacity, -1L);
        this.usersRoleCache = this.cache("users-role", capacity, expired);
        this.hugegraph.proxy(this);

        // TODO: Consider better way to get, use auth client's config now
        this.auditLogMaxRate = config.get(AuthOptions.AUTH_AUDIT_LOG_RATE);
        LOGGER.getServerLogger().logInitAuthProxy(hugegraph.name(), this.auditLogMaxRate);
    }

    @Override
    public HugeGraph hugegraph() {
        this.verifyAdminPermission();
        return this.hugegraph;
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> clazz)
                                               throws IllegalArgumentException {
        this.verifyAnyPermission();
        return this.hugegraph.compute(clazz);
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        this.verifyAnyPermission();
        return this.hugegraph.compute();
    }

    @Override
    public GraphTraversalSource traversal() {
        // Just return proxy
        return new GraphTraversalSourceProxy(this);
    }

    @SuppressWarnings({ "rawtypes", "deprecation" })
    @Override
    public <I extends Io> I io(final Io.Builder<I> builder) {
        this.verifyAnyPermission();
        return this.hugegraph.io(builder);
    }

    @Override
    public SchemaManager schema() {
        SchemaManager schema = this.hugegraph.schema();
        schema.proxy(this);
        return schema;
    }

    @Override
    public Id getNextId(HugeType type) {
        if (type == HugeType.TASK) {
            verifyPermission(HugePermission.WRITE, ResourceType.TASK);
        } else {
            this.verifyAdminPermission();
        }
        return this.hugegraph.getNextId(type);
    }

    @Override
    public Id addPropertyKey(PropertyKey key) {
        verifySchemaPermission(HugePermission.WRITE, key);
        return this.hugegraph.addPropertyKey(key);
    }

    @Override
    public Id removePropertyKey(Id key) {
        PropertyKey pkey = this.hugegraph.propertyKey(key);
        verifySchemaPermission(HugePermission.DELETE, pkey);
        return this.hugegraph.removePropertyKey(key);
    }

    @Override
    public Id clearPropertyKey(PropertyKey propertyKey) {
        verifySchemaPermission(HugePermission.DELETE, propertyKey);
        return this.hugegraph.clearPropertyKey(propertyKey);
    }

    @Override
    public Collection<PropertyKey> propertyKeys() {
        Collection<PropertyKey> pkeys = this.hugegraph.propertyKeys();
        return verifySchemaPermission(HugePermission.READ, pkeys);
    }

    @Override
    public PropertyKey propertyKey(String key) {
        return verifySchemaPermission(HugePermission.READ, () -> {
            return this.hugegraph.propertyKey(key);
        });
    }

    @Override
    public PropertyKey propertyKey(Id key) {
        return verifySchemaPermission(HugePermission.READ, () -> {
            return this.hugegraph.propertyKey(key);
        });
    }

    @Override
    public boolean existsPropertyKey(String key) {
        verifyNameExistsPermission(ResourceType.PROPERTY_KEY, key);
        return this.hugegraph.existsPropertyKey(key);
    }

    @Override
    public boolean existsOlapTable(PropertyKey key) {
        verifySchemaPermission(HugePermission.READ, key);
        return this.hugegraph.existsOlapTable(key);
    }

    @Override
    public void addVertexLabel(VertexLabel label) {
        verifySchemaPermission(HugePermission.WRITE, label);
        this.hugegraph.addVertexLabel(label);
    }

    @Override
    public Id removeVertexLabel(Id id) {
        VertexLabel label = this.hugegraph.vertexLabel(id);
        verifySchemaPermission(HugePermission.DELETE, label);
        return this.hugegraph.removeVertexLabel(id);
    }

    @Override
    public Collection<VertexLabel> vertexLabels() {
        Collection<VertexLabel> labels = this.hugegraph.vertexLabels();
        return verifySchemaPermission(HugePermission.READ, labels);
    }

    @Override
    public VertexLabel vertexLabel(String label) {
        return verifySchemaPermission(HugePermission.READ, () -> {
            return this.hugegraph.vertexLabel(label);
        });
    }

    @Override
    public VertexLabel vertexLabel(Id label) {
        return verifySchemaPermission(HugePermission.READ, () -> {
            return this.hugegraph.vertexLabel(label);
        });
    }

    @Override
    public VertexLabel vertexLabelOrNone(Id label) {
        return verifySchemaPermission(HugePermission.READ, () -> {
            return this.hugegraph.vertexLabelOrNone(label);
        });
    }

    @Override
    public boolean existsVertexLabel(String label) {
        verifyNameExistsPermission(ResourceType.VERTEX_LABEL, label);
        return this.hugegraph.existsVertexLabel(label);
    }

    @Override
    public boolean existsLinkLabel(Id vertexLabel) {
        verifyNameExistsPermission(ResourceType.VERTEX_LABEL,
                                   this.vertexLabel(vertexLabel).name());
        return this.hugegraph.existsLinkLabel(vertexLabel);
    }

    @Override
    public void addEdgeLabel(EdgeLabel label) {
        verifySchemaPermission(HugePermission.WRITE, label);
        this.hugegraph.addEdgeLabel(label);
    }

    @Override
    public Id removeEdgeLabel(Id id) {
        EdgeLabel label = this.hugegraph.edgeLabel(id);
        verifySchemaPermission(HugePermission.DELETE, label);
        return this.hugegraph.removeEdgeLabel(id);
    }

    @Override
    public Collection<EdgeLabel> edgeLabels() {
        Collection<EdgeLabel> labels = this.hugegraph.edgeLabels();
        return verifySchemaPermission(HugePermission.READ, labels);
    }

    @Override
    public EdgeLabel edgeLabel(String label) {
        return verifySchemaPermission(HugePermission.READ, () -> {
            return this.hugegraph.edgeLabel(label);
        });
    }

    @Override
    public EdgeLabel edgeLabel(Id label) {
        return verifySchemaPermission(HugePermission.READ, () -> {
            return this.hugegraph.edgeLabel(label);
        });
    }

    @Override
    public EdgeLabel edgeLabelOrNone(Id label) {
        return verifySchemaPermission(HugePermission.READ, () -> {
            return this.hugegraph.edgeLabelOrNone(label);
        });
    }

    @Override
    public boolean existsEdgeLabel(String label) {
        verifyNameExistsPermission(ResourceType.EDGE_LABEL, label);
        return this.hugegraph.existsEdgeLabel(label);
    }

    @Override
    public void addIndexLabel(SchemaLabel schemaLabel, IndexLabel indexLabel) {
        verifySchemaPermission(HugePermission.WRITE, indexLabel);
        this.hugegraph.addIndexLabel(schemaLabel, indexLabel);
    }

    @Override
    public Id removeIndexLabel(Id id) {
        IndexLabel label = this.hugegraph.indexLabel(id);
        verifySchemaPermission(HugePermission.DELETE, label);
        return this.hugegraph.removeIndexLabel(id);
    }

    @Override
    public Id rebuildIndex(SchemaElement schema) {
        if (schema.type() == HugeType.INDEX_LABEL) {
            verifySchemaPermission(HugePermission.WRITE, schema);
        } else {
            SchemaLabel label = (SchemaLabel) schema;
            for (Id il : label.indexLabels()) {
                IndexLabel indexLabel = this.hugegraph.indexLabel(il);
                verifySchemaPermission(HugePermission.WRITE, indexLabel);
            }
        }
        return this.hugegraph.rebuildIndex(schema);
    }

    @Override
    public Collection<IndexLabel> indexLabels() {
        Collection<IndexLabel> labels = this.hugegraph.indexLabels();
        return verifySchemaPermission(HugePermission.READ, labels);
    }

    @Override
    public IndexLabel indexLabel(String label) {
        return verifySchemaPermission(HugePermission.READ, () -> {
            return this.hugegraph.indexLabel(label);
        });
    }

    @Override
    public IndexLabel indexLabel(Id label) {
        return verifySchemaPermission(HugePermission.READ, () -> {
            return this.hugegraph.indexLabel(label);
        });
    }

    @Override
    public boolean existsIndexLabel(String label) {
        verifyNameExistsPermission(ResourceType.INDEX_LABEL, label);
        return this.hugegraph.existsIndexLabel(label);
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        return verifyElemPermission(HugePermission.WRITE, () -> {
            return (HugeVertex) this.hugegraph.addVertex(keyValues);
        });
    }

    @Override
    public void removeVertex(Vertex vertex) {
        verifyElemPermission(HugePermission.DELETE, vertex);
        this.hugegraph.removeVertex(vertex);
    }

    @Override
    public void removeVertex(String label, Object id) {
        this.removeVertex(this.vertex(id));
    }

    @Override
    public <V> void addVertexProperty(VertexProperty<V> property) {
        verifyElemPermission(HugePermission.WRITE, property.element());
        this.hugegraph.addVertexProperty(property);
    }

    @Override
    public <V> void removeVertexProperty(VertexProperty<V> property) {
        verifyElemPermission(HugePermission.WRITE, property.element());
        this.hugegraph.removeVertexProperty(property);
    }

    @Override
    public Edge addEdge(Edge edge) {
        return verifyElemPermission(HugePermission.WRITE, () -> {
            return (HugeEdge) this.hugegraph.addEdge(edge);
        });
    }

    @Override
    public void canAddEdge(Edge edge) {
        verifyElemPermission(HugePermission.WRITE, () -> (HugeEdge) edge);
    }

    @Override
    public void removeEdge(Edge edge) {
        verifyElemPermission(HugePermission.DELETE, edge);
        this.hugegraph.removeEdge(edge);
    }

    @Override
    public void removeEdge(String label, Object id) {
        this.removeEdge(this.edge(id));
    }

    @Override
    public <V> void addEdgeProperty(Property<V> property) {
        verifyElemPermission(HugePermission.WRITE, property.element());
        this.hugegraph.addEdgeProperty(property);
    }

    @Override
    public <V> void removeEdgeProperty(Property<V> property) {
        verifyElemPermission(HugePermission.WRITE, property.element());
        this.hugegraph.removeEdgeProperty(property);
    }

    @Override
    public Iterator<Vertex> vertices(Query query) {
        return verifyElemPermission(HugePermission.READ,
                                    this.hugegraph.vertices(query));
    }

    @Override
    public Iterator<Vertex> vertices(Object... objects) {
        return verifyElemPermission(HugePermission.READ,
                                    this.hugegraph.vertices(objects));
    }

    @Override
    public Vertex vertex(Object object) {
        Vertex vertex = this.hugegraph.vertex(object);
        verifyElemPermission(HugePermission.READ, vertex);
        return vertex;
    }

    @Override
    public Iterator<Vertex> adjacentVertex(Object id) {
        return verifyElemPermission(HugePermission.READ,
                                    this.hugegraph.adjacentVertex(id));
    }

    @Override
    public Iterator<Vertex> adjacentVertices(Iterator<Edge> edges) {
        Iterator<Vertex> vertices = this.hugegraph.adjacentVertices(edges);
        return verifyElemPermission(HugePermission.READ, vertices);
    }

    @Override
    public boolean checkAdjacentVertexExist() {
        verifyAnyPermission();
        return this.hugegraph.checkAdjacentVertexExist();
    }

    @Override
    public Iterator<Edge> edges(Query query) {
        return verifyElemPermission(HugePermission.READ,
                                    this.hugegraph.edges(query));
    }

    @Override
    public Iterator<Edge> edges(Object... objects) {
        return verifyElemPermission(HugePermission.READ,
                                    this.hugegraph.edges(objects));
    }

    @Override
    public Edge edge(Object id) {
        Edge edge = this.hugegraph.edge(id);
        verifyElemPermission(HugePermission.READ, edge);
        return edge;
    }

    @Override
    public Iterator<Edge> adjacentEdges(Id vertexId) {
        Iterator<Edge> edges = this.hugegraph.adjacentEdges(vertexId);
        return verifyElemPermission(HugePermission.READ, edges);
    }

    @Override
    public Number queryNumber(Query query) {
        ResourceType resType;
        if (query.resultType().isVertex()) {
            resType = ResourceType.VERTEX_AGGR;
        } else {
            assert query.resultType().isEdge();
            resType = ResourceType.EDGE_AGGR;
        }
        this.verifyPermission(HugePermission.READ, resType);
        return this.hugegraph.queryNumber(query);

    }

    @Override
    public Transaction tx() {
        /*
         * Can't verifyPermission() here, will be called by rollbackAll().
         */
        return this.hugegraph.tx();
    }

    @Override
    public void close() throws Exception {
        this.verifyAdminPermission();
        this.hugegraph.close();
    }

    @Override
    public HugeFeatures features() {
        // Can't verifyPermission() here, will be called by rollbackAll()
        //verifyStatusPermission();
        return this.hugegraph.features();
    }

    @Override
    public Variables variables() {
        // Just return proxy
        return new VariablesProxy(this.hugegraph.variables());
    }

    @Override
    public HugeConfig configuration() {
        this.verifyAnyPermission();
        return (HugeConfig) this.hugegraph.configuration();
    }

    @Override
    public String toString() {
        this.verifyAnyPermission();
        return this.hugegraph.toString();
    }

    @Override
    public void proxy(HugeGraph graph) {
        throw new NotSupportException("Graph.proxy()");
    }

    @Override
    public boolean sameAs(HugeGraph graph) {
        if (graph instanceof HugeGraphAuthProxy) {
            graph = ((HugeGraphAuthProxy) graph).hugegraph;
        }
        return this.hugegraph.sameAs(graph);
    }

    @Override
    public long now() {
        // It's ok anyone call this method, so not verifyStatusPermission()
        return this.hugegraph.now();
    }

    @Override
    public <K, V> V option(TypedOption<K, V> option) {
        this.verifyAnyPermission();
        return this.hugegraph.option(option);
    }

    @Override
    public String graphSpace() {
        // none verify permission
        return this.hugegraph.graphSpace();
    }

    @Override
    public void graphSpace(String graphSpace) {
        this.hugegraph.graphSpace(graphSpace);
    }

    @Override
    public String name() {
        this.verifyAnyPermission();
        return this.hugegraph.name();
    }

    @Override
    public String backend() {
        this.verifyAnyPermission();
        return this.hugegraph.backend();
    }

    @Override
    public String backendVersion() {
        this.verifyAnyPermission();
        return this.hugegraph.backendVersion();
    }

    @Override
    public BackendStoreSystemInfo backendStoreSystemInfo() {
        this.verifyAdminPermission();
        return this.hugegraph.backendStoreSystemInfo();
    }

    @Override
    public BackendFeatures backendStoreFeatures() {
        this.verifyAnyPermission();
        return this.hugegraph.backendStoreFeatures();
    }

    @Override
    public GraphMode mode() {
        this.verifyStatusPermission();
        return this.hugegraph.mode();
    }

    @Override
    public void mode(GraphMode mode) {
        this.verifyPermission(HugePermission.WRITE, ResourceType.STATUS);
        this.hugegraph.mode(mode);
    }

    @Override
    public GraphReadMode readMode() {
        this.verifyStatusPermission();
        return this.hugegraph.readMode();
    }

    @Override
    public void readMode(GraphReadMode readMode) {
        this.verifyPermission(HugePermission.WRITE, ResourceType.STATUS);
        this.hugegraph.readMode(readMode);
    }

    @Override
    public void waitStarted() {
        this.verifyAnyPermission();
        this.hugegraph.waitStarted();
    }

    @Override
    public void serverStarted(Id serverId, NodeRole serverRole) {
        this.verifyAdminPermission();
        this.hugegraph.serverStarted(serverId, serverRole);
    }

    @Override
    public boolean started() {
        this.verifyAdminPermission();
        return this.hugegraph.started();
    }

    @Override
    public boolean closed() {
        this.verifyAdminPermission();
        return this.hugegraph.closed();
    }

    @Override
    public <R> R metadata(HugeType type, String meta, Object... args) {
        this.verifyNamePermission(HugePermission.EXECUTE,
                                  ResourceType.META, meta);
        return this.hugegraph.metadata(type, meta, args);
    }

    @Override
    public TaskScheduler taskScheduler() {
        // Just return proxy
        return this.taskScheduler;
    }

    @Override
    public AuthManager authManager() {
        return this.authManager;
    }

    @Override
    public void switchAuthManager(AuthManager authManager) {
        this.authManager = authManager;
    }

    @Override
    public RaftGroupManager raftGroupManager(String group) {
        this.verifyAdminPermission();
        return this.hugegraph.raftGroupManager(group);
    }

    @Override
    public void initBackend() {
        this.verifyAdminPermission();
        this.hugegraph.initBackend();
    }

    @Override
    public void clearBackend() {
        this.verifyAdminPermission();
        this.hugegraph.clearBackend();
    }

    @Override
    public void truncateBackend() {
        this.verifyAdminPermission();
        this.hugegraph.truncateBackend();
    }

    @Override
    public void truncateGraph() {
        this.verifyAdminPermission();
        this.hugegraph.truncateGraph();
    }

    @Override
    public void createSnapshot() {
        this.verifyPermission(HugePermission.WRITE, ResourceType.STATUS);
        this.hugegraph.createSnapshot();
    }

    @Override
    public void resumeSnapshot() {
        this.verifyPermission(HugePermission.WRITE, ResourceType.STATUS);
        this.hugegraph.resumeSnapshot();
    }

    private <V> Cache<Id, V> cache(String prefix, long capacity,
                                   long expiredTime) {
        String name = prefix + "-" + this.hugegraph.name();
        Cache<Id, V> cache = CacheManager.instance().cache(name, capacity);
        if (expiredTime > 0L) {
            cache.expire(Duration.ofSeconds(expiredTime).toMillis());
        } else {
            cache.expire(expiredTime);
        }
        return cache;
    }

    private void verifyAdminPermission() {
        verifyPermission(HugePermission.ANY, ResourceType.ROOT);
    }

    private void verifyStatusPermission() {
        verifyPermission(HugePermission.READ, ResourceType.STATUS);
    }

    private void verifyAnyPermission() {
        verifyPermission(HugePermission.READ, ResourceType.NONE);
    }

    private void verifyPermission(HugePermission actionPerm,
                                  ResourceType resType) {
        /*
         * The owner role should match the graph name
         * NOTE: the graph names in gremlin-server.yaml/graphs and
         * hugegraph.properties/store must be the same if enable auth.
         */
        verifyResPermission(actionPerm, true, () -> {
            String graphSpace = this.hugegraph.graphSpace();
            String graph = this.hugegraph.name();
            Namifiable elem = HugeResource.NameObject.ANY;
            return ResourceObject.of(graphSpace, graph, resType, elem);
        });
    }

    private <V extends AuthElement> V verifyUserPermission(
                                      HugePermission actionPerm,
                                      V elementFetcher) {
        return verifyUserPermission(actionPerm, true, () -> elementFetcher);
    }

    private <V extends AuthElement> List<V> verifyUserPermission(
                                            HugePermission actionPerm,
                                            List<V> elems) {
        List<V> results = new ArrayList<>();
        for (V elem : elems) {
            V r = verifyUserPermission(actionPerm, false, () -> elem);
            if (r != null) {
                results.add(r);
            }
        }
        return results;
    }

    private <V extends AuthElement> V verifyUserPermission(
                                      HugePermission actionPerm,
                                      boolean throwIfNoPerm,
                                      Supplier<V> elementFetcher) {
        return verifyResPermission(actionPerm, throwIfNoPerm, () -> {
            String graphSpace = this.hugegraph.graphSpace();
            String graph = this.hugegraph.name();
            V elem = elementFetcher.get();
            @SuppressWarnings("unchecked")
            ResourceObject<V> r = (ResourceObject<V>)
                              ResourceObject.of(graphSpace, graph, elem);
            return r;
        });
    }

    private void verifyElemPermission(HugePermission actionPerm, Element elem) {
        verifyElemPermission(actionPerm, true, () -> elem);
    }

    private <V extends HugeElement> V verifyElemPermission(
                                      HugePermission actionPerm,
                                      Supplier<V> elementFetcher) {
        return verifyElemPermission(actionPerm, true, elementFetcher);
    }

    private <V extends Element> Iterator<V> verifyElemPermission(
                                            HugePermission actionPerm,
                                            Iterator<V> elems) {
        return new FilterIterator<>(elems, elem -> {
            V r = verifyElemPermission(actionPerm, false, () -> elem);
            return r != null;
        });
    }

    private <V extends Element> V verifyElemPermission(
                                  HugePermission actionPerm,
                                  boolean throwIfNoPerm,
                                  Supplier<V> elementFetcher) {
        return verifyResPermission(actionPerm, throwIfNoPerm, () -> {
            String graphSpace = this.hugegraph.graphSpace();
            String graph = this.hugegraph.name();
            HugeElement elem = (HugeElement) elementFetcher.get();
            @SuppressWarnings("unchecked")
            ResourceObject<V> r = (ResourceObject<V>)
                              ResourceObject.of(graphSpace, graph, elem);
            return r;
        });
    }

    private void verifyNameExistsPermission(ResourceType resType, String name) {
        verifyNamePermission(HugePermission.READ, resType, name);
    }

    private void verifyNamePermission(HugePermission actionPerm,
                                      ResourceType resType, String name) {
        verifyResPermission(actionPerm, true, () -> {
            String graphSpace = this.hugegraph.graphSpace();
            String graph = this.hugegraph.name();
            Namifiable elem = HugeResource.NameObject.of(name);
            return ResourceObject.of(graphSpace, graph, resType, elem);
        });
    }

    private void verifySchemaPermission(HugePermission actionPerm,
                                        SchemaElement schema) {
        verifySchemaPermission(actionPerm, true, () -> schema);
    }

    private <V extends SchemaElement> Collection<V> verifySchemaPermission(
                                                    HugePermission actionPerm,
                                                    Collection<V> schemas) {
        List<V> results = new ArrayList<>();
        for (V schema : schemas) {
            V r = verifySchemaPermission(actionPerm, false, () -> schema);
            if (r != null) {
                results.add(r);
            }
        }
        return results;
    }

    private <V extends SchemaElement> V verifySchemaPermission(
                                        HugePermission actionPerm,
                                        Supplier<V> schemaFetcher) {
        return verifySchemaPermission(actionPerm, true, schemaFetcher);
    }

    private <V extends SchemaElement> V verifySchemaPermission(
                                        HugePermission actionPerm,
                                        boolean throwIfNoPerm,
                                        Supplier<V> schemaFetcher) {
        return verifyResPermission(actionPerm, throwIfNoPerm, () -> {
            String graphSpace = this.hugegraph.graphSpace();
            String graph = this.hugegraph.name();
            SchemaElement elem = schemaFetcher.get();
            @SuppressWarnings("unchecked")
            ResourceObject<V> r = (ResourceObject<V>)
                              ResourceObject.of(graphSpace, graph, elem);
            return r;
        });
    }

    private <V> V verifyResPermission(HugePermission actionPerm,
                                      boolean throwIfNoPerm,
                                      Supplier<ResourceObject<V>> fetcher) {
        return verifyResPermission(actionPerm, throwIfNoPerm, fetcher, null);
    }

    private <V> V verifyResPermission(HugePermission actionPerm,
                                      boolean throwIfNoPerm,
                                      Supplier<ResourceObject<V>> fetcher,
                                      Supplier<Boolean> checker) {
        // TODO: call verifyPermission() before actual action
        Context context = getContext();
        E.checkState(context != null,
                     "Missing authentication context " +
                     "when verifying resource permission");
        String username = context.user().username();
        Object role = context.user().role();
        ResourceObject<V> ro = fetcher.get();
        String action = actionPerm.string();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.logCustomDebug(
                "Verify permission {} {} for user '{}' with role {}",
                "Jermy Li",
                action, ro, username, role);
        }

        V result = ro.operated();
        // Verify role permission
        if (!RolePerm.match(role, actionPerm, ro)) {
            result = null;
        }
        // Verify permission for one access another, like: granted <= user role
        else if (ro.type().isGrantOrUser()) {
            AuthElement element = (AuthElement) ro.operated();
            RolePermission grant = this.authManager().rolePermission(element);
            if (!RolePerm.match(role, grant, ro)) {
                result = null;
            }
        }

        // Check resource detail if needed
        if (result != null && checker != null && !checker.get()) {
            result = null;
        }

        // Log user action, limit rate for each user
        Id usrId = context.user().userId();
        RateLimiter auditLimiter = this.auditLimiters.getOrFetch(usrId, id -> {
            return RateLimiter.create(this.auditLogMaxRate);
        });

        if (!(actionPerm == HugePermission.READ && ro.type().isSchema()) &&
            auditLimiter.tryAcquire()) {
            String status = result == null ? "denied" : "allowed";
            LOGGER.getServerLogger().logVerifyResPermission(
                username, status, action ,ro);
        }

        // result = null means no permission, throw if needed
        if (result == null && throwIfNoPerm) {
            String error = String.format("Permission denied: %s %s",
                                         action, ro);
            throw new ForbiddenException(error);
        }
        return result;
    }

    class TaskSchedulerProxy extends TaskScheduler {

        private final TaskScheduler taskScheduler;

        public TaskSchedulerProxy(TaskScheduler origin) {
            super(origin);
            this.taskScheduler = origin;
        }

        @Override
        public HugeGraph graph() {
            return this.taskScheduler.graph();
        }

        @Override
        public int pendingTasks() {
            verifyTaskPermission(HugePermission.READ);
            return this.taskScheduler.pendingTasks();
        }

        @Override
        public <V> void restoreTasks() {
            verifyTaskPermission(HugePermission.WRITE);
            this.taskScheduler.restoreTasks();
        }

        @Override
        public <V> Future<?> schedule(HugeTask<V> task) {
            verifyTaskPermission(HugePermission.EXECUTE);
            task.context(getContextString());
            return this.taskScheduler.schedule(task);
        }

        @Override
        public <V> void cancel(HugeTask<V> task) {
            verifyTaskPermission(HugePermission.WRITE, task);
            this.taskScheduler.cancel(task);
        }

        @Override
        public <V> void save(HugeTask<V> task) {
            verifyTaskPermission(HugePermission.WRITE, task);
            this.taskScheduler.save(task);
        }

        @Override
        public <V> HugeTask<V> task(Id id) {
            return verifyTaskPermission(HugePermission.READ,
                                        this.taskScheduler.task(id));
        }

        @Override
        public <V> Iterator<HugeTask<V>> tasks(List<Id> ids) {
            return verifyTaskPermission(HugePermission.READ,
                                        this.taskScheduler.tasks(ids));
        }

        @Override
        public <V> Iterator<HugeTask<V>> tasks(TaskStatus status,
                                               long limit, String page) {
            Iterator<HugeTask<V>> tasks = this.taskScheduler.tasks(status,
                                                                   limit, page);
            return verifyTaskPermission(HugePermission.READ, tasks);
        }

        @Override
        public <V> HugeTask<V> delete(Id id, boolean force) {
            verifyTaskPermission(HugePermission.DELETE,
                                 this.taskScheduler.task(id));
            return this.taskScheduler.delete(id, force);
        }

        @Override
        public boolean close() {
            verifyAdminPermission();
            return this.taskScheduler.close();
        }

        @Override
        public <V> HugeTask<V> waitUntilTaskCompleted(Id id, long seconds)
                                                      throws TimeoutException {
            verifyAnyPermission();
            return this.taskScheduler.waitUntilTaskCompleted(id, seconds);
        }

        @Override
        public <V> HugeTask<V> waitUntilTaskCompleted(Id id)
                                                      throws TimeoutException {
            verifyAnyPermission();
            return this.taskScheduler.waitUntilTaskCompleted(id);
        }

        @Override
        public void waitUntilAllTasksCompleted(long seconds)
                                               throws TimeoutException {
            verifyAnyPermission();
            this.taskScheduler.waitUntilAllTasksCompleted(seconds);
        }

        @Override
        public void checkRequirement(String op) {
            verifyAnyPermission();
            this.taskScheduler.checkRequirement(op);
        }

        private void verifyTaskPermission(HugePermission actionPerm) {
            verifyPermission(actionPerm, ResourceType.TASK);
        }

        private <V> HugeTask<V> verifyTaskPermission(HugePermission actionPerm,
                                                     HugeTask<V> task) {
            return verifyTaskPermission(actionPerm, true, task);
        }

        private <V> Iterator<HugeTask<V>> verifyTaskPermission(
                                          HugePermission actionPerm,
                                          Iterator<HugeTask<V>> tasks) {
            return new FilterIterator<>(tasks, task -> {
                return verifyTaskPermission(actionPerm, false, task) != null;
            });
        }

        private <V> HugeTask<V> verifyTaskPermission(HugePermission actionPerm,
                                                     boolean throwIfNoPerm,
                                                     HugeTask<V> task) {
            Object r = verifyResPermission(actionPerm, throwIfNoPerm, () -> {
                String graphSpace = HugeGraphAuthProxy.this.hugegraph
                                                           .graphSpace();
                String graph = HugeGraphAuthProxy.this.hugegraph.name();
                String name = task.id().toString();
                Namifiable elem = HugeResource.NameObject.of(name);
                return ResourceObject.of(graphSpace, graph,
                                         ResourceType.TASK, elem);
            }, () -> {
                return hasTaskPermission(task);
            });
            return r == null ? null : task;
        }

        private boolean hasTaskPermission(HugeTask<?> task) {
            Context context = getContext();
            if (context == null) {
                return false;
            }
            User currentUser = context.user();

            User taskUser = User.fromJson(task.context());
            if (taskUser == null) {
                return User.ADMIN.equals(currentUser);
            }

            return Objects.equals(currentUser.getName(), taskUser.getName()) ||
                   RolePerm.match(currentUser.role(), taskUser.role(), null);
        }

        @Override
        protected ServerInfoManager serverManager() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        protected <V> V call(Callable<V> callable) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        protected void taskDone(HugeTask<?> task) {
            // TODO Auto-generated method stub
            
        }
    }

    class VariablesProxy implements Variables {

        private final Variables variables;

        public VariablesProxy(Variables variables) {
            this.variables = variables;
        }

        @Override
        public <R> Optional<R> get(String key) {
            verifyPermission(HugePermission.READ, ResourceType.VAR);
            return this.variables.get(key);
        }

        @Override
        public Set<String> keys() {
            verifyPermission(HugePermission.READ, ResourceType.VAR);
            return this.variables.keys();
        }

        @Override
        public void set(String key, Object value) {
            verifyPermission(HugePermission.WRITE, ResourceType.VAR);
            this.variables.set(key, value);
        }

        @Override
        public void remove(String key) {
            verifyPermission(HugePermission.DELETE, ResourceType.VAR);
            this.variables.remove(key);
        }
    }

    class GraphTraversalSourceProxy extends GraphTraversalSource {

        public GraphTraversalSourceProxy(Graph graph) {
            super(graph);
        }

        public GraphTraversalSourceProxy(Graph graph,
                                         TraversalStrategies strategies) {
            super(graph, strategies);
        }

        @Override
        public TraversalStrategies getStrategies() {
            // getStrategies()/getGraph() is called by super.clone()
            return new TraversalStrategiesProxy(super.getStrategies());
        }
    }

    class TraversalStrategiesProxy implements TraversalStrategies {

        private static final String REST_WORKER = "grizzly-http-server";
        private static final long serialVersionUID = -5424364720492307019L;
        private final TraversalStrategies strategies;

        public TraversalStrategiesProxy(TraversalStrategies strategies) {
            this.strategies = strategies;
        }

        @Override
        public List<TraversalStrategy<?>> toList() {
            return this.strategies.toList();
        }

        @Override
        public void applyStrategies(Admin<?, ?> traversal) {
            String script;
            if (traversal instanceof HugeScriptTraversal) {
                script = ((HugeScriptTraversal<?, ?>) traversal).script();
            } else {
                GroovyTranslator translator = GroovyTranslator.of("g");
                script = translator.translate(traversal.getBytecode());
            }

            /*
             * Verify gremlin-execute permission for user gremlin(in gremlin-
             * server-exec worker) and gremlin job(in task worker).
             * But don't check permission in rest worker, because the following
             * places need to call traversal():
             *  1.vertices/edges rest api
             *  2.oltp rest api (like crosspointpath/neighborrank)
             *  3.olap rest api (like centrality/lpa/louvain/subgraph)
             */
            String caller = Thread.currentThread().getName();
            if (!caller.contains(REST_WORKER)) {
                verifyNamePermission(HugePermission.EXECUTE,
                                     ResourceType.GREMLIN, script);
            }

            this.strategies.applyStrategies(traversal);
        }

        @Override
        public TraversalStrategies addStrategies(TraversalStrategy<?>...
                                                 strategies) {
            return this.strategies.addStrategies(strategies);
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        @Override
        public TraversalStrategies removeStrategies(
               Class<? extends TraversalStrategy>... strategyClasses) {
            return this.strategies.removeStrategies(strategyClasses);
        }

        @Override
        public TraversalStrategies clone() {
            return this.strategies.clone();
        }

        @SuppressWarnings("unused")
        private String translate(Bytecode bytecode) {
            // GroovyTranslator.of("g").translate(bytecode);
            List<Instruction> steps = bytecode.getStepInstructions();
            StringBuilder sb = new StringBuilder();
            sb.append("g");
            int stepsPrint = Math.min(10, steps.size());
            for (int i = 0; i < stepsPrint; i++) {
                Instruction step = steps.get(i);
                sb.append('.').append(step);
            }
            if (stepsPrint < steps.size()) {
                sb.append("..");
            }
            return sb.toString();
        }
    }

    private static final ThreadLocal<Context> contexts =
                                              new InheritableThreadLocal<>();

    protected static final Context setContext(Context context) {
        Context old = contexts.get();
        contexts.set(context);
        return old;
    }

    protected static final void resetContext() {
        contexts.remove();
    }

    protected static final Context getContext() {
        // Return task context first
        String taskContext = TaskManager.getContext();
        User user = User.fromJson(taskContext);
        if (user != null) {
            return new Context(user);
        }

        return contexts.get();
    }

    protected static final String getContextString() {
        Context context = getContext();
        if (context == null) {
            return null;
        }
        return context.user().toJson();
    }

    protected static final void logUser(User user, String path) {
        LOGGER.getAuditLogger().logUserLogin(user.username(), user.client(), path);
    }

    static class Context {

        private static final Context ADMIN = new Context(User.ADMIN);

        private final User user;

        public Context(User user) {
            E.checkNotNull(user, "user");
            this.user = user;
        }

        public User user() {
            return this.user;
        }

        public static Context admin() {
            return ADMIN;
        }
    }

    static class ContextTask implements Runnable {

        private final Runnable runner;
        private final Context context;

        public ContextTask(Runnable runner) {
            this.context = getContext();
            this.runner = runner;
        }

        @Override
        public void run() {
            setContext(this.context);
            try {
                this.runner.run();
            } finally {
                resetContext();
            }
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
}
