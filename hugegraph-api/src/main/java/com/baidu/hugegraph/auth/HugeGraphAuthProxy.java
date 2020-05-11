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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import javax.ws.rs.ForbiddenException;

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.auth.HugeAuthenticator.RolePerm;
import com.baidu.hugegraph.auth.HugeAuthenticator.User;
import com.baidu.hugegraph.auth.HugeResource.RolePermission;
import com.baidu.hugegraph.auth.ResourceObject.ResourceType;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendFeatures;
import com.baidu.hugegraph.backend.store.BackendStoreSystemInfo;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.iterator.FilterIterator;
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
import com.baidu.hugegraph.task.TaskManager;
import com.baidu.hugegraph.task.TaskScheduler;
import com.baidu.hugegraph.task.TaskStatus;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.Namifiable;
import com.baidu.hugegraph.type.define.GraphMode;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public final class HugeGraphAuthProxy implements HugeGraph {

    static {
        HugeGraph.registerTraversalStrategies(HugeGraphAuthProxy.class);
    }

    private static final Logger LOG = Log.logger(HugeGraph.class);

    private final HugeGraph hugegraph;
    private final TaskScheduler taskScheduler;

    public HugeGraphAuthProxy(HugeGraph hugegraph) {
        LOG.info("Wrap graph '{}' with HugeGraphAuthProxy", hugegraph.name());
        this.hugegraph = hugegraph;
        this.taskScheduler = new TaskSchedulerProxy(hugegraph.taskScheduler());
        this.hugegraph.proxy(this);
    }

    @Override
    public HugeGraph hugegraph() {
        verifyAdminPermission();
        return this.hugegraph;
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> clazz)
                                               throws IllegalArgumentException {
        this.verifyStatusPermission();
        return this.hugegraph.compute(clazz);
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        this.verifyStatusPermission();
        return this.hugegraph.compute();
    }

    @Override
    public GraphTraversalSource traversal() {
        verifyPermission(HugePermission.EXECUTE, ResourceType.GREMLIN);
        // Just return proxy
        return new GraphTraversalSourceProxy(this);
    }

    @SuppressWarnings({ "rawtypes", "deprecation" })
    @Override
    public <I extends Io> I io(final Io.Builder<I> builder) {
        this.verifyStatusPermission();
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
    public void addPropertyKey(PropertyKey key) {
        verifySchemaPermission(HugePermission.WRITE, key);
        this.hugegraph.addPropertyKey(key);
    }

    @Override
    public void removePropertyKey(Id key) {
        PropertyKey pkey = this.hugegraph.propertyKey(key);
        verifySchemaPermission(HugePermission.DELETE, pkey);
        this.hugegraph.removePropertyKey(key);
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
        verifySchemaExistsPermission(ResourceType.PROPERTY_KEY, key);
        return this.hugegraph.existsPropertyKey(key);
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
        verifySchemaExistsPermission(ResourceType.VERTEX_LABEL, label);
        return this.hugegraph.existsVertexLabel(label);
    }

    @Override
    public boolean existsLinkLabel(Id vertexLabel) {
        verifySchemaExistsPermission(ResourceType.VERTEX_LABEL,
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
        verifySchemaExistsPermission(ResourceType.EDGE_LABEL, label);
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
        verifySchemaExistsPermission(ResourceType.INDEX_LABEL, label);
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
        this.hugegraph.addEdge(edge);
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
        verifyStatusPermission();
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
        verifyAdminPermission();
        this.hugegraph.close();
    }

    @Override
    public HugeFeatures features() {
        // Can't verifyPermission() here, will be called by rollbackAll()
        verifyPermission(HugePermission.READ, ResourceType.STATUS);
        return this.hugegraph.features();
    }

    @Override
    public Variables variables() {
        this.verifyStatusPermission();
        // Just return proxy
        return new VariablesProxy(this.hugegraph.variables());
    }

    @Override
    public HugeConfig configuration() {
        throw new NotSupportException("Graph.configuration()");
    }

    @Override
    public String toString() {
        this.verifyStatusPermission();
        return this.hugegraph.toString();
    }

    @Override
    public void proxy(HugeGraph graph) {
        throw new NotSupportException("Graph.proxy()");
    }

    @Override
    public String name() {
        this.verifyStatusPermission();
        return this.hugegraph.name();
    }

    @Override
    public String backend() {
        this.verifyStatusPermission();
        return this.hugegraph.backend();
    }

    @Override
    public String backendVersion() {
        this.verifyStatusPermission();
        return this.hugegraph.backendVersion();
    }

    @Override
    public boolean backendStoreInitialized() {
        verifyStatusPermission();
        return this.hugegraph.backendStoreInitialized();
    }

    @Override
    public BackendStoreSystemInfo backendStoreSystemInfo() {
        verifyAdminPermission();
        return this.hugegraph.backendStoreSystemInfo();
    }

    @Override
    public BackendFeatures backendStoreFeatures() {
        this.verifyPermission(HugePermission.READ, ResourceType.STATUS);
        return this.hugegraph.backendStoreFeatures();
    }

    @Override
    public GraphMode mode() {
        this.verifyPermission(HugePermission.READ, ResourceType.STATUS);
        return this.hugegraph.mode();
    }

    @Override
    public void mode(GraphMode mode) {
        this.verifyPermission(HugePermission.WRITE, ResourceType.STATUS);
        this.hugegraph.mode(mode);
    }

    @Override
    public boolean closed() {
        verifyAdminPermission();
        return this.hugegraph.closed();
    }

    @Override
    public <R> R metadata(HugeType type, String meta, Object... args) {
        this.verifyPermission(HugePermission.EXECUTE, ResourceType.META, meta);
        return this.hugegraph.metadata(type, meta, args);
    }

    @Override
    public TaskScheduler taskScheduler() {
        // Just return proxy
        return this.taskScheduler;
    }

    @Override
    public RolePermission matchUser(String username, String password) {
        // Can't verifyPermission() here, login first with temp permission
        Context context = setContext(Context.admin());
        try {
            return this.hugegraph.matchUser(username, password);
        } catch (Exception e) {
            LOG.error("Failed to login user {}: ", username, e);
            throw e;
        } finally {
            setContext(context);
        }
    }

    @Override
    public UserManager userManager() {
        // TODO: return proxy
        verifyAdminPermission();
        return this.hugegraph.userManager();
    }

    @Override
    public void initBackend() {
        verifyAdminPermission();
        this.hugegraph.initBackend();
    }

    @Override
    public void clearBackend() {
        verifyAdminPermission();
        this.hugegraph.clearBackend();
    }

    @Override
    public void truncateBackend() {
        verifyAdminPermission();
        this.hugegraph.truncateBackend();
    }

    private void verifyAdminPermission() {
        verifyPermission(HugePermission.ALL, ResourceType.ROOT);
    }

    private void verifyStatusPermission() {
        verifyPermission(HugePermission.READ, ResourceType.STATUS);
    }

    private void verifySchemaExistsPermission(ResourceType resType,
                                              String schema) {
        verifyPermission(HugePermission.READ, resType, schema);
    }

    private void verifyPermission(HugePermission actionPerm,
                                  ResourceType resType) {
        verifyPermission(actionPerm, resType, "*");
    }

    private void verifyPermission(HugePermission actionPerm,
                                  ResourceType resType, String name) {
        /*
         * The owner role should match the graph name
         * NOTE: the graph names in gremlin-server.yaml/graphs and
         * hugegraph.properties/store must be the same if enable auth.
         */
        String graph = this.hugegraph.name();
        verifyResPermission(actionPerm, true, () -> {
            Namifiable elem = new HugeResource.NameObject(name);
            return ResourceObject.of(graph, resType, elem);
        });
    }

    private static void verifyElemPermission(HugePermission actionPerm,
                                             Element elem) {
        verifyElemPermission(actionPerm, true, () -> elem);
    }

    private static <V extends HugeElement> V verifyElemPermission(
                                             HugePermission actionPerm,
                                             Supplier<V> elementFetcher) {
        return verifyElemPermission(actionPerm, true, elementFetcher);
    }

    private static <V extends Element> Iterator<V> verifyElemPermission(
                                                   HugePermission actionPerm,
                                                   Iterator<V> elems) {
        return new FilterIterator<>(elems, elem -> {
            V r = verifyElemPermission(actionPerm, false, () -> elem);
            return r != null;
        });
    }

    private static <V extends Element> V verifyElemPermission(
                                         HugePermission actionPerm,
                                         boolean throwIfNoPermission,
                                         Supplier<V> elementFetcher) {
        ResourceObject<V> ro;
        ro = verifyResPermission(actionPerm, throwIfNoPermission, () -> {
            HugeElement elem = (HugeElement) elementFetcher.get();
            @SuppressWarnings("unchecked")
            ResourceObject<V> r = (ResourceObject<V>) ResourceObject.of(elem);
            return r;
        });
        if (ro != null) {
            return ro.operated();
        }
        return null;
    }

    private static void verifySchemaPermission(HugePermission actionPerm,
                                               SchemaElement schema) {
        verifySchemaPermission(actionPerm, () -> schema);
    }

    private static <V extends SchemaElement> Collection<V>
                                             verifySchemaPermission(
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

    private static <V extends SchemaElement> V verifySchemaPermission(
                                               HugePermission actionPerm,
                                               Supplier<V> schemaFetcher) {
        return verifySchemaPermission(actionPerm, true, schemaFetcher);
    }

    private static <V extends SchemaElement> V verifySchemaPermission(
                                               HugePermission actionPerm,
                                               boolean throwIfNoPermission,
                                               Supplier<V> schemaFetcher) {
        ResourceObject<V> ro;
        ro = verifyResPermission(actionPerm, throwIfNoPermission, () -> {
            SchemaElement elem = schemaFetcher.get();
            @SuppressWarnings("unchecked")
            ResourceObject<V> r = (ResourceObject<V>) ResourceObject.of(elem);
            return r;
        });
        if (ro != null) {
            return ro.operated();
        }
        return null;
    }

    private static <V> ResourceObject<V> verifyResPermission(
                                         HugePermission actionPerm,
                                         boolean throwIfNoPermission,
                                         Supplier<ResourceObject<V>> fetcher) {
        // TODO: call verifyPermission() before actual action
        Context context = getContext();
        E.checkState(context != null,
                     "Missing authentication context " +
                     "when verifying resource permission");
        Object role = context.user().role();
        ResourceObject<V> ro = fetcher.get();
        String action = actionPerm.string();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Verify permission '{}' for role '{}' with resource {}",
                      action, role, ro);
        }
        if (!RolePerm.match(role, actionPerm, ro)) {
            if (throwIfNoPermission) {
                String error = String.format("Permission denied: %s %s",
                                             action, ro);
                throw new ForbiddenException(error);
            } else {
                ro = null;
            }
        }
        return ro;
    }

    class TaskSchedulerProxy implements TaskScheduler {

        private final TaskScheduler taskScheduler;

        public TaskSchedulerProxy(TaskScheduler origin) {
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
            verifyTaskPermission(HugePermission.WRITE);
            task.context(getContextString());
            return this.taskScheduler.schedule(task);
        }

        @Override
        public <V> boolean cancel(HugeTask<V> task) {
            verifyTaskPermission(HugePermission.WRITE);
            return this.taskScheduler.cancel(task);
        }

        @Override
        public <V> void save(HugeTask<V> task) {
            verifyTaskPermission(HugePermission.WRITE);
            this.taskScheduler.save(task);
        }

        @Override
        public <V> HugeTask<V> task(Id id) {
            verifyTaskPermission(HugePermission.READ);
            return this.taskScheduler.task(id);
        }

        @Override
        public <V> Iterator<HugeTask<V>> tasks(List<Id> ids) {
            verifyTaskPermission(HugePermission.READ);
            return this.taskScheduler.tasks(ids);
        }

        @Override
        public <V> Iterator<HugeTask<V>> tasks(TaskStatus status,
                                               long limit, String page) {
            verifyTaskPermission(HugePermission.READ);
            return this.taskScheduler.tasks(status, limit, page);
        }

        @Override
        public <V> HugeTask<V> delete(Id id) {
            verifyTaskPermission(HugePermission.DELETE);
            return this.taskScheduler.delete(id);
        }

        @Override
        public boolean close() {
            verifyAdminPermission();
            return this.taskScheduler.close();
        }

        @Override
        public int taskInputSizeLimit() {
            verifyStatusPermission();
            return this.taskScheduler.taskInputSizeLimit();
        }

        @Override
        public int taskResultSizeLimit() {
            verifyStatusPermission();
            return this.taskScheduler.taskResultSizeLimit();
        }

        @Override
        public <V> HugeTask<V> waitUntilTaskCompleted(Id id, long seconds)
                                                      throws TimeoutException {
            verifyStatusPermission();
            return this.taskScheduler.waitUntilTaskCompleted(id, seconds);
        }

        @Override
        public void waitUntilAllTasksCompleted(long seconds)
                                               throws TimeoutException {
            verifyStatusPermission();
            this.taskScheduler.waitUntilAllTasksCompleted(seconds);
        }

        private void verifyTaskPermission(HugePermission perm) {
            // TODO: verify task details
            verifyPermission(perm, ResourceType.TASK);
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
        public Graph getGraph() {
            // Be called by GraphTraversalSource clone
            verifyPermission(HugePermission.EXECUTE, ResourceType.GREMLIN);
            return this.graph;
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("User '{}' login from client [{}] for path '{}'",
                      user.username(), user.client(), path);
        }
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
