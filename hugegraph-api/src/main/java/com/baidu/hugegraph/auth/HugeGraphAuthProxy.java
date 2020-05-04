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
import com.baidu.hugegraph.auth.RolePermission;
import com.baidu.hugegraph.auth.ResourceObject.ResourceType;
import com.baidu.hugegraph.auth.SchemaDefine.UserElement;
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
    private final UserManager userManager;

    public HugeGraphAuthProxy(HugeGraph hugegraph) {
        LOG.info("Wrap graph '{}' with HugeGraphAuthProxy", hugegraph.name());
        this.hugegraph = hugegraph;
        this.taskScheduler = new TaskSchedulerProxy(hugegraph.taskScheduler());
        this.userManager = new UserManagerProxy(hugegraph.userManager());
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
        //verifyPermission(HugePermission.READ, ResourceType.STATUS);
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
        this.verifySchemaPermission(HugePermission.EXECUTE,
                                    ResourceType.META, meta);
        return this.hugegraph.metadata(type, meta, args);
    }

    @Override
    public TaskScheduler taskScheduler() {
        // Just return proxy
        return this.taskScheduler;
    }

    @Override
    public UserManager userManager() {
        // Just return proxy
        return this.userManager;
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
        HugeUser admin = this.hugegraph.userManager()
                             .findUser(HugeAuthenticator.USER_ADMIN);
        E.checkState(admin != null, "Does not exist admin user");
        try {
            this.hugegraph.truncateBackend();
        } finally {
            // Restore admin user to continue to do any operation
            this.hugegraph.userManager().createUser(admin);
        }
    }

    private void verifyAdminPermission() {
        verifyPermission(HugePermission.ALL, ResourceType.ROOT);
    }

    private void verifyStatusPermission() {
        verifyPermission(HugePermission.READ, ResourceType.STATUS);
    }

    private void verifyPermission(HugePermission actionPerm,
                                  ResourceType resType) {
        /*
         * The owner role should match the graph name
         * NOTE: the graph names in gremlin-server.yaml/graphs and
         * hugegraph.properties/store must be the same if enable auth.
         */
        String graph = this.hugegraph.name();
        verifyResPermission(actionPerm, true, () -> {
            Namifiable elem = HugeResource.NameObject.NONE;
            return ResourceObject.of(graph, resType, elem);
        });
    }

    private <V extends UserElement> V verifyUserPermission(
                                      HugePermission actionPerm,
                                      V elementFetcher) {
        return verifyUserPermission(actionPerm, true, () -> elementFetcher);
    }

    private <V extends UserElement> List<V> verifyUserPermission(
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

    private <V extends UserElement> V verifyUserPermission(
                                      HugePermission actionPerm,
                                      boolean throwIfNoPermission,
                                      Supplier<V> elementFetcher) {
        String graph = this.hugegraph.name();
        return verifyResPermission(actionPerm, true, () -> {
            V elem = elementFetcher.get();
            @SuppressWarnings("unchecked")
            ResourceObject<V> r = (ResourceObject<V>) ResourceObject.of(graph,
                                                                        elem);
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
                                  boolean throwIfNoPermission,
                                  Supplier<V> elementFetcher) {
        return verifyResPermission(actionPerm, throwIfNoPermission, () -> {
            HugeElement elem = (HugeElement) elementFetcher.get();
            String graph = this.hugegraph.name();
            @SuppressWarnings("unchecked")
            ResourceObject<V> r = (ResourceObject<V>) ResourceObject.of(graph,
                                                                        elem);
            return r;
        });
    }

    private void verifySchemaExistsPermission(ResourceType resType,
                                              String schema) {
        verifySchemaPermission(HugePermission.READ, resType, schema);
    }

    private void verifySchemaPermission(HugePermission actionPerm,
                                        ResourceType resType, String name) {
        verifyResPermission(actionPerm, true, () -> {
            Namifiable elem = HugeResource.NameObject.of(name);
            String graph = this.hugegraph.name();
            return ResourceObject.of(graph, resType, elem);
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
                                        boolean throwIfNoPermission,
                                        Supplier<V> schemaFetcher) {
        return verifyResPermission(actionPerm, throwIfNoPermission, () -> {
            SchemaElement elem = schemaFetcher.get();
            String graph = this.hugegraph.name();
            @SuppressWarnings("unchecked")
            ResourceObject<V> r = (ResourceObject<V>) ResourceObject.of(graph,
                                                                        elem);
            return r;
        });
    }

    private <V> V verifyResPermission(HugePermission actionPerm,
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

        V result = ro.operated();
        // verify role permission
        if (!RolePerm.match(role, actionPerm, ro)) {
            result = null;
        }
        // verify grant permission <= user role
        else if (ro.type() == ResourceType.GRANT) {
            UserElement element = (UserElement) ro.operated();
            RolePermission grant = this.hugegraph.userManager()
                                                 .rolePermission(element);
            if (!RolePerm.match(role, grant, ro)) {
                result = null;
            }
        }

        if (result == null && throwIfNoPermission) {
            String error = String.format("Permission denied: %s %s",
                                         action, ro);
            throw new ForbiddenException(error);
        }
        return result;
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

    class UserManagerProxy implements UserManager {

        private final UserManager userManager;

        public UserManagerProxy(UserManager origin) {
            this.userManager = origin;
        }

        private UserElement updateCreator(UserElement elem) {
            String username = currentUsername();
            if (username != null && elem.creator() == null) {
                elem.creator(username);
            }
            return elem;
        }

        private String currentUsername() {
            Context context = getContext();
            if (context != null) {
                return context.user().username();
            }
            return null;
        }

        @Override
        public boolean close() {
            verifyAdminPermission();
            return this.userManager.close();
        }

        @Override
        public Id createUser(HugeUser user) {
            E.checkArgument(!user.name().equals(HugeAuthenticator.USER_ADMIN),
                            "Invalid user name '%s'", user.name());
            this.updateCreator(user);
            verifyPermission(HugePermission.WRITE, ResourceType.USER_GROUP);
            return this.userManager.createUser(user);
        }

        @Override
        public Id updateUser(HugeUser updatedUser) {
            String username = currentUsername();
            HugeUser user = this.userManager.getUser(updatedUser.id());
            if (!user.name().equals(username)) {
                this.updateCreator(updatedUser);
                verifyPermission(HugePermission.WRITE, ResourceType.USER_GROUP);
            }
            return this.userManager.updateUser(updatedUser);
        }

        @Override
        public HugeUser deleteUser(Id id) {
            verifyPermission(HugePermission.DELETE, ResourceType.USER_GROUP);
            return this.userManager.deleteUser(id);
        }

        @Override
        public HugeUser findUser(String name) {
            HugeUser user = this.userManager.findUser(name);
            String username = currentUsername();
            if (!user.name().equals(username)) {
                verifyPermission(HugePermission.READ, ResourceType.USER_GROUP);
            }
            return user;
        }

        @Override
        public HugeUser getUser(Id id) {
            HugeUser user = this.userManager.getUser(id);
            String username = currentUsername();
            if (!user.name().equals(username)) {
                verifyPermission(HugePermission.READ, ResourceType.USER_GROUP);
            }
            return user;
        }

        @Override
        public List<HugeUser> listUsers(List<Id> ids) {
            verifyPermission(HugePermission.READ, ResourceType.USER_GROUP);
            return this.userManager.listUsers(ids);
        }

        @Override
        public List<HugeUser> listAllUsers(long limit) {
            verifyPermission(HugePermission.READ, ResourceType.USER_GROUP);
            return this.userManager.listAllUsers(limit);
        }

        @Override
        public Id createGroup(HugeGroup group) {
            this.updateCreator(group);
            verifyPermission(HugePermission.WRITE, ResourceType.USER_GROUP);
            return this.userManager.createGroup(group);
        }

        @Override
        public Id updateGroup(HugeGroup group) {
            this.updateCreator(group);
            verifyPermission(HugePermission.WRITE, ResourceType.USER_GROUP);
            return this.userManager.updateGroup(group);
        }

        @Override
        public HugeGroup deleteGroup(Id id) {
            verifyPermission(HugePermission.DELETE, ResourceType.USER_GROUP);
            return this.userManager.deleteGroup(id);
        }

        @Override
        public HugeGroup getGroup(Id id) {
            verifyPermission(HugePermission.READ, ResourceType.USER_GROUP);
            return this.userManager.getGroup(id);
        }

        @Override
        public List<HugeGroup> listGroups(List<Id> ids) {
            verifyPermission(HugePermission.READ, ResourceType.USER_GROUP);
            return this.userManager.listGroups(ids);
        }

        @Override
        public List<HugeGroup> listAllGroups(long limit) {
            verifyPermission(HugePermission.READ, ResourceType.USER_GROUP);
            return this.userManager.listAllGroups(limit);
        }

        @Override
        public Id createTarget(HugeTarget target) {
            this.updateCreator(target);
            verifyPermission(HugePermission.WRITE, ResourceType.TARGET);
            return this.userManager.createTarget(target);
        }

        @Override
        public Id updateTarget(HugeTarget target) {
            this.updateCreator(target);
            verifyPermission(HugePermission.WRITE, ResourceType.TARGET);
            return this.userManager.updateTarget(target);
        }

        @Override
        public HugeTarget deleteTarget(Id id) {
            verifyPermission(HugePermission.DELETE, ResourceType.TARGET);
            return this.userManager.deleteTarget(id);
        }

        @Override
        public HugeTarget getTarget(Id id) {
            verifyPermission(HugePermission.READ, ResourceType.TARGET);
            return this.userManager.getTarget(id);
        }

        @Override
        public List<HugeTarget> listTargets(List<Id> ids) {
            verifyPermission(HugePermission.READ, ResourceType.TARGET);
            return this.userManager.listTargets(ids);
        }

        @Override
        public List<HugeTarget> listAllTargets(long limit) {
            verifyPermission(HugePermission.READ, ResourceType.TARGET);
            return this.userManager.listAllTargets(limit);
        }

        @Override
        public Id createBelong(HugeBelong belong) {
            this.updateCreator(belong);
            verifyUserPermission(HugePermission.WRITE, belong);
            return this.userManager.createBelong(belong);
        }

        @Override
        public Id updateBelong(HugeBelong belong) {
            this.updateCreator(belong);
            verifyUserPermission(HugePermission.WRITE, belong);
            return this.userManager.updateBelong(belong);
        }

        @Override
        public HugeBelong deleteBelong(Id id) {
            verifyUserPermission(HugePermission.DELETE,
                                  this.userManager.getBelong(id));
            return this.userManager.deleteBelong(id);
        }

        @Override
        public HugeBelong getBelong(Id id) {
            return verifyUserPermission(HugePermission.READ,
                                         this.userManager.getBelong(id));
        }

        @Override
        public List<HugeBelong> listBelong(List<Id> ids) {
            return verifyUserPermission(HugePermission.READ,
                                         this.userManager.listBelong(ids));
        }

        @Override
        public List<HugeBelong> listAllBelong(long limit) {
            return verifyUserPermission(HugePermission.READ,
                                         this.userManager.listAllBelong(limit));
        }

        @Override
        public List<HugeBelong> listBelongByUser(Id user, long limit) {
            List<HugeBelong> r = this.userManager.listBelongByUser(user, limit);
            return verifyUserPermission(HugePermission.READ, r);
        }

        @Override
        public List<HugeBelong> listBelongByGroup(Id group, long limit) {
            List<HugeBelong> r = this.userManager.listBelongByGroup(group,
                                                                    limit);
            return verifyUserPermission(HugePermission.READ, r);
        }

        @Override
        public Id createAccess(HugeAccess access) {
            verifyUserPermission(HugePermission.WRITE, access);
            this.updateCreator(access);
            return this.userManager.createAccess(access);
        }

        @Override
        public Id updateAccess(HugeAccess access) {
            verifyUserPermission(HugePermission.WRITE, access);
            this.updateCreator(access);
            return this.userManager.updateAccess(access);
        }

        @Override
        public HugeAccess deleteAccess(Id id) {
            verifyUserPermission(HugePermission.DELETE,
                                  this.userManager.getAccess(id));
            return this.userManager.deleteAccess(id);
        }

        @Override
        public HugeAccess getAccess(Id id) {
            return verifyUserPermission(HugePermission.READ,
                                         this.userManager.getAccess(id));
        }

        @Override
        public List<HugeAccess> listAccess(List<Id> ids) {
            return verifyUserPermission(HugePermission.READ,
                                         this.userManager.listAccess(ids));
        }

        @Override
        public List<HugeAccess> listAllAccess(long limit) {
            return verifyUserPermission(HugePermission.READ,
                                         this.userManager.listAllAccess(limit));
        }

        @Override
        public List<HugeAccess> listAccessByGroup(Id group, long limit) {
            List<HugeAccess> r = this.userManager.listAccessByGroup(group,
                                                                    limit);
            return verifyUserPermission(HugePermission.READ, r);
        }

        @Override
        public List<HugeAccess> listAccessByTarget(Id target, long limit) {
            List<HugeAccess> r = this.userManager.listAccessByTarget(target,
                                                                     limit);
            return verifyUserPermission(HugePermission.READ, r);
        }

        @Override
        public HugeUser matchUser(String name, String password) {
            // Unneeded to verify permission
            return this.userManager.matchUser(name, password);
        }

        @Override
        public RolePermission rolePermission(UserElement element) {
            String username = currentUsername();
            if (!(element instanceof HugeUser) ||
                !((HugeUser) element).name().equals(username)) {
                verifyUserPermission(HugePermission.READ, element);
            }
            return this.userManager.rolePermission(element);
        }

        @Override
        public RolePermission loginUser(String username, String password) {
            // Can't verifyPermission() here, login first with temp permission
            Context context = setContext(Context.admin());
            try {
                return this.userManager.loginUser(username, password);
            } catch (Exception e) {
                LOG.error("Failed to login user {}: ", username, e);
                throw e;
            } finally {
                setContext(context);
            }
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
