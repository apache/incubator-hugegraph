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

package com.baidu.hugegraph.backend.tx;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.exception.ExistedException;
import com.baidu.hugegraph.exception.NotAllowException;
import com.baidu.hugegraph.job.JobBuilder;
import com.baidu.hugegraph.job.schema.EdgeLabelRemoveCallable;
import com.baidu.hugegraph.job.schema.IndexLabelRemoveCallable;
import com.baidu.hugegraph.job.schema.RebuildIndexCallable;
import com.baidu.hugegraph.job.schema.SchemaCallable;
import com.baidu.hugegraph.job.schema.VertexLabelRemoveCallable;
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.schema.SchemaLabel;
import com.baidu.hugegraph.schema.Userdata;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.task.HugeTask;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.GraphMode;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.type.define.SchemaStatus;
import com.baidu.hugegraph.util.DateUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.LockUtil;
import com.google.common.collect.ImmutableSet;

public class SchemaTransaction extends IndexableTransaction {

    private SchemaIndexTransaction indexTx;

    public SchemaTransaction(HugeGraph graph, BackendStore store) {
        super(graph, store);
        this.autoCommit(true);

        this.indexTx = new SchemaIndexTransaction(graph, store);
    }

    @Override
    protected AbstractTransaction indexTransaction() {
        return this.indexTx;
    }

    @Override
    protected void beforeRead() {
        /*
         * NOTE: each schema operation will be auto committed,
         * we expect the tx is clean when query.
         */
        if (this.hasUpdates()) {
            throw new BackendException("There are still dirty changes");
        }
    }

    @Watched(prefix = "schema")
    public List<PropertyKey> getPropertyKeys() {
        return this.getAllSchema(HugeType.PROPERTY_KEY);
    }

    @Watched(prefix = "schema")
    public List<VertexLabel> getVertexLabels() {
        return this.getAllSchema(HugeType.VERTEX_LABEL);
    }

    @Watched(prefix = "schema")
    public List<EdgeLabel> getEdgeLabels() {
        return this.getAllSchema(HugeType.EDGE_LABEL);
    }

    @Watched(prefix = "schema")
    public List<IndexLabel> getIndexLabels() {
        return this.getAllSchema(HugeType.INDEX_LABEL);
    }

    @Watched(prefix = "schema")
    public void addPropertyKey(PropertyKey propertyKey) {
        this.addSchema(propertyKey);
    }

    @Watched(prefix = "schema")
    public PropertyKey getPropertyKey(Id id) {
        E.checkArgumentNotNull(id, "Property key id can't be null");
        return this.getSchema(HugeType.PROPERTY_KEY, id);
    }

    @Watched(prefix = "schema")
    public PropertyKey getPropertyKey(String name) {
        E.checkArgumentNotNull(name, "Property key name can't be null");
        E.checkArgument(!name.isEmpty(), "Property key name can't be empty");
        return this.getSchema(HugeType.PROPERTY_KEY, name);
    }

    @Watched(prefix = "schema")
    public void removePropertyKey(Id id) {
        LOG.debug("SchemaTransaction remove property key '{}'", id);
        PropertyKey propertyKey = this.getPropertyKey(id);
        // If the property key does not exist, return directly
        if (propertyKey == null) {
            return;
        }

        List<VertexLabel> vertexLabels = this.getVertexLabels();
        for (VertexLabel vertexLabel : vertexLabels) {
            if (vertexLabel.properties().contains(id)) {
                throw new NotAllowException(
                          "Not allowed to remove property key: '%s' " +
                          "because the vertex label '%s' is still using it.",
                          propertyKey, vertexLabel.name());
            }
        }

        List<EdgeLabel> edgeLabels = this.getEdgeLabels();
        for (EdgeLabel edgeLabel : edgeLabels) {
            if (edgeLabel.properties().contains(id)) {
                throw new NotAllowException(
                          "Not allowed to remove property key: '%s' " +
                          "because the edge label '%s' is still using it.",
                          propertyKey, edgeLabel.name());
            }
        }

        this.removeSchema(propertyKey);
    }

    @Watched(prefix = "schema")
    public void addVertexLabel(VertexLabel vertexLabel) {
        this.addSchema(vertexLabel);
    }

    @Watched(prefix = "schema")
    public VertexLabel getVertexLabel(Id id) {
        E.checkArgumentNotNull(id, "Vertex label id can't be null");
        return this.getSchema(HugeType.VERTEX_LABEL, id);
    }

    @Watched(prefix = "schema")
    public VertexLabel getVertexLabel(String name) {
        E.checkArgumentNotNull(name, "Vertex label name can't be null");
        E.checkArgument(!name.isEmpty(), "Vertex label name can't be empty");
        return this.getSchema(HugeType.VERTEX_LABEL, name);
    }

    @Watched(prefix = "schema")
    public Id removeVertexLabel(Id id) {
        LOG.debug("SchemaTransaction remove vertex label '{}'", id);
        SchemaCallable callable = new VertexLabelRemoveCallable();
        return asyncRun(this.graph(), HugeType.VERTEX_LABEL, id, callable);
    }

    @Watched(prefix = "schema")
    public void addEdgeLabel(EdgeLabel edgeLabel) {
        this.addSchema(edgeLabel);
    }

    @Watched(prefix = "schema")
    public EdgeLabel getEdgeLabel(Id id) {
        E.checkArgumentNotNull(id, "Edge label id can't be null");
        return this.getSchema(HugeType.EDGE_LABEL, id);
    }

    @Watched(prefix = "schema")
    public EdgeLabel getEdgeLabel(String name) {
        E.checkArgumentNotNull(name, "Edge label name can't be null");
        E.checkArgument(!name.isEmpty(), "Edge label name can't be empty");
        return this.getSchema(HugeType.EDGE_LABEL, name);
    }

    @Watched(prefix = "schema")
    public Id removeEdgeLabel(Id id) {
        LOG.debug("SchemaTransaction remove edge label '{}'", id);
        SchemaCallable callable = new EdgeLabelRemoveCallable();
        return asyncRun(this.graph(), HugeType.EDGE_LABEL, id, callable);
    }

    @Watched(prefix = "schema")
    public void addIndexLabel(SchemaLabel schemaLabel, IndexLabel indexLabel) {
        this.addSchema(indexLabel);

        /*
         * Update index name in base-label(VL/EL)
         * TODO: should wrap update base-label and create index in one tx.
         */
        schemaLabel.indexLabel(indexLabel.id());
        this.updateSchema(schemaLabel);
    }

    @Watched(prefix = "schema")
    public IndexLabel getIndexLabel(Id id) {
        E.checkArgumentNotNull(id, "Index label id can't be null");
        return this.getSchema(HugeType.INDEX_LABEL, id);
    }

    @Watched(prefix = "schema")
    public IndexLabel getIndexLabel(String name) {
        E.checkArgumentNotNull(name, "Index label name can't be null");
        E.checkArgument(!name.isEmpty(), "Index label name can't be empty");
        return this.getSchema(HugeType.INDEX_LABEL, name);
    }

    @Watched(prefix = "schema")
    public Id removeIndexLabel(Id id) {
        LOG.debug("SchemaTransaction remove index label '{}'", id);
        SchemaCallable callable = new IndexLabelRemoveCallable();
        return asyncRun(this.graph(), HugeType.INDEX_LABEL, id, callable);
    }

    @Watched(prefix = "schema")
    public Id rebuildIndex(SchemaElement schema) {
        return this.rebuildIndex(schema, ImmutableSet.of());
    }

    @Watched(prefix = "schema")
    public Id rebuildIndex(SchemaElement schema, Set<Id> dependencies) {
        LOG.debug("SchemaTransaction rebuild index for {} with id '{}'",
                  schema.type(), schema.id());
        SchemaCallable callable = new RebuildIndexCallable();
        return asyncRun(this.graph(), schema.type(), schema.id(),
                        callable, dependencies);
    }

    @Watched(prefix = "schema")
    public void updateSchemaStatus(SchemaElement schema, SchemaStatus status) {
        schema.status(status);
        this.updateSchema(schema);
    }

    public <V> V lockCheckAndCreateSchema(HugeType type, String name,
                                          Function<String, V> callback) {
        String graph = this.graph().name();
        LockUtil.Locks locks = new LockUtil.Locks(graph);
        try {
            locks.lockWrites(LockUtil.hugeType2Group(type),
                             IdGenerator.of(name));
            return callback.apply(name);
        } finally {
            locks.unlock();
        }
    }

    protected void updateSchema(SchemaElement schema) {
        this.addSchema(schema);
    }

    protected void addSchema(SchemaElement schema) {
        LOG.debug("SchemaTransaction add {} with id '{}'",
                  schema.type(), schema.id());
        setCreateTimeIfNeeded(schema);

        LockUtil.Locks locks = new LockUtil.Locks(this.graph().name());
        try {
            locks.lockWrites(LockUtil.hugeType2Group(schema.type()),
                             schema.id());
            this.beforeWrite();
            this.doInsert(this.serialize(schema));
            this.indexTx.updateNameIndex(schema, false);
            this.afterWrite();
        } finally {
            locks.unlock();
        }
    }

    protected <T extends SchemaElement> T getSchema(HugeType type, Id id) {
        LOG.debug("SchemaTransaction get {} by id '{}'",
                  type.readableName(), id);
        this.beforeRead();
        BackendEntry entry = this.query(type, id);
        if (entry == null) {
            return null;
        }
        T schema = this.deserialize(entry, type);
        this.afterRead();
        return schema;
    }

    /**
     * Currently doesn't allow to exist schema with the same name
     * @param type the query schema type
     * @param name the query schema name
     * @param <T>  SubClass of SchemaElement
     * @return     the queried schema object
     */
    protected <T extends SchemaElement> T getSchema(HugeType type,
                                                    String name) {
        LOG.debug("SchemaTransaction get {} by name '{}'",
                  type.readableName(), name);
        this.beforeRead();

        ConditionQuery query = new ConditionQuery(type);
        query.eq(HugeKeys.NAME, name);
        QueryResults<BackendEntry> results = this.indexTx.query(query);

        this.afterRead();

        // Should not exist schema with same name
        BackendEntry entry = results.one();
        if (entry == null) {
            return null;
        }
        T schema = this.deserialize(entry, type);
        return schema;
    }

    protected <T extends SchemaElement> List<T> getAllSchema(HugeType type) {
        List<T> results = new ArrayList<>();
        Query query = new Query(type);
        Iterator<BackendEntry> entries = this.query(query).iterator();
        try {
            while (entries.hasNext()) {
                results.add(this.deserialize(entries.next(), type));
                Query.checkForceCapacity(results.size());
            }
        } finally {
            CloseableIterator.closeIterator(entries);
        }
        return results;
    }

    protected void removeSchema(SchemaElement schema) {
        LOG.debug("SchemaTransaction remove {} by id '{}'",
                  schema.type(), schema.id());
        LockUtil.Locks locks = new LockUtil.Locks(this.graph().name());
        try {
            locks.lockWrites(LockUtil.hugeType2Group(schema.type()),
                             schema.id());
            this.beforeWrite();
            this.indexTx.updateNameIndex(schema, true);
            BackendEntry e = this.serializer.writeId(schema.type(), schema.id());
            this.doRemove(e);
            this.afterWrite();
        } finally {
            locks.unlock();
        }
    }

    private BackendEntry serialize(SchemaElement schema) {
        switch (schema.type()) {
            case PROPERTY_KEY:
                return this.serializer.writePropertyKey((PropertyKey) schema);
            case VERTEX_LABEL:
                return this.serializer.writeVertexLabel((VertexLabel) schema);
            case EDGE_LABEL:
                return this.serializer.writeEdgeLabel((EdgeLabel) schema);
            case INDEX_LABEL:
                return this.serializer.writeIndexLabel((IndexLabel) schema);
            default:
                throw new AssertionError(String.format(
                          "Unknown schema type '%s'", schema.type()));
        }
    }

    @SuppressWarnings({"unchecked"})
    private <T> T deserialize(BackendEntry entry, HugeType type) {
        switch (type) {
            case PROPERTY_KEY:
                return (T) this.serializer.readPropertyKey(this.graph(), entry);
            case VERTEX_LABEL:
                return (T) this.serializer.readVertexLabel(this.graph(), entry);
            case EDGE_LABEL:
                return (T) this.serializer.readEdgeLabel(this.graph(), entry);
            case INDEX_LABEL:
                return (T) this.serializer.readIndexLabel(this.graph(), entry);
            default:
                throw new AssertionError(String.format(
                          "Unknown schema type '%s'", type));
        }
    }

    @Watched(prefix = "schema")
    public Id validOrGenerateId(HugeType type, Id id, String name) {
        boolean forSystem = Graph.Hidden.isHidden(name);
        if (id != null) {
            this.checkIdAndUpdateNextId(type, id, name, forSystem);
        } else {
            if (forSystem) {
                id = this.getNextSystemId();
            } else {
                id = this.getNextId(type);
            }
        }
        return id;
    }

    private void checkIdAndUpdateNextId(HugeType type, Id id,
                                        String name, boolean forSystem) {
        if (forSystem) {
            if (id.number() && id.asLong() < 0) {
                return;
            }
            throw new IllegalStateException(String.format(
                      "Invalid system id '%s'", id));
        }
        HugeGraph graph = this.graph();
        E.checkState(id.number() && id.asLong() > 0L,
                     "Schema id must be number and >0, but got '%s'", id);
        E.checkState(graph.mode() == GraphMode.RESTORING,
                     "Can't build schema with provided id '%s' " +
                     "when graph '%s' in mode '%s'",
                     id, graph, graph.mode());
        this.setNextIdLowest(type, id.asLong());
    }

    @Watched(prefix = "schema")
    public Id getNextId(HugeType type) {
        LOG.debug("SchemaTransaction get next id for {}", type);
        return this.store().nextId(type);
    }

    @Watched(prefix = "schema")
    public void setNextIdLowest(HugeType type, long lowest) {
        LOG.debug("SchemaTransaction set next id to {} for {}", lowest, type);
        this.store().setCounterLowest(type, lowest);
    }

    @Watched(prefix = "schema")
    public Id getNextSystemId() {
        LOG.debug("SchemaTransaction get next system id");
        Id id = this.store().nextId(HugeType.SYS_SCHEMA);
        return IdGenerator.of(-id.asLong());
    }

    @Watched(prefix = "schema")
    public void checkIdIfRestoringMode(HugeType type, Id id) {
        if (this.graph().mode() == GraphMode.RESTORING) {
            E.checkArgument(id != null,
                            "Must provide schema id if in RESTORING mode");
            SchemaElement element = this.getSchema(type, id);
            if (element != null) {
                throw new ExistedException(type.readableName() + " id", id);
            }
        }
    }

    private static void setCreateTimeIfNeeded(SchemaElement schema) {
        if (!schema.userdata().containsKey(Userdata.CREATE_TIME)) {
            schema.userdata(Userdata.CREATE_TIME, DateUtil.now());
        }
    }

    private static Id asyncRun(HugeGraph graph, HugeType schemaType,
                               Id schemaId, SchemaCallable callable) {
        return asyncRun(graph, schemaType, schemaId,
                        callable, ImmutableSet.of());
    }

    @Watched(prefix = "schema")
    private static Id asyncRun(HugeGraph graph, HugeType schemaType,
                               Id schemaId, SchemaCallable callable,
                               Set<Id> dependencies) {
        String schemaName = graph.schemaTransaction()
                                 .getSchema(schemaType, schemaId).name();
        String name = SchemaCallable.formatTaskName(schemaType, schemaId,
                                                    schemaName);

        JobBuilder<Object> builder = JobBuilder.of(graph).name(name)
                                               .job(callable)
                                               .dependencies(dependencies);
        HugeTask<?> task = builder.schedule();

        // If SCHEMA_SYNC_DELETION is true, wait async thread done before
        // continue. This is used when running tests.
        if (graph.configuration().get(CoreOptions.SCHEMA_SYNC_DELETION)) {
            try {
                task.get();
                assert task.completed();
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                throw new HugeException("Async task failed with error: %s",
                                        cause, cause.getMessage());
            } catch (Exception e) {
                throw new HugeException("Async task failed with error: %s",
                                        e, e.getMessage());
            }
        }
        return task.id();
    }
}
