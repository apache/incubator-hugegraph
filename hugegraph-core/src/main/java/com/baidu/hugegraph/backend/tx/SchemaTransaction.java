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

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.exception.NotAllowException;
import com.baidu.hugegraph.job.JobBuilder;
import com.baidu.hugegraph.job.schema.EdgeLabelRemoveCallable;
import com.baidu.hugegraph.job.schema.IndexLabelRemoveCallable;
import com.baidu.hugegraph.job.schema.OlapPropertyKeyClearCallable;
import com.baidu.hugegraph.job.schema.OlapPropertyKeyCreateCallable;
import com.baidu.hugegraph.job.schema.OlapPropertyKeyRemoveCallable;
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
import com.baidu.hugegraph.type.define.WriteType;
import com.baidu.hugegraph.type.define.SchemaStatus;
import com.baidu.hugegraph.util.DateUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.LockUtil;
import com.google.common.collect.ImmutableSet;

public class SchemaTransaction extends IndexableTransaction {

    private SchemaIndexTransaction indexTx;

    public SchemaTransaction(HugeGraphParams graph, BackendStore store) {
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
        if (this.hasUpdate()) {
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
    public Id addPropertyKey(PropertyKey propertyKey) {
        this.addSchema(propertyKey);
        if (propertyKey.olap()) {
            return this.createOlapPk(propertyKey);
        }
        return IdGenerator.ZERO;
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
    public Id removePropertyKey(Id id) {
        LOG.debug("SchemaTransaction remove property key '{}'", id);
        PropertyKey propertyKey = this.getPropertyKey(id);
        // If the property key does not exist, return directly
        if (propertyKey == null) {
            return null;
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

        if (propertyKey.oltp()) {
            this.removeSchema(propertyKey);
            return IdGenerator.ZERO;
        } else {
            return this.removeOlapPk(propertyKey);
        }
    }

    @Watched(prefix = "schema")
    public void addVertexLabel(VertexLabel vertexLabel) {
        this.addSchema(vertexLabel);
    }

    @Watched(prefix = "schema")
    public VertexLabel getVertexLabel(Id id) {
        E.checkArgumentNotNull(id, "Vertex label id can't be null");
        if (SchemaElement.OLAP_ID.equals(id)) {
            return VertexLabel.OLAP_VL;
        }
        return this.getSchema(HugeType.VERTEX_LABEL, id);
    }

    @Watched(prefix = "schema")
    public VertexLabel getVertexLabel(String name) {
        E.checkArgumentNotNull(name, "Vertex label name can't be null");
        E.checkArgument(!name.isEmpty(), "Vertex label name can't be empty");
        if (SchemaElement.OLAP.equals(name)) {
            return VertexLabel.OLAP_VL;
        }
        return this.getSchema(HugeType.VERTEX_LABEL, name);
    }

    @Watched(prefix = "schema")
    public Id removeVertexLabel(Id id) {
        LOG.debug("SchemaTransaction remove vertex label '{}'", id);
        SchemaCallable callable = new VertexLabelRemoveCallable();
        VertexLabel schema = this.getVertexLabel(id);
        return asyncRun(this.graph(), schema, callable);
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
        EdgeLabel schema = this.getEdgeLabel(id);
        return asyncRun(this.graph(), schema, callable);
    }

    @Watched(prefix = "schema")
    public void addIndexLabel(SchemaLabel schemaLabel, IndexLabel indexLabel) {
        this.addSchema(indexLabel);

        /*
         * Update index name in base-label(VL/EL)
         * TODO: should wrap update base-label and create index in one tx.
         */
        if (schemaLabel instanceof VertexLabel &&
            schemaLabel.equals(VertexLabel.OLAP_VL)) {
            return;
        }
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
        IndexLabel schema = this.getIndexLabel(id);
        return asyncRun(this.graph(), schema, callable);
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
        return asyncRun(this.graph(), schema, callable, dependencies);
    }

    public void createIndexLabelForOlapPk(PropertyKey propertyKey) {
        WriteType writeType = propertyKey.writeType();
        if (writeType == WriteType.OLTP ||
            writeType == WriteType.OLAP_COMMON) {
            return;
        }

        String indexName = SchemaElement.OLAP + "_by_" + propertyKey.name();
        IndexLabel.Builder builder = this.graph().schema()
                                         .indexLabel(indexName)
                                         .onV(SchemaElement.OLAP)
                                         .by(propertyKey.name());
        if (propertyKey.writeType() == WriteType.OLAP_SECONDARY) {
            builder.secondary();
        } else {
            assert propertyKey.writeType() == WriteType.OLAP_RANGE;
            builder.range();
        }
        builder.build();
        this.graph().addIndexLabel(VertexLabel.OLAP_VL, builder.build());
    }

    public Id createOlapPk(PropertyKey propertyKey) {
        LOG.debug("SchemaTransaction create olap property key {} with id '{}'",
                  propertyKey.name(), propertyKey.id());
        SchemaCallable callable = new OlapPropertyKeyCreateCallable();
        return asyncRun(this.graph(), propertyKey, callable);
    }

    public Id clearOlapPk(PropertyKey propertyKey) {
        LOG.debug("SchemaTransaction clear olap property key {} with id '{}'",
                  propertyKey.name(), propertyKey.id());
        SchemaCallable callable = new OlapPropertyKeyClearCallable();
        return asyncRun(this.graph(), propertyKey, callable);
    }

    public Id removeOlapPk(PropertyKey propertyKey) {
        LOG.debug("SchemaTransaction remove olap property key {} with id '{}'",
                  propertyKey.name(), propertyKey.id());
        SchemaCallable callable = new OlapPropertyKeyRemoveCallable();
        return asyncRun(this.graph(), propertyKey, callable);
    }

    public void createOlapPk(Id id) {
        this.store().provider().createOlapTable(this.graph(), id);
    }

    public void initAndRegisterOlapTables() {
        for (PropertyKey pk : this.getPropertyKeys()) {
            if (pk.olap()) {
                this.store().provider().initAndRegisterOlapTable(this.graph(),
                                                                 pk.id());
            }
        }
    }

    public void clearOlapPk(Id id) {
        this.store().provider().clearOlapTable(this.graph(), id);
    }

    public void removeOlapPk(Id id) {
        this.store().provider().removeOlapTable(this.graph(), id);
    }

    @Watched(prefix = "schema")
    public void updateSchemaStatus(SchemaElement schema, SchemaStatus status) {
        schema.status(status);
        this.updateSchema(schema);
    }

    @Watched(prefix = "schema")
    public boolean existsSchemaId(HugeType type, Id id) {
        return this.getSchema(type, id) != null;
    }

    protected void updateSchema(SchemaElement schema) {
        this.addSchema(schema);
    }

    protected void addSchema(SchemaElement schema) {
        LOG.debug("SchemaTransaction add {} with id '{}'",
                  schema.type(), schema.id());
        setCreateTimeIfNeeded(schema);

        LockUtil.Locks locks = new LockUtil.Locks(this.params().name());
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
        LockUtil.Locks locks = new LockUtil.Locks(this.graphName());
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

    public void checkSchemaName(String name) {
        String illegalReg = this.params().configuration()
                                .get(CoreOptions.SCHEMA_ILLEGAL_NAME_REGEX);

        E.checkNotNull(name, "name");
        E.checkArgument(!name.isEmpty(), "The name can't be empty.");
        E.checkArgument(name.length() < 256,
                        "The length of name must less than 256 bytes.");
        E.checkArgument(!name.matches(illegalReg),
                        "Illegal schema name '%s'", name);

        final char[] filters = {'#', '>', ':', '!'};
        for (char c : filters) {
            E.checkArgument(name.indexOf(c) == -1,
                            "The name can't contain character '%s'.", c);
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
        E.checkState(id.number() && id.asLong() > 0L,
                     "Schema id must be number and >0, but got '%s'", id);
        GraphMode mode = this.graphMode();
        E.checkState(mode == GraphMode.RESTORING,
                     "Can't build schema with provided id '%s' " +
                     "when graph '%s' in mode '%s'",
                     id, this.graphName(), mode);
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

    private static void setCreateTimeIfNeeded(SchemaElement schema) {
        if (!schema.userdata().containsKey(Userdata.CREATE_TIME)) {
            schema.userdata(Userdata.CREATE_TIME, DateUtil.now());
        }
    }

    private static Id asyncRun(HugeGraph graph, SchemaElement schema,
                               SchemaCallable callable) {
        return asyncRun(graph, schema, callable, ImmutableSet.of());
    }

    @Watched(prefix = "schema")
    private static Id asyncRun(HugeGraph graph, SchemaElement schema,
                               SchemaCallable callable, Set<Id> dependencies) {
        E.checkArgument(schema != null, "Schema can't be null");
        String name = SchemaCallable.formatTaskName(schema.type(),
                                                    schema.id(),
                                                    schema.name());

        JobBuilder<Object> builder = JobBuilder.of(graph).name(name)
                                               .job(callable)
                                               .dependencies(dependencies);
        HugeTask<?> task = builder.schedule();

        // If TASK_SYNC_DELETION is true, wait async thread done before
        // continue. This is used when running tests.
        if (graph.option(CoreOptions.TASK_SYNC_DELETION)) {
            task.syncWait();
        }
        return task.id();
    }
}
