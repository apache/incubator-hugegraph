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

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
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
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.type.define.SchemaStatus;
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

    public List<PropertyKey> getPropertyKeys() {
        return this.getAllSchema(HugeType.PROPERTY_KEY);
    }

    public List<VertexLabel> getVertexLabels() {
        return this.getAllSchema(HugeType.VERTEX_LABEL);
    }

    public List<EdgeLabel> getEdgeLabels() {
        return this.getAllSchema(HugeType.EDGE_LABEL);
    }

    public List<IndexLabel> getIndexLabels() {
        return this.getAllSchema(HugeType.INDEX_LABEL);
    }

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
        return this.getSchema(HugeType.PROPERTY_KEY, name);
    }

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
        return this.getSchema(HugeType.VERTEX_LABEL, name);
    }

    public Id removeVertexLabel(Id id) {
        HugeConfig config = this.graph().configuration();
        if (config.get(CoreOptions.SCHEMA_SYNC_DELETION)) {
            removeVertexLabelSync(this.graph(), id);
            return null;
        } else {
            SchemaCallable callable = new VertexLabelRemoveCallable();
            return asyncRun(this.graph(), HugeType.VERTEX_LABEL, id, callable);
        }
    }

    public static void removeVertexLabelSync(HugeGraph graph, Id id) {
        LOG.debug("SchemaTransaction remove vertex label '{}'", id);
        GraphTransaction graphTx = graph.graphTransaction();
        SchemaTransaction schemaTx = graph.schemaTransaction();
        VertexLabel vertexLabel = schemaTx.getVertexLabel(id);
        // If the vertex label does not exist, return directly
        if (vertexLabel == null) {
            return;
        }

        List<EdgeLabel> edgeLabels = schemaTx.getEdgeLabels();
        for (EdgeLabel edgeLabel : edgeLabels) {
            if (edgeLabel.linkWithLabel(id)) {
                throw new HugeException(
                          "Not allowed to remove vertex label '%s' " +
                          "because the edge label '%s' still link with it",
                          vertexLabel.name(), edgeLabel.name());
            }
        }

        /*
         * Copy index label ids because removeIndexLabel will mutate
         * vertexLabel.indexLabels()
         */
        Set<Id> indexLabelIds = ImmutableSet.copyOf(vertexLabel.indexLabels());
        LockUtil.Locks locks = new LockUtil.Locks();
        try {
            locks.lockWrites(LockUtil.VERTEX_LABEL_DELETE, id);
            schemaTx.updateSchemaStatus(vertexLabel, SchemaStatus.DELETING);
            for (Id indexLabelId : indexLabelIds) {
                removeIndexLabelSync(graph, indexLabelId);
            }

            // TODO: use event to replace direct call
            // Deleting a vertex will automatically deletes the held edge
            graphTx.removeVertices(vertexLabel);
            schemaTx.removeSchema(vertexLabel);
            // Should commit changes to backend store before release delete lock
            graph.tx().commit();
        } finally {
            locks.unlock();
        }
    }

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
        return this.getSchema(HugeType.EDGE_LABEL, name);
    }

    public Id removeEdgeLabel(Id id) {
        HugeConfig config = this.graph().configuration();
        if (config.get(CoreOptions.SCHEMA_SYNC_DELETION)) {
            removeEdgeLabelSync(this.graph(), id);
            return null;
        } else {
            SchemaCallable callable = new EdgeLabelRemoveCallable();
            return asyncRun(this.graph(), HugeType.EDGE_LABEL, id, callable);
        }
    }

    public static void removeEdgeLabelSync(HugeGraph graph, Id id) {
        LOG.debug("SchemaTransaction remove edge label '{}'", id);
        GraphTransaction graphTx = graph.graphTransaction();
        SchemaTransaction schemaTx = graph.schemaTransaction();
        EdgeLabel edgeLabel = schemaTx.getEdgeLabel(id);
        // If the edge label does not exist, return directly
        if (edgeLabel == null) {
            return;
        }
        // TODO: use event to replace direct call
        // Remove index related data(include schema) of this edge label
        Set<Id> indexIds = ImmutableSet.copyOf(edgeLabel.indexLabels());
        LockUtil.Locks locks = new LockUtil.Locks();
        try {
            locks.lockWrites(LockUtil.EDGE_LABEL_DELETE, id);
            schemaTx.updateSchemaStatus(edgeLabel, SchemaStatus.DELETING);
            for (Id indexId : indexIds) {
                removeIndexLabelSync(graph, indexId);
            }
            // Remove all edges which has matched label
            graphTx.removeEdges(edgeLabel);
            schemaTx.removeSchema(edgeLabel);
            // Should commit changes to backend store before release delete lock
            graph.tx().commit();
        } finally {
            locks.unlock();
        }
    }

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
        return this.getSchema(HugeType.INDEX_LABEL, name);
    }

    public Id removeIndexLabel(Id id) {
        HugeConfig config = this.graph().configuration();
        if (config.get(CoreOptions.SCHEMA_SYNC_DELETION)) {
            removeIndexLabelSync(this.graph(), id);
            return null;
        } else {
            SchemaCallable callable = new IndexLabelRemoveCallable();
            return asyncRun(this.graph(), HugeType.INDEX_LABEL, id, callable);
        }
    }

    public static void removeIndexLabelSync(HugeGraph graph, Id id) {
        LOG.debug("SchemaTransaction remove index label '{}'", id);
        GraphTransaction graphTx = graph.graphTransaction();
        SchemaTransaction schemaTx = graph.schemaTransaction();
        IndexLabel indexLabel = schemaTx.getIndexLabel(id);
        // If the index label does not exist, return directly
        if (indexLabel == null) {
            return;
        }
        LockUtil.Locks locks = new LockUtil.Locks();
        try {
            locks.lockWrites(LockUtil.INDEX_LABEL_DELETE, id);
            // TODO add update lock
            // Set index label to "deleting" status
            schemaTx.updateSchemaStatus(indexLabel, SchemaStatus.DELETING);
            // Remove index data
            // TODO: use event to replace direct call
            graphTx.removeIndex(indexLabel);
            // Remove label from indexLabels of vertex or edge label
            schemaTx.removeIndexLabelFromBaseLabel(indexLabel);
            schemaTx.removeSchema(indexLabel);
            // Should commit changes to backend store before release delete lock
            graph.tx().commit();
        } finally {
            locks.unlock();
        }
    }

    public Id rebuildIndex(SchemaElement schema) {
        LOG.debug("SchemaTransaction rebuild index for {} with id '{}'",
                  schema.type(), schema.id());
        HugeGraph graph = this.graph();
        if (graph.configuration().get(CoreOptions.SCHEMA_SYNC_DELETION)) {
            graph.graphTransaction().rebuildIndex(schema);
            return null;
        } else {
            SchemaCallable callable = new RebuildIndexCallable();
            return asyncRun(this.graph(), schema.type(), schema.id(), callable);
        }
    }

    public void updateSchemaStatus(SchemaElement schema, SchemaStatus status) {
        schema.status(status);
        this.updateSchema(schema);
    }

    protected void updateSchema(SchemaElement schema) {
        this.addSchema(schema);
    }

    protected void addSchema(SchemaElement schema) {
        LOG.debug("SchemaTransaction add {} with id '{}'",
                  schema.type(), schema.id());
        LockUtil.Locks locks = new LockUtil.Locks();
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
        LOG.debug("SchemaTransaction get {} by id '{}'", type, id);
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
        LOG.debug("SchemaTransaction get {} by name '{}'", type, name);
        this.beforeRead();
        ConditionQuery query = new ConditionQuery(type);
        query.eq(HugeKeys.NAME, name);
        Iterator<BackendEntry> itor = this.indexTx.query(query);
        this.afterRead();
        if (itor.hasNext()) {
            T schema = this.deserialize(itor.next(), type);
            E.checkState(!itor.hasNext(),
                         "Should not exist schema with same name '%s'", name);
            return schema;
        }
        return null;
    }

    protected <T extends SchemaElement> List<T> getAllSchema(HugeType type) {
        Query query = new Query(type);
        Iterator<BackendEntry> entries = this.query(query);

        List<T> result = new ArrayList<>();
        while (entries.hasNext()) {
            result.add(this.deserialize(entries.next(), type));
        }
        return result;
    }

    protected void removeSchema(SchemaElement schema) {
        LOG.debug("SchemaTransaction remove {} by id '{}'",
                  schema.type(), schema.id());
        LockUtil.Locks locks = new LockUtil.Locks();
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

    protected void removeIndexLabelFromBaseLabel(IndexLabel label) {
        HugeType baseType = label.baseType();
        Id baseValue = label.baseValue();
        SchemaLabel schemaLabel;
        if (baseType == HugeType.VERTEX_LABEL) {
            schemaLabel = this.getVertexLabel(baseValue);
        } else {
            assert baseType == HugeType.EDGE_LABEL;
            schemaLabel = this.getEdgeLabel(baseValue);
        }
        assert schemaLabel != null;
        schemaLabel.removeIndexLabel(label.id());
        this.updateSchema(schemaLabel);
    }

    public Id getNextId(HugeType type) {
        LOG.debug("SchemaTransaction get next id for {}", type);
        return this.store().nextId(type);
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

    private static Id asyncRun(HugeGraph graph, HugeType schemaType,
                               Id schemaId, SchemaCallable callable) {
        String name = SchemaCallable.formatTaskName(schemaType, schemaId);
        JobBuilder builder = JobBuilder.of(graph).name(name).job(callable);
        return builder.schedule();
    }
}
