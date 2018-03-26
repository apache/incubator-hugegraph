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
import com.baidu.hugegraph.backend.store.BackendAction.Action;
import com.baidu.hugegraph.exception.NotAllowException;
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.schema.SchemaLabel;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
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

    public void removeVertexLabel(Id id) {
        LOG.debug("SchemaTransaction remove vertex label '{}'", id);
        VertexLabel vertexLabel = this.getVertexLabel(id);
        // If the vertex label does not exist, return directly
        if (vertexLabel == null) {
            return;
        }

        List<EdgeLabel> edgeLabels = this.getEdgeLabels();
        for (EdgeLabel edgeLabel : edgeLabels) {
            if (edgeLabel.linkWithLabel(id)) {
                throw new HugeException("Not allowed to remove vertex label " +
                                        "'%s' because the edge label '%s' " +
                                        "still link with it",
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
            locks.lockWrites(LockUtil.VERTEX_LABEL, id);
            for (Id indexLabelId : indexLabelIds) {
                this.removeIndexLabel(indexLabelId);
            }

            // TODO: use event to replace direct call
            // Deleting a vertex will automatically deletes the held edge
            this.graph().graphTransaction().removeVertices(vertexLabel);
            this.removeSchema(vertexLabel);
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

    public void removeEdgeLabel(Id id) {
        LOG.debug("SchemaTransaction remove edge label '{}'", id);
        EdgeLabel edgeLabel = this.getEdgeLabel(id);
        // If the edge label does not exist, return directly
        if (edgeLabel == null) {
            return;
        }
        // TODO: use event to replace direct call
        // Remove index related data(include schema) of this edge label
        Set<Id> indexIds = ImmutableSet.copyOf(edgeLabel.indexLabels());
        LockUtil.Locks locks = new LockUtil.Locks();
        try {
            locks.lockWrites(LockUtil.EDGE_LABEL, id);
            for (Id indexId : indexIds) {
                this.removeIndexLabel(indexId);
            }
            // Remove all edges which has matched label
            this.graph().graphTransaction().removeEdges(edgeLabel);
            this.removeSchema(edgeLabel);
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
        this.addSchema(schemaLabel);
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

    public void removeIndexLabel(Id id) {
        LOG.debug("SchemaTransaction remove index label '{}'", id);
        IndexLabel indexLabel = this.getIndexLabel(id);
        // If the index label does not exist, return directly
        if (indexLabel == null) {
            return;
        }

        LockUtil.Locks locks = new LockUtil.Locks();
        try {
            locks.lockWrites(LockUtil.INDEX_LABEL, id);
            // Remove index data
            // TODO: use event to replace direct call
            this.graph().graphTransaction().removeIndex(indexLabel);
            // Remove label from indexLabels of vertex or edge label
            this.removeIndexLabelFromBaseLabel(indexLabel);
            this.removeSchema(indexLabel);
        } finally {
            locks.unlock();
        }
    }

    public void rebuildIndex(SchemaElement schema) {
        LOG.debug("SchemaTransaction rebuild index for '{}' '{}'",
                  schema.type(), schema.id());
        this.graph().graphTransaction().rebuildIndex(schema);
    }

    protected void addSchema(SchemaElement schema) {
        LOG.debug("SchemaTransaction add {}: {}", schema.type(), schema.id());
        this.beforeWrite();
        this.doInsert(this.serialize(schema));
        this.indexTx.updateNameIndex(schema, false);
        this.afterWrite();
    }

    protected <T extends SchemaElement> T getSchema(HugeType type, Id id) {
        LOG.debug("SchemaTransaction get {} with id {}", type, id);
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
     */
    protected <T extends SchemaElement> T getSchema(HugeType type,
                                                    String name) {
        LOG.debug("SchemaTransaction get {} with name {}", type, name);
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
        LOG.debug("SchemaTransaction remove {} with id {}",
                  schema.type(), schema.id());
        this.beforeWrite();
        this.indexTx.updateNameIndex(schema, true);
        BackendEntry e = this.serializer.writeId(schema.type(), schema.id());
        this.doAction(Action.DELETE, e);
        this.afterWrite();
    }

    protected void removeIndexLabelFromBaseLabel(IndexLabel label) {
        HugeType baseType = label.baseType();
        Id baseValue = label.baseValue();
        if (baseType == HugeType.VERTEX_LABEL) {
            VertexLabel vertexLabel = this.getVertexLabel(baseValue);
            vertexLabel.removeIndexLabel(label.id());
            addVertexLabel(vertexLabel);
        } else {
            assert baseType == HugeType.EDGE_LABEL;
            EdgeLabel edgeLabel = this.getEdgeLabel(baseValue);
            edgeLabel.removeIndexLabel(label.id());
            addEdgeLabel(edgeLabel);
        }
    }

    @Override
    public void commit() throws BackendException {
        try {
            super.commit();
        } catch (Throwable e) {
            // TODO: use event to replace direct call
            this.graph().graphTransaction().reset();
            throw e;
        }
        // TODO: use event to replace direct call
        this.graph().graphTransaction().commit();
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
}
