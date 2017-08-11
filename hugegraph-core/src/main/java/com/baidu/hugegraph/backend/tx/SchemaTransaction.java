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
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.baidu.hugegraph.backend.tx;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.exception.NotAllowException;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.schema.SchemaLabel;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.LockUtil;
import com.google.common.collect.ImmutableSet;

public class SchemaTransaction extends AbstractTransaction {

    public SchemaTransaction(HugeGraph graph, BackendStore store) {
        super(graph, store);
    }

    public List<PropertyKey> getPropertyKeys() {
        List<PropertyKey> propertyKeys = new ArrayList<>();
        Query q = new Query(HugeType.PROPERTY_KEY);
        Iterable<BackendEntry> entries = query(q);
        entries.forEach(entry -> {
            propertyKeys.add(this.serializer.readPropertyKey(entry));
        });
        return propertyKeys;
    }

    public List<VertexLabel> getVertexLabels() {
        List<VertexLabel> vertexLabels = new ArrayList<>();
        Query q = new Query(HugeType.VERTEX_LABEL);
        Iterable<BackendEntry> entries = query(q);
        entries.forEach(entry -> {
            vertexLabels.add(this.serializer.readVertexLabel(entry));
        });
        return vertexLabels;
    }

    public List<EdgeLabel> getEdgeLabels() {
        List<EdgeLabel> edgeLabels = new ArrayList<>();
        Query q = new Query(HugeType.EDGE_LABEL);
        Iterable<BackendEntry> entries = query(q);
        entries.forEach(entry -> {
            edgeLabels.add(this.serializer.readEdgeLabel(entry));
        });
        return edgeLabels;
    }

    public List<IndexLabel> getIndexLabels() {
        List<IndexLabel> indexLabels = new ArrayList<>();
        Query q = new Query(HugeType.INDEX_LABEL);
        Iterable<BackendEntry> entries = query(q);
        entries.forEach(entry -> {
            indexLabels.add(this.serializer.readIndexLabel(entry));
        });
        return indexLabels;
    }

    public void addPropertyKey(PropertyKey propKey) {
        logger.debug("SchemaTransaction add property key: {}", propKey);
        this.addSchema(propKey, this.serializer.writePropertyKey(propKey));
    }

    public PropertyKey getPropertyKey(String name) {
        BackendEntry entry = querySchema(new PropertyKey(name));
        return this.serializer.readPropertyKey(entry);
    }

    public void removePropertyKey(String name) {
        List<VertexLabel> vertexLabels = this.getVertexLabels();
        for (VertexLabel vertexLabel : vertexLabels) {
            if (vertexLabel.properties().contains(name)) {
                throw new NotAllowException(
                          "Not allowed to remove property key: '%s' " +
                          "because the vertex label '%s' is still using it.",
                          name, vertexLabel.name());
            }
        }
        List<EdgeLabel> edgeLabels = this.getEdgeLabels();
        for (EdgeLabel edgeLabel : edgeLabels) {
            if (edgeLabel.properties().contains(name)) {
                throw new NotAllowException(
                          "Not allowed to remove property key: '%s' " +
                          "because the edge label '%s' is still using it.",
                          name, edgeLabel.name());
            }
        }
        logger.debug("SchemaTransaction remove property key '{}'", name);
        this.removeSchema(new PropertyKey(name));
    }

    public void addVertexLabel(VertexLabel vertexLabel) {
        logger.debug("SchemaTransaction add vertex label: {}", vertexLabel);
        BackendEntry entry = this.serializer.writeVertexLabel(vertexLabel);
        this.addSchema(vertexLabel, entry);
    }

    public VertexLabel getVertexLabel(String name) {
        BackendEntry entry = querySchema(new VertexLabel(name));
        return this.serializer.readVertexLabel(entry);
    }

    public void removeVertexLabel(String name) {
        VertexLabel vertexLabel = this.getVertexLabel(name);
        // If the vertex label does not exist, return directly
        if (vertexLabel == null) {
            return;
        }

        List<EdgeLabel> edgeLabels = this.getEdgeLabels();
        for (EdgeLabel edgeLabel : edgeLabels) {
            if (edgeLabel.linkWithLabel(name)) {
                throw new HugeException("Not allowed to remove vertex label " +
                                        "'%s' because the edge label '%s' " +
                                        "still link with it",
                                        name, edgeLabel.name());
            }
        }
        /*
         *  Copy index names because removeIndexLabel will mutate
         *  vertexLabel.indexNames()
         */
        Set<String> indexNames = ImmutableSet.copyOf(vertexLabel.indexNames());
        LockUtil.Locks locks = new LockUtil.Locks();
        try {
            locks.lockWrites(LockUtil.VERTEX_LABEL, name);
            for (String indexName : indexNames) {
                this.removeIndexLabel(indexName);
            }

            // TODO: use event to replace direct call
            // Deleting a vertex will automatically deletes the held edge
            this.graph().graphTransaction().removeVertices(vertexLabel);
            logger.debug("SchemaTransaction remove vertex label '{}'", name);
            this.removeSchema(new VertexLabel(name));
        } finally {
            locks.unlock();
        }
    }

    public void addEdgeLabel(EdgeLabel edgeLabel) {
        logger.debug("SchemaTransaction add edge label: {}", edgeLabel);
        this.addSchema(edgeLabel, this.serializer.writeEdgeLabel(edgeLabel));
    }

    public EdgeLabel getEdgeLabel(String name) {
        BackendEntry entry = querySchema(new EdgeLabel(name));
        return this.serializer.readEdgeLabel(entry);
    }

    public void removeEdgeLabel(String name) {
        EdgeLabel edgeLabel = this.getEdgeLabel(name);
        // If the edge label does not exist, return directly
        if (edgeLabel == null) {
            return;
        }
        // TODO: use event to replace direct call
        // Remove index related data(include schema) of this edge label
        Set<String> indexNames = ImmutableSet.copyOf(edgeLabel.indexNames());
        LockUtil.Locks locks = new LockUtil.Locks();
        try {
            locks.lockWrites(LockUtil.EDGE_LABEL, name);
            for (String indexName : indexNames) {
                this.removeIndexLabel(indexName);
            }
            // Remove all edges which has matched label
            this.graph().graphTransaction().removeEdges(edgeLabel);

            logger.debug("SchemaTransaction remove edge label '{}'", name);
            this.removeSchema(new EdgeLabel(name));
        } finally {
            locks.unlock();
        }
    }

    public void addIndexLabel(IndexLabel indexLabel) {
        logger.debug("SchemaTransaction add index label: {}", indexLabel);
        this.addSchema(indexLabel, this.serializer.writeIndexLabel(indexLabel));
    }

    public IndexLabel getIndexLabel(String name) {
        BackendEntry entry = querySchema(new IndexLabel(name));
        return this.serializer.readIndexLabel(entry);
    }

    public void removeIndexLabel(String indexName) {
        logger.debug("SchemaTransaction remove index label '{}'", indexName);
        LockUtil.Locks locks = new LockUtil.Locks();
        try {
            locks.lockWrites(LockUtil.INDEX_LABEL, indexName);
            // Remove index data
            // TODO: use event to replace direct call
            this.graph().graphTransaction().removeIndex(indexName);
            // Remove indexName from indexNames of vertex label or edge label
            this.removeIndexNames(indexName);
            this.removeSchema(new IndexLabel(indexName));
        } finally {
            locks.unlock();
        }
    }

    public void rebuildIndex(IndexLabel indexLabel) {
        logger.debug("SchemaTransaction rebuild index for '{}' '{}'",
                     indexLabel.type(), indexLabel.name());
        // Obtain index label from db by name
        indexLabel = this.getIndexLabel(indexLabel.name());
        this.graph().graphTransaction().rebuildIndex(indexLabel);
    }

    public void rebuildIndex(SchemaLabel schemaLabel) {
        logger.debug("SchemaTransaction rebuild index for '{}' '{}'",
                     schemaLabel.type(), schemaLabel.name());
        // Obtain vertex/edge label from db by name
        if (schemaLabel.type() == HugeType.VERTEX_LABEL) {
            schemaLabel = this.getVertexLabel(schemaLabel.name());
        } else {
            assert schemaLabel.type() == HugeType.EDGE_LABEL;
            schemaLabel = this.getEdgeLabel(schemaLabel.name());
        }
        this.graph().graphTransaction().rebuildIndex(schemaLabel);
    }

    protected void addSchema(SchemaElement schemaElement, BackendEntry entry) {
        this.beforeWrite();
        this.addEntry(entry);
        this.afterWrite();
    }

    protected BackendEntry querySchema(SchemaElement element) {
        Id id = IdGenerator.of(element);
        this.beforeRead();
        BackendEntry entry = this.query(element.type(), id);
        this.afterRead();
        return entry;
    }

    protected void removeSchema(SchemaElement element) {
        Id id = IdGenerator.of(element);
        this.beforeWrite();
        this.removeEntry(element.type(), id);
        this.afterWrite();
    }

    protected void removeIndexNames(String indexName) {
        IndexLabel label = this.getIndexLabel(indexName);
        HugeType baseType = label.baseType();
        String baseValue = label.baseValue();
        if (baseType == HugeType.VERTEX_LABEL) {
            VertexLabel vertexLabel = this.getVertexLabel(baseValue);
            vertexLabel.indexNames().remove(indexName);
            addVertexLabel(vertexLabel);
        } else {
            assert baseType == HugeType.EDGE_LABEL;
            EdgeLabel edgeLabel = this.getEdgeLabel(baseValue);
            edgeLabel.indexNames().remove(indexName);
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
}
