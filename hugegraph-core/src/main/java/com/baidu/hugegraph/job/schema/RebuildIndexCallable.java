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

package com.baidu.hugegraph.job.schema;

import java.util.Collection;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.schema.SchemaLabel;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.SchemaStatus;
import com.baidu.hugegraph.util.LockUtil;
import com.google.common.collect.ImmutableSet;

public class RebuildIndexCallable extends SchemaCallable {

    @Override
    public String type() {
        return SchemaCallable.REBUILD_INDEX;
    }

    @Override
    public Object execute() {
        this.rebuildIndex(this.schemaElement());
        return null;
    }

    private void rebuildIndex(SchemaElement schema) {
        switch (schema.type()) {
            case INDEX_LABEL:
                IndexLabel indexLabel = (IndexLabel) schema;
                SchemaLabel label;
                if (indexLabel.baseType() == HugeType.VERTEX_LABEL) {
                    label = this.graph().vertexLabel(indexLabel.baseValue());
                } else {
                    assert indexLabel.baseType() == HugeType.EDGE_LABEL;
                    label = this.graph().edgeLabel(indexLabel.baseValue());
                }
                this.rebuildIndex(label, ImmutableSet.of(indexLabel.id()));
                break;
            case VERTEX_LABEL:
            case EDGE_LABEL:
                label = (SchemaLabel) schema;
                this.rebuildIndex(label, label.indexLabels());
                break;
            default:
                assert schema.type() == HugeType.PROPERTY_KEY;
                throw new AssertionError(String.format(
                          "The %s can't rebuild index", schema.type()));
        }
    }

    private void rebuildIndex(SchemaLabel label, Collection<Id> indexLabelIds) {
        SchemaTransaction schemaTx = this.graph().schemaTransaction();
        GraphTransaction graphTx = this.graph().graphTransaction();

        Consumer<?> indexUpdater = (elem) -> {
            for (Id id : indexLabelIds) {
                graphTx.updateIndex(id, (HugeElement) elem);
                /*
                 * Commit per batch to avoid too much data in single commit,
                 * especially for Cassandra backend
                 */
                graphTx.commitIfGtSize(GraphTransaction.COMMIT_BATCH);
            }
        };

        LockUtil.Locks locks = new LockUtil.Locks(this.graph().name());
        try {
            locks.lockWrites(LockUtil.INDEX_LABEL_REBUILD, indexLabelIds);
            locks.lockWrites(LockUtil.INDEX_LABEL_DELETE, indexLabelIds);

            Set<IndexLabel> ils = indexLabelIds.stream()
                                               .map(schemaTx::getIndexLabel)
                                               .collect(Collectors.toSet());
            for (IndexLabel il : ils) {
                if (il.status() == SchemaStatus.CREATING) {
                    continue;
                }
                schemaTx.updateSchemaStatus(il, SchemaStatus.REBUILDING);
            }

            this.removeIndex(indexLabelIds);
            /*
             * Note: Here must commit index transaction firstly.
             * Because remove index convert to (id like <?>:personByCity):
             * `delete from index table where label = ?`,
             * But append index will convert to (id like Beijing:personByCity):
             * `update index element_ids += xxx where field_value = ?
             * and index_label_name = ?`,
             * They have different id lead to it can't compare and optimize
             */
            graphTx.commit();
            if (label.type() == HugeType.VERTEX_LABEL) {
                @SuppressWarnings("unchecked")
                Consumer<Vertex> consumer = (Consumer<Vertex>) indexUpdater;
                graphTx.traverseVerticesByLabel((VertexLabel) label, consumer);
            } else {
                assert label.type() == HugeType.EDGE_LABEL;
                @SuppressWarnings("unchecked")
                Consumer<Edge> consumer = (Consumer<Edge>) indexUpdater;
                graphTx.traverseEdgesByLabel((EdgeLabel) label, consumer);
            }
            graphTx.commit();

            for (IndexLabel il : ils) {
                schemaTx.updateSchemaStatus(il, SchemaStatus.CREATED);
            }
        } finally {
            locks.unlock();
        }
    }

    private void removeIndex(Collection<Id> indexLabelIds) {
        SchemaTransaction schemaTx = this.graph().schemaTransaction();
        GraphTransaction graphTx = this.graph().graphTransaction();

        for (Id id : indexLabelIds) {
            IndexLabel indexLabel = schemaTx.getIndexLabel(id);
            if (indexLabel == null) {
                /*
                 * TODO: How to deal with non-existent index name:
                 * continue or throw exception?
                 */
                continue;
            }
            graphTx.removeIndex(indexLabel);
        }
    }

    private SchemaElement schemaElement() {
        HugeType type = this.schemaType();
        Id id = this.schemaId();
        SchemaTransaction schemaTx = this.graph().schemaTransaction();
        switch (type) {
            case VERTEX_LABEL:
                return schemaTx.getVertexLabel(id);
            case EDGE_LABEL:
                return schemaTx.getEdgeLabel(id);
            case INDEX_LABEL:
                return schemaTx.getIndexLabel(id);
            default:
                throw new AssertionError(String.format(
                          "Invalid HugeType '%s' for rebuild", type));
        }
    }
}