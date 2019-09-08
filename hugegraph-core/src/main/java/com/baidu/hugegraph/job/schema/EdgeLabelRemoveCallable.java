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

import java.util.Set;

import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.type.define.SchemaStatus;
import com.baidu.hugegraph.util.LockUtil;
import com.google.common.collect.ImmutableSet;

public class EdgeLabelRemoveCallable extends SchemaCallable {

    @Override
    public String type() {
        return SchemaCallable.REMOVE_SCHEMA;
    }

    @Override
    public Object execute() {
        removeEdgeLabel(this.params(), this.schemaId());
        return null;
    }

    protected static void removeEdgeLabel(HugeGraphParams graph, Id id) {
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
        LockUtil.Locks locks = new LockUtil.Locks(graph.name());
        try {
            locks.lockWrites(LockUtil.EDGE_LABEL_DELETE, id);
            schemaTx.updateSchemaStatus(edgeLabel, SchemaStatus.DELETING);
            for (Id indexId : indexIds) {
                IndexLabelRemoveCallable.removeIndexLabel(graph, indexId);
            }
            // Remove all edges which has matched label
            graphTx.removeEdges(edgeLabel);
            removeSchema(schemaTx, edgeLabel);
            // Should commit changes to backend store before release delete lock
            graph.graph().tx().commit();
        } finally {
            locks.unlock();
        }
    }
}
