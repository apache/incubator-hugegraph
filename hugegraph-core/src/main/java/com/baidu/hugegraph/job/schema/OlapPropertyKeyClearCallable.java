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

import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.StandardHugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.type.define.SchemaStatus;
import com.baidu.hugegraph.util.LockUtil;

public class OlapPropertyKeyClearCallable extends IndexLabelRemoveCallable {

    @Override
    public String type() {
        return CLEAR_OLAP;
    }

    @Override
    public Object execute() {
        Id olap = this.schemaId();

        // Clear corresponding index data
        clearIndexLabel(this.params(), olap);

        // Clear olap table
        this.params().schemaTransaction().clearOlapPk(olap);
        return null;
    }

    protected static void clearIndexLabel(HugeGraphParams graph, Id id) {
        Id olapIndexLabel = findOlapIndexLabel(graph, id);
        if (olapIndexLabel == null) {
            return;
        }
        GraphTransaction graphTx = graph.graphTransaction();
        SchemaTransaction schemaTx = graph.schemaTransaction();
        IndexLabel indexLabel = schemaTx.getIndexLabel(olapIndexLabel);
        // If the index label does not exist, return directly
        if (indexLabel == null) {
            return;
        }
        String spaceGraph = ((StandardHugeGraph) graph.graph()).spaceGraphName();
        LockUtil.Locks locks = new LockUtil.Locks(spaceGraph);
        try {
            locks.lockWrites(LockUtil.INDEX_LABEL_CLEAR, olapIndexLabel);
            // Set index label to "clearing" status
            schemaTx.updateSchemaStatus(indexLabel, SchemaStatus.CLEARING);
            try {
                // Remove index data
                graphTx.removeIndex(indexLabel);
                /*
                 * Should commit changes to backend store before release
                 * delete lock
                 */
                graph.graph().tx().commit();
                schemaTx.updateSchemaStatus(indexLabel, SchemaStatus.CREATED);
            } catch (Throwable e) {
                schemaTx.updateSchemaStatus(indexLabel, SchemaStatus.INVALID);
                throw e;
            }
        } finally {
            locks.unlock();
        }
    }

    protected static Id findOlapIndexLabel(HugeGraphParams graph, Id olap) {
        SchemaTransaction schemaTx = graph.schemaTransaction();
        for (IndexLabel indexLabel : schemaTx.getIndexLabels()) {
            if (indexLabel.indexFields().contains(olap)) {
                return indexLabel.id();
            }
        }
        return null;
    }
}
