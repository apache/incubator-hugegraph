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
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.type.define.SchemaStatus;
import com.baidu.hugegraph.util.LockUtil;

public class IndexLabelRemoveCallable extends SchemaCallable {

    @Override
    public String type() {
        return SchemaCallable.REMOVE_SCHEMA;
    }

    @Override
    public Object execute() {
        removeIndexLabel(this.params(), this.schemaId());
        return null;
    }

    protected static void removeIndexLabel(HugeGraphParams graph, Id id) {
        GraphTransaction graphTx = graph.graphTransaction();
        SchemaTransaction schemaTx = graph.schemaTransaction();
        IndexLabel indexLabel = schemaTx.getIndexLabel(id);
        // If the index label does not exist, return directly
        if (indexLabel == null) {
            return;
        }
        LockUtil.Locks locks = new LockUtil.Locks(graph.name());
        try {
            locks.lockWrites(LockUtil.INDEX_LABEL_DELETE, id);
            // TODO add update lock
            // Set index label to "deleting" status
            schemaTx.updateSchemaStatus(indexLabel, SchemaStatus.DELETING);
            try {
                // Remove index data
                // TODO: use event to replace direct call
                graphTx.removeIndex(indexLabel);
                // Remove label from indexLabels of vertex or edge label
                removeIndexLabelFromBaseLabel(schemaTx, indexLabel);
                removeSchema(schemaTx, indexLabel);
                /*
                 * Should commit changes to backend store before release
                 * delete lock
                 */
                graph.graph().tx().commit();
            } catch (Throwable e) {
                schemaTx.updateSchemaStatus(indexLabel, SchemaStatus.INVALID);
                throw e;
            }
        } finally {
            locks.unlock();
        }
    }
}
