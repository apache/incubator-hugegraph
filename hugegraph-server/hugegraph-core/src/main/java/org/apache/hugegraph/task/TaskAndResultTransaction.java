/*
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

package org.apache.hugegraph.task;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.structure.HugeVertex;

public class TaskAndResultTransaction extends TaskTransaction {

    public static final String TASKRESULT = HugeTaskResult.P.TASKRESULT;

    /**
     * Task transactions, for persistence
     */
    protected volatile TaskAndResultTransaction taskTx = null;

    public TaskAndResultTransaction(HugeGraphParams graph, BackendStore store) {
        super(graph, store);
        this.autoCommit(true);
    }

    public HugeVertex constructTaskVertex(HugeTask<?> task) {
        if (!this.graph().existsVertexLabel(TASK)) {
            throw new HugeException("Schema is missing for task(%s) '%s'",
                                    task.id(), task.name());
        }

        return this.constructVertex(false, task.asArrayWithoutResult());
    }

    public HugeVertex constructTaskResultVertex(HugeTaskResult taskResult) {
        if (!this.graph().existsVertexLabel(TASKRESULT)) {
            throw new HugeException("Schema is missing for task result");
        }

        return this.constructVertex(false, taskResult.asArray());
    }
}
