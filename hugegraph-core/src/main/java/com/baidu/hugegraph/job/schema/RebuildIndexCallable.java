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

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.type.HugeType;

public class RebuildIndexCallable extends SchemaCallable {

    @Override
    public String type() {
        return SchemaCallable.REBUILD_INDEX;
    }

    @Override
    protected void runTask() {
        this.graph().graphTransaction().rebuildIndex(this.schemaElement());
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