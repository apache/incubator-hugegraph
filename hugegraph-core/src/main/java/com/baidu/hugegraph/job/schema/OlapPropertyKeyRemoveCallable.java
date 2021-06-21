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
import com.baidu.hugegraph.schema.PropertyKey;

public class OlapPropertyKeyRemoveCallable
       extends OlapPropertyKeyClearCallable {

    @Override
    public String type() {
        return REMOVE_OLAP;
    }

    @Override
    public Object execute() {
        Id olap = this.schemaId();

        // Remove corresponding index label and index data
        Id indexLabel = findOlapIndexLabel(this.params(), olap);
        if (indexLabel != null) {
            removeIndexLabel(this.params(), indexLabel);
        }

        // Remove olap table
        this.params().schemaTransaction().removeOlapPk(olap);

        // Remove olap property key
        SchemaTransaction schemaTx = this.params().schemaTransaction();
        PropertyKey propertyKey = schemaTx.getPropertyKey(olap);
        removeSchema(schemaTx, propertyKey);
        return null;
    }
}
