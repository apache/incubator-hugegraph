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

import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.schema.PropertyKey;

public class OlapPropertyKeyCreateCallable extends SchemaCallable {

    @Override
    public String type() {
        return CREATE_OLAP;
    }

    @Override
    public Object execute() {
        SchemaTransaction schemaTx = this.params().schemaTransaction();
        PropertyKey propertyKey = schemaTx.getPropertyKey(this.schemaId());
        schemaTx.createIndexLabelForOlapPk(propertyKey);
        schemaTx.createOlapPk(this.schemaId());
        return null;
    }
}
