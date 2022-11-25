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

package org.apache.hugegraph.backend.store.palo;

import org.apache.hugegraph.backend.serializer.TableBackendEntry;
import org.apache.hugegraph.backend.store.mysql.MysqlSerializer;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.schema.SchemaLabel;
import org.apache.hugegraph.type.define.HugeKeys;

public class PaloSerializer extends MysqlSerializer {

    public PaloSerializer(HugeConfig config) {
        super(config);
    }

    @Override
    protected void writeEnableLabelIndex(SchemaLabel schema,
                                         TableBackendEntry entry) {
        Byte enable = (byte) (schema.enableLabelIndex() ? 1 : 0);
        entry.column(HugeKeys.ENABLE_LABEL_INDEX, enable);
    }

    @Override
    protected void readEnableLabelIndex(SchemaLabel schema,
                                        TableBackendEntry entry) {
        Number enable = entry.column(HugeKeys.ENABLE_LABEL_INDEX);
        schema.enableLabelIndex(enable.byteValue() != 0);
    }
}
