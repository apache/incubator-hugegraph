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

package com.baidu.hugegraph.backend.store.postgresql;

import org.apache.logging.log4j.util.Strings;

import com.baidu.hugegraph.backend.id.IdUtil;
import com.baidu.hugegraph.backend.serializer.TableBackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.mysql.MysqlSerializer;
import com.baidu.hugegraph.structure.HugeIndex;
import com.baidu.hugegraph.type.define.HugeKeys;

public class PostgresqlSerializer extends MysqlSerializer {

    @Override
    public BackendEntry writeIndex(HugeIndex index) {
        TableBackendEntry entry = newBackendEntry(index);
        /*
         * When field-values is null and elementIds size is 0, it is
         * meaningful for deletion of index data in secondary/range index.
         */
        if (index.fieldValues() == null && index.elementIds().size() == 0) {
            entry.column(HugeKeys.INDEX_LABEL_ID, index.indexLabel().asLong());
        } else {
            Object value = index.fieldValues();
            if (value != null && value.equals("\u0000")) {
                value = Strings.EMPTY;
            }
            entry.column(HugeKeys.FIELD_VALUES, value);
            entry.column(HugeKeys.INDEX_LABEL_ID, index.indexLabel().asLong());
            entry.column(HugeKeys.ELEMENT_IDS,
                         IdUtil.writeString(index.elementId()));
            entry.subId(index.elementId());
        }
        return entry;
    }
}
