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

package com.baidu.hugegraph.backend.store.hbase;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.serializer.BinaryBackendEntry;
import com.baidu.hugegraph.backend.serializer.BinarySerializer;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumn;
import com.baidu.hugegraph.structure.HugeIndex;

public class HbaseSerializer extends BinarySerializer {

    public HbaseSerializer() {
        super(false);
    }

    @Override
    protected byte[] formatIndexName(HugeIndex index) {
        Id elemId = index.elementId();
        BytesBuffer buffer = BytesBuffer.allocate(1 + elemId.length());
        // Write element-id
        buffer.writeId(elemId, true);
        return buffer.bytes();
    }

    @Override
    protected void parseIndexName(BinaryBackendEntry entry, HugeIndex index) {
        for (BackendColumn col : entry.columns()) {
            BytesBuffer buffer = BytesBuffer.wrap(col.name);
            index.elementIds(buffer.readId(true));
        }
    }
}
