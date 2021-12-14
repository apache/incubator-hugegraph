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

package com.baidu.hugegraph.traversal.algorithm.records;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.traversal.algorithm.records.record.Record;
import com.baidu.hugegraph.traversal.algorithm.records.record.RecordFactory;
import com.baidu.hugegraph.traversal.algorithm.records.record.RecordType;
import com.baidu.hugegraph.util.collection.MappingFactory;
import com.baidu.hugegraph.util.collection.ObjectIntMapping;

public abstract class AbstractRecords implements Records {

    private final ObjectIntMapping<Id> idMapping;
    private final RecordType type;
    private final boolean concurrent;
    private Record currentRecord;
    private Record parentRecord;

    public AbstractRecords(RecordType type, boolean concurrent) {
        this.type = type;
        this.concurrent = concurrent;
        this.parentRecord = null;
        this.idMapping = MappingFactory.newObjectIntMapping(this.concurrent);
    }

    @Watched
    protected final int code(Id id) {
        if (id.number()) {
            long l = id.asLong();
            if (0 <= l && l <= Integer.MAX_VALUE) {
                return (int) l;
            }
        }
        int code = this.idMapping.object2Code(id);
        assert code > 0;
        return -code;
    }

    @Watched
    protected final Id id(int code) {
        if (code >= 0) {
            return IdGenerator.of(code);
        }
        return this.idMapping.code2Object(-code);
    }

    protected final Record newRecord() {
        return RecordFactory.newRecord(this.type, this.concurrent);
    }

    protected final Record currentRecord() {
        return this.currentRecord;
    }

    protected void currentRecord(Record currentRecord, Record parentRecord) {
        this.parentRecord = parentRecord;
        this.currentRecord = currentRecord;
    }

    protected Record parentRecord() {
        return this.parentRecord;
    }
}
