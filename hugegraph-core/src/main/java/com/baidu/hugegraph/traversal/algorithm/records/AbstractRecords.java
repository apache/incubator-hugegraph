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
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.traversal.algorithm.records.record.Record;
import com.baidu.hugegraph.traversal.algorithm.records.record.RecordFactory;
import com.baidu.hugegraph.traversal.algorithm.records.record.RecordType;
import com.baidu.hugegraph.util.collection.ObjectIntMapping;

public abstract class AbstractRecords implements Records {

    private final ObjectIntMapping<Id> idMapping;
    protected final RecordType type;
    private final boolean single;

    public AbstractRecords(RecordType type, boolean single) {
        this.idMapping = new ObjectIntMapping<>();
        this.type = type;
        this.single = single;
    }

    public AbstractRecords(RecordType type) {
        this.idMapping = new ObjectIntMapping<>();
        this.type = type;
        this.single = true;
    }

    @Watched
    protected int code(Id id) {
        return this.idMapping.object2Code(id);
    }

    @Watched
    protected Id id(int code) {
        return (Id) this.idMapping.code2Object(code);
    }

    protected Record newRecord() {
        return RecordFactory.newRecord(this.type, single);
    }
}

