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

package com.baidu.hugegraph.traversal.algorithm.records.record;

public class RecordFactory {

    public static Record newRecord(RecordType type) {
        return newRecord(type, true);
    }

    public static Record newRecord(RecordType type, boolean single) {
        Record record;
        switch (type) {
            case ARRAY:
                record = new IntArrayRecord();
                break;
            case SET:
                record = new IntSetRecord();
                break;
            case INT:
                record = new IntIntRecord();
                break;
            default:
                throw new AssertionError("Unsupported record type: " + type);
        }
        if (!single) {
            record = new SyncRecord(record);
        }

        return record;
    }
}
