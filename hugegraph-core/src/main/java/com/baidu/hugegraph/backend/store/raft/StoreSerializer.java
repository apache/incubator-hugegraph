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

package com.baidu.hugegraph.backend.store.raft;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.serializer.BinaryBackendEntry;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.backend.store.BackendAction;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumn;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.raft.RaftBackendStore.IncrCounter;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Action;
import com.baidu.hugegraph.type.define.SerialEnum;

public class StoreSerializer {

    public static byte[] writeMutations(List<BackendMutation> mutations) {
        int estimateSize = mutations.size() * 250 * 32;
        BytesBuffer buffer = BytesBuffer.allocate(1 + estimateSize);
        buffer.writeVInt(mutations.size());
        for (BackendMutation mutation : mutations) {
            buffer.writeBigBytes(writeMutation(mutation));
        }
        return buffer.bytes();
    }

    public static List<BackendMutation> readMutations(BytesBuffer buffer) {
        int size = buffer.readVInt();
        List<BackendMutation> mutations = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            BytesBuffer buf = BytesBuffer.wrap(buffer.readBigBytes());
            mutations.add(readMutation(buf));
        }
        return mutations;
    }

    public static byte[] writeMutation(BackendMutation mutation) {
        int sizePerEntry = 32;
        BytesBuffer buffer = BytesBuffer.allocate(mutation.size() * sizePerEntry);
        // write mutation size
        buffer.writeVInt(mutation.size());
        for (Iterator<BackendAction> items = mutation.mutation();
             items.hasNext();) {
            BackendAction item = items.next();
            // write Action
            buffer.write(item.action().code());

            BackendEntry entry = item.entry();
            // write HugeType
            buffer.write(entry.type().code());
            // write id
            buffer.writeBytes(entry.id().asBytes());
            // wirte subId
            if (entry.subId() != null) {
                buffer.writeId(entry.subId());
            } else {
                buffer.writeId(IdGenerator.ZERO);
            }
            // write ttl
            buffer.writeVLong(entry.ttl());
            // write columns
            buffer.writeVInt(entry.columns().size());
            for (BackendColumn column : entry.columns()) {
                buffer.writeBytes(column.name);
                buffer.writeBytes(column.value);
            }
        }
        return buffer.bytes();
    }

    public static BackendMutation readMutation(BytesBuffer buffer) {
        int size = buffer.readVInt();
        BackendMutation mutation = new BackendMutation(size);
        for (int i = 0; i < size; i++) {
            // read action
            Action action = Action.fromCode(buffer.read());
            // read HugeType
            HugeType type = SerialEnum.fromCode(HugeType.class, buffer.read());
            // read id
            byte[] idBytes = buffer.readBytes();
            // read subId
            Id subId = buffer.readId();
            if (subId.equals(IdGenerator.ZERO)) {
                subId = null;
            }
            // read ttl
            long ttl = buffer.readVLong();

            BinaryBackendEntry entry = new BinaryBackendEntry(type, idBytes);
            entry.subId(subId);
            entry.ttl(ttl);
            // read columns
            int columnsSize = buffer.readVInt();
            for (int c = 0; c < columnsSize; c++) {
                byte[] name = buffer.readBytes();
                byte[] value = buffer.readBytes();
                entry.column(BackendColumn.of(name, value));
            }
            mutation.put(entry, action);
        }
        return mutation;
    }

    public static byte[] writeIncrCounter(IncrCounter incrCounter) {
        BytesBuffer buffer = BytesBuffer.allocate(1 + BytesBuffer.LONG_LEN);
        buffer.write(incrCounter.type().code());
        buffer.writeVLong(incrCounter.increment());
        return buffer.bytes();
    }

    public static IncrCounter readIncrCounter(BytesBuffer buffer) {
        HugeType type = SerialEnum.fromCode(HugeType.class, buffer.read());
        long increment = buffer.readVLong();
        return new IncrCounter(type, increment);
    }
}
