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

import java.util.Iterator;

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

    public static byte[] serializeMutation(BackendMutation mutation) {
        int sizePerEntry = 32;
        BytesBuffer buffer = BytesBuffer.allocate(mutation.size() * sizePerEntry);
        // write mutation size
        buffer.writeInt(mutation.size());
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
            buffer.writeLong(entry.ttl());
            // write columns
            buffer.writeInt(entry.columns().size());
            for (BackendColumn column : entry.columns()) {
                buffer.writeBytes(column.name);
                buffer.writeBytes(column.value);
            }
        }
        return buffer.bytes();
    }

    public static BackendMutation deserializeMutation(byte[] bytes) {
        BackendMutation mutation = new BackendMutation();
        BytesBuffer buffer = BytesBuffer.wrap(bytes);
        int size = buffer.readInt();
        for (int i = 0; i < size; i++) {
            // read action
            Action action = SerialEnum.fromCode(Action.class, buffer.read());
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
            long ttl = buffer.readLong();

            BinaryBackendEntry entry = new BinaryBackendEntry(type, idBytes);
            entry.subId(subId);
            entry.ttl(ttl);
            // read columns
            int columnsSize = buffer.readInt();
            for (int c = 0; c < columnsSize; c++) {
                byte[] name = buffer.readBytes();
                byte[] value = buffer.readBytes();
                entry.column(BackendColumn.of(name, value));
            }
            mutation.add(entry, action);
        }
        return mutation;
    }

    public static byte[] serializeIncrCounter(IncrCounter incrCounter) {
        BytesBuffer buffer = BytesBuffer.allocate(1 + BytesBuffer.LONG_LEN);
        buffer.write(incrCounter.type().code());
        buffer.writeLong(incrCounter.increment());
        return buffer.bytes();
    }

    public static IncrCounter deserializeIncrCounter(byte[] bytes) {
        BytesBuffer buffer = BytesBuffer.wrap(bytes);
        HugeType type = SerialEnum.fromCode(HugeType.class, buffer.read());
        long increment = buffer.readLong();
        return new IncrCounter(type, increment);
    }
}
