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

package com.baidu.hugegraph.unit.serializer;

import java.util.Iterator;

import org.junit.Test;

import com.baidu.hugegraph.backend.serializer.BinaryBackendEntry;
import com.baidu.hugegraph.backend.store.BackendAction;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.raft.StoreCommand;
import com.baidu.hugegraph.backend.store.raft.StoreSerializer;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Action;

public class StoreSerializerTest {

    @Test
    public void testSerializeBackendMutation() {
        BinaryBackendEntry entry = new BinaryBackendEntry(HugeType.VERTEX,
                                                          new byte[]{1, 2});
        entry.column(new byte[]{1}, new byte[]{1});
        entry.column(new byte[]{2}, new byte[]{2});
        entry.column(new byte[]{127}, new byte[]{127});
        BackendMutation origin = new BackendMutation();
        origin.add(entry, Action.INSERT);
        byte[] bytes = StoreSerializer.serializeMutation(origin);

        BackendMutation actual = StoreSerializer.deserializeMutation(bytes);
        Assert.assertEquals(1, actual.size());
        Iterator<BackendAction> iter = actual.mutation();
        while (iter.hasNext()) {
            BackendAction item = iter.next();
            Assert.assertEquals(Action.INSERT, item.action());
            BackendEntry e = item.entry();
            Assert.assertEquals(entry.type(), e.type());
            Assert.assertEquals(entry.id(), e.id());
            Assert.assertEquals(entry.subId(), e.subId());
            Assert.assertEquals(entry.ttl(), e.ttl());
            Assert.assertEquals(entry.columnsSize(), e.columnsSize());
            Assert.assertEquals(entry.columns(), e.columns());
        }
    }

    @Test
    public void testSerializeStoreCommand() {
        BinaryBackendEntry entry = new BinaryBackendEntry(HugeType.VERTEX,
                                                          new byte[]{1, 2});
        entry.column(new byte[]{1}, new byte[]{1});
        entry.column(new byte[]{2}, new byte[]{2});
        entry.column(new byte[]{127}, new byte[]{127});
        BackendMutation origin = new BackendMutation();
        origin.add(entry, Action.INSERT);
        byte[] mutationBytes = StoreSerializer.serializeMutation(origin);

        StoreCommand command = new StoreCommand(StoreCommand.MUTATE,
                                                mutationBytes);
        byte[] commandBytes = command.toBytes();
        StoreCommand actual = StoreCommand.fromBytes(commandBytes);
        Assert.assertEquals(command.command(), actual.command());
        Assert.assertArrayEquals(command.data(), actual.data());
    }
}
