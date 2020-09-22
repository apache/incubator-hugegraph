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

import com.baidu.hugegraph.backend.store.raft.RaftRequests.StoreAction;
import com.baidu.hugegraph.backend.store.raft.RaftRequests.StoreType;

public class StoreCommand {

    private static byte[] EMPTY = new byte[0];

    private final StoreType type;
    private final StoreAction action;
    private final byte[] data;

    public StoreCommand(StoreType type, StoreAction action) {
        this(type, action, EMPTY);
    }

    public StoreCommand(StoreType type, StoreAction action, byte[] data) {
        this.type = type;
        this.action = action;
        this.data = data;
    }

    public StoreType type() {
        return this.type;
    }

    public StoreAction action() {
        return this.action;
    }

    public byte[] data() {
        return this.data;
    }

    public byte[] toBytes() {
        byte[] bytes = new byte[1 + 1 + this.data.length];
        bytes[0] = (byte) this.type.getNumber();
        bytes[1] = (byte) this.action.getNumber();
        System.arraycopy(this.data, 0, bytes, 2, this.data.length);
        return bytes;
    }

    public static StoreCommand fromBytes(byte[] bytes) {
        StoreType type = StoreType.valueOf(bytes[0]);
        StoreAction action = StoreAction.valueOf(bytes[1]);
        byte[] data = new byte[bytes.length - 2];
        System.arraycopy(bytes, 2, data, 0, bytes.length - 2);
        return new StoreCommand(type, action, data);
    }
}
