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

    private final StoreType type;
    private final StoreAction action;
    private final byte[] data;

    public StoreCommand(StoreType type, StoreAction action, byte[] data) {
        this.type = type;
        this.action = action;
        if (data == null) {
            this.data = new byte[2];
        } else {
            assert data.length >= 2;
            this.data = data;
        }
        this.data[0] = (byte) this.type.getNumber();
        this.data[1] = (byte) this.action.getNumber();
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

    public static byte[] wrap(byte value) {
        byte[] bytes = new byte[3];
        bytes[2] = value;
        return bytes;
    }

    public static StoreCommand fromBytes(byte[] bytes) {
        StoreType type = StoreType.valueOf(bytes[0]);
        StoreAction action = StoreAction.valueOf(bytes[1]);
        return new StoreCommand(type, action, bytes);
    }
}
