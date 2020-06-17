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

public class StoreCommand {

    private static byte[] EMPTY = new byte[0];

    private final StoreAction action;
    private final byte[] data;

    public StoreCommand(StoreAction action) {
        this(action, EMPTY);
    }

    public StoreCommand(StoreAction action, byte[] data) {
        this.action = action;
        this.data = data;
    }

    public StoreAction action() {
        return this.action;
    }

    public byte[] data() {
        return this.data;
    }

    public byte[] toBytes() {
        byte[] bytes = new byte[1 + this.data.length];
        bytes[0] = this.action.code();
        System.arraycopy(this.data, 0, bytes, 1, this.data.length);
        return bytes;
    }

    public static StoreCommand fromBytes(byte[] bytes) {
        StoreAction action = StoreAction.fromCode(bytes[0]);
        byte[] data = new byte[bytes.length - 1];
        System.arraycopy(bytes, 1, data, 0, bytes.length - 1);
        return new StoreCommand(action, data);
    }
}
