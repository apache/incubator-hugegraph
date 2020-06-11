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

    public static final byte NONE = 0x00;
    public static final byte BEGIN_TX = 0x01;
    public static final byte COMMIT_TX = 0x02;
    public static final byte ROLLBACK_TX = 0x03;
    public static final byte MUTATE = 0x04;
    public static final byte INCR_COUNTER = 0x05;
    public static final byte QUERY = 0x06;
    public static final byte INIT = 0x07;
    public static final byte CLEAR = 0x08;
    public static final byte TRUNCATE = 0x09;

    private final byte command;
    private final byte[] data;

    public StoreCommand(byte command) {
        this(command, EMPTY);
    }

    public StoreCommand(byte command, byte[] data) {
        this.command = command;
        this.data = data;
    }

    public byte command() {
        return this.command;
    }

    public byte[] data() {
        return this.data;
    }

    public byte[] toBytes() {
        byte[] bytes = new byte[1 + this.data.length];
        bytes[0] = this.command;
        System.arraycopy(this.data, 0, bytes, 1, this.data.length);
        return bytes;
    }

    public static StoreCommand fromBytes(byte[] bytes) {
        byte command = bytes[0];
        byte[] data = new byte[bytes.length - 1];
        System.arraycopy(bytes, 1, data, 0, bytes.length - 1);
        return new StoreCommand(command, data);
    }
}
