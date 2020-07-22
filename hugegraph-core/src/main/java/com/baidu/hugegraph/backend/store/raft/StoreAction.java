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

import com.baidu.hugegraph.type.define.SerialEnum;

public enum StoreAction implements SerialEnum {

    NONE(0, "none"),

    INIT(1, "init"),
    CLEAR(2, "clear"),
    TRUNCATE(3, "truncate"),

    BEGIN_TX(10, "begin_tx"),
    COMMIT_TX(11, "commit_tx"),
    ROLLBACK_TX(12, "rollback_tx"),

    MUTATE(20, "mutate"),
    INCR_COUNTER(21, "incr_counter"),

    QUERY(30, "query");

    private final byte code;
    private final String name;

    static {
        SerialEnum.register(StoreAction.class);
    }

    StoreAction(int code, String name) {
        assert code < 256;
        this.code = (byte) code;
        this.name = name;
    }

    @Override
    public byte code() {
        return this.code;
    }

    public String string() {
        return this.name;
    }

    public static StoreAction fromCode(byte code) {
        return SerialEnum.fromCode(StoreAction.class, code);
    }
}
