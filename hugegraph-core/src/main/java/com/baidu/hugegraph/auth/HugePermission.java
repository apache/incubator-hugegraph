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

package com.baidu.hugegraph.auth;

import com.baidu.hugegraph.type.define.SerialEnum;

public enum HugePermission implements SerialEnum {

    NONE(0x00),

    READ(0x01),
    WRITE(0x02),
    DELETE(0x04),
    EXECUTE(0x08),

    ANY(0x7f);

    private final byte code;

    static {
        SerialEnum.register(HugePermission.class);
    }

    HugePermission(int code) {
        assert code < 256;
        this.code = (byte) code;
    }

    @Override
    public byte code() {
        return this.code;
    }

    public String string() {
        return this.name().toLowerCase();
    }

    public boolean match(HugePermission other) {
        if (other == ANY) {
            return this == ANY;
        }
        return (this.code & other.code) != 0;
    }

    public static HugePermission fromCode(byte code) {
        return SerialEnum.fromCode(HugePermission.class, code);
    }
}
