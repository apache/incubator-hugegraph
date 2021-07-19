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

package com.baidu.hugegraph.type.define;

public enum HugeGroupTag implements SerialEnum {

    SUPER_ADMIN(1, "Super administrator"),
    OP_SUPER_ADMIN(2, "Cluster operation and maintenance administrator"),
    PROJECT_ADMIN(3, "Project manager"),
    OP_PROJECT_ADMIN(4, "Project operation and maintenance administrator");

    private final byte code;
    private final String name;

    HugeGroupTag(int code, String name) {
        assert code < 256;
        this.code = (byte) code;
        this.name = name;
    }

    static {
        SerialEnum.register(HugeGroupTag.class);
    }

    @Override
    public byte code() {
        return this.code;
    }

    public String string() {
        return this.name;
    }

    public static HugeGroupTag fromCode(byte code) {
        return SerialEnum.fromCode(HugeGroupTag.class, code);
    }
}
