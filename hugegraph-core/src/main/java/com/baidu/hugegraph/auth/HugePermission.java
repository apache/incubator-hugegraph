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

    // general
    ANY(0),
    SCHEMA_ANY(1),
    VERTEX_ANY(2),
    EDGE_ANY(3),

    READ(15),
    WRITE(16),
    DELETE(17),

    // global
    GREMLIN(20),
    JOB_GREMLIN(21),

    TASK_READ(31),
    TASK_WRITE(32),
    TASK_DELETE(33),

    META_READ(34),
    META_WRITE(35),

    // schema
    SCHEMA_READ(50),
    SCHEMA_WRITE(51),
    SCHEMA_DELETE(52),

    // vertex
    VERTEX_READ(100),
    VERTEX_WRITE(101),
    VERTEX_DELETE(102),

    // edge
    EDGE_READ(150),
    EDGE_WRITE(151),
    EDGE_DELETE(152),

    // path
    PATH_READ(200),

    // variables
    VAR_READ(210),
    VAR_WRITE(211),
    VAR_DELETE(212),

    // index
    INDEX_REBUILD(220);

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
        String string = this.name().toLowerCase();
        if (0 <= this.code && this.code < 15) {
            int offset = string.indexOf('_');
            if (offset >= 0) {
                string = string.substring(0, offset);
            } else {
                string = "";
            }
            string += ".*";
        } else if (15 <= this.code && this.code < 20) {
            string = ".*" + string;
        }
        return string;
    }

    public static HugePermission fromCode(byte code) {
        return SerialEnum.fromCode(HugePermission.class, code);
    }
}
