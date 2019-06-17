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

import com.baidu.hugegraph.type.HugeType;

public enum IndexType implements SerialEnum {

    // For secondary query
    SECONDARY(1, "secondary"),

    // For range query
    RANGE(2, "range"),

    // For full-text query (not supported now)
    SEARCH(3, "search"),

    // For prefix + range query
    SHARD(4, "shard");

    private byte code = 0;
    private String name = null;

    static {
        SerialEnum.register(IndexType.class);
    }

    IndexType(int code, String name) {
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

    public HugeType type() {
        switch (this) {
            case SECONDARY:
                return HugeType.SECONDARY_INDEX;
            case RANGE:
                return HugeType.RANGE_INDEX;
            case SEARCH:
                return HugeType.SEARCH_INDEX;
            case SHARD:
                return HugeType.SHARD_INDEX;
            default:
                throw new AssertionError(String.format(
                          "Unknown index type '%s'", this));
        }
    }
}
