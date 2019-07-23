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

package com.baidu.hugegraph.type;

import java.util.HashMap;
import java.util.Map;

import com.baidu.hugegraph.type.define.SerialEnum;

public enum HugeType implements SerialEnum {

    UNKNOWN(0, "UNKNOWN"),

    /* Schema types */
    VERTEX_LABEL(1, "VL"),
    EDGE_LABEL(2, "EL"),
    PROPERTY_KEY(3, "PK"),
    INDEX_LABEL(4, "IL"),

    /* Data types */
    VERTEX(101, "V"),
    // System meta
    SYS_PROPERTY(102, "S"),
    // Property
    PROPERTY(103, "U"),
    // Edge
    EDGE(120, "E"),
    // Edge's direction is OUT for the specified vertex
    EDGE_OUT(130, "O"),
    // Edge's direction is IN for the specified vertex
    EDGE_IN(140, "I"),

    SECONDARY_INDEX(150, "SI"),
    RANGE_INT_INDEX(160, "I4I"),
    RANGE_FLOAT_INDEX(161, "F4I"),
    RANGE_LONG_INDEX(162, "L8I"),
    RANGE_DOUBLE_INDEX(163, "D8I"),
    SEARCH_INDEX(170, "FI"),
    SHARD_INDEX(175, "PI"),

    TASK(180, "T"),

    // System schema
    SYS_SCHEMA(250, "SS"),

    MAX_TYPE(255, "~");

    private byte type = 0;
    private String name;

    private static final Map<String, HugeType> ALL_NAME = new HashMap<>();

    static {
        SerialEnum.register(HugeType.class);
        for (HugeType type : values()) {
            ALL_NAME.put(type.name, type);
        }
    }

    HugeType(int type, String name) {
        assert type < 256;
        this.type = (byte) type;
        this.name = name;
    }

    @Override
    public byte code() {
        return this.type;
    }

    public String string() {
        return this.name;
    }

    public String readableName() {
        return this.name().replace('_', ' ').toLowerCase();
    }

    public boolean isSchema() {
        return this == HugeType.VERTEX_LABEL ||
               this == HugeType.EDGE_LABEL ||
               this == HugeType.PROPERTY_KEY ||
               this == HugeType.INDEX_LABEL;
    }

    public boolean isGraph() {
        return this.isVertex() || this.isEdge() ;
    }

    public boolean isVertex() {
        return this == HugeType.VERTEX;
    }

    public boolean isEdge() {
        return this == EDGE || this == EDGE_OUT || this == EDGE_IN;
    }

    public boolean isIndex() {
        return this == HugeType.SECONDARY_INDEX ||
               this == HugeType.SEARCH_INDEX ||
               this == HugeType.RANGE_INT_INDEX ||
               this == HugeType.RANGE_FLOAT_INDEX ||
               this == HugeType.RANGE_LONG_INDEX ||
               this == HugeType.RANGE_DOUBLE_INDEX ||
               this == HugeType.SHARD_INDEX;
    }

    public boolean isStringIndex() {
        return this == HugeType.SECONDARY_INDEX ||
               this == HugeType.SEARCH_INDEX ||
               this == HugeType.SHARD_INDEX;
    }

    public boolean isNumericIndex() {
        return this == HugeType.RANGE_INT_INDEX ||
               this == HugeType.RANGE_FLOAT_INDEX ||
               this == HugeType.RANGE_LONG_INDEX ||
               this == HugeType.RANGE_DOUBLE_INDEX ||
               this == HugeType.SHARD_INDEX;
    }

    public boolean isSecondaryIndex() {
        return this == HugeType.SECONDARY_INDEX;
    }

    public boolean isSearchIndex() {
        return this == HugeType.SEARCH_INDEX;
    }

    public boolean isRangeIndex() {
        return this == HugeType.RANGE_INT_INDEX ||
               this == HugeType.RANGE_FLOAT_INDEX ||
               this == HugeType.RANGE_LONG_INDEX ||
               this == HugeType.RANGE_DOUBLE_INDEX;
    }

    public boolean isShardIndex() {
        return this == HugeType.SHARD_INDEX;
    }

    public static HugeType fromString(String type) {
        return ALL_NAME.get(type);
    }
}
