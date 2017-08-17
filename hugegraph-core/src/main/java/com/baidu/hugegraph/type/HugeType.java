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

public enum HugeType {

    UNKNOWN(0),

    /* Schema types */
    VERTEX_LABEL(1),
    EDGE_LABEL(2),
    PROPERTY_KEY(3),
    INDEX_LABEL(4),

    /* Data types */
    VERTEX(101),
    // System meta
    SYS_PROPERTY(102),
    // Property
    PROPERTY(103),
    EDGE(120),
    // Edge's direction is OUT for the specified vertex
    EDGE_OUT(120),
    // Edge's direction is IN for the specified vertex
    EDGE_IN(121),

    SECONDARY_INDEX(150),
    SEARCH_INDEX(151),

    MAX_TYPE(255);

    // HugeType define
    private byte type = 0;

    private HugeType(int type) {
        assert type < 256;
        this.type = (byte) type;
    }

    public byte code() {
        return this.type;
    }
}
