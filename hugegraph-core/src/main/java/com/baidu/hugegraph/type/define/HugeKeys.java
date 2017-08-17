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

public enum HugeKeys {

    UNKNOWN(0, "undefined"),

    /* Column names of schema type (common) */
    ID(1, "id"),
    NAME(2, "name"),
    TIMESTANMP(3, "timestamp"),

    /* Column names of schema type (VertexLabel) */
    ID_STRATEGY(50, "idStrategy"),
    PROPERTIES(51, "properties"),
    PRIMARY_KEYS(52, "primaryKeys"),
    INDEX_NAMES(53, "indexNames"),

    /* Column names of schema type (EdgeLabel) */
    LINKS(80, "links"),
    FREQUENCY(81, "frequency"),
    SOURCE_LABEL(82, "sourceLabel"),
    TARGET_LABEL(83, "targetLabel"),
    SORT_KEYS(84, "sortKeys"),

    /* Column names of schema type (PropertyKey) */
    DATA_TYPE(120, "dataType"),
    CARDINALITY(121, "cardinality"),

    /* Column names of schema type (IndexLabel) */
    BASE_TYPE(150, "baseType"),
    BASE_VALUE(151, "baseValue"),
    INDEX_TYPE(152, "indexType"),
    FIELDS(153, "fields"),

    /* Column names of index data */
    INDEX_NAME(180, "indexName"),
    FIELD_VALUES(181, "fieldValues"),
    INDEX_LABEL_NAME(182, "indexLabelName"),
    ELEMENT_IDS(183, "elementIds"),

    /* Column names of data type (Vertex/Edge) */
    LABEL(200, "label"),
    SOURCE_VERTEX(201, "sourceVertex"),
    TARGET_VERTEX(202, "targetVertex"),
    PROPERTY_KEY(203, "propertyKey"),
    PROPERTY_VALUE(204, "propertyValue"),
    DIRECTION(205, "direction"),
    SORT_VALUES(206, "sortValues"),
    PRIMARY_VALUES(207, "primaryValues");

    /* HugeKeys define */
    private byte code = 0;
    private String name = null;

    private HugeKeys(int code, String name) {
        assert code < 256;
        this.code = (byte) code;
        this.name = name;
    }

    public byte code() {
        return this.code;
    }

    public String string() {
        return this.name;
    }
}
