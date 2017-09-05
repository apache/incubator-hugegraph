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
    ID_STRATEGY(50, "id_strategy"),
    PROPERTIES(51, "properties"),
    PRIMARY_KEYS(52, "primary_keys"),
    INDEX_NAMES(53, "index_names"),
    NULLABLE_KEYS(54, "nullable_keys"),

    /* Column names of schema type (EdgeLabel) */
    LINKS(80, "links"),
    FREQUENCY(81, "frequency"),
    SOURCE_LABEL(82, "source_label"),
    TARGET_LABEL(83, "target_label"),
    SORT_KEYS(84, "sort_keys"),

    /* Column names of schema type (PropertyKey) */
    DATA_TYPE(120, "data_type"),
    CARDINALITY(121, "cardinality"),

    /* Column names of schema type (IndexLabel) */
    BASE_TYPE(150, "base_type"),
    BASE_VALUE(151, "base_value"),
    INDEX_TYPE(152, "index_type"),
    FIELDS(153, "fields"),

    /* Column names of index data */
    INDEX_NAME(180, "index_name"),
    FIELD_VALUES(181, "field_values"),
    INDEX_LABEL_NAME(182, "index_label_name"),
    ELEMENT_IDS(183, "element_ids"),

    /* Column names of data type (Vertex/Edge) */
    LABEL(200, "label"),
    OWNER_VERTEX(201, "owner_vertex"),
    OTHER_VERTEX(202, "other_vertex"),
    PROPERTY_KEY(203, "property_key"),
    PROPERTY_VALUE(204, "property_value"),
    DIRECTION(205, "direction"),
    SORT_VALUES(206, "sort_values"),
    PRIMARY_VALUES(207, "primary_values");

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
