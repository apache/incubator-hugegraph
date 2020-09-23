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

public enum GraphMode {

    /*
     * None mode is regular mode
     * 1. Not allowed create schema with specified id
     * 2. Not support create vertex with id for AUTOMATIC id strategy
     */
    NONE(1, "none"),

    /*
     * Restoring mode is used to restore schema and graph data to an new graph.
     * 1. Support create schema with specified id
     * 2. Support create vertex with id for AUTOMATIC id strategy
     */
    RESTORING(2, "restoring"),

    /*
     * MERGING mode is used to merge schema and graph data to an existing graph.
     * 1. Not allowed create schema with specified id
     * 2. Support create vertex with id for AUTOMATIC id strategy
     */
    MERGING(3, "merging"),

    /*
     * LOADING mode used to load data via hugegraph-loader.
     */
    LOADING(4, "loading");

    private final byte code;
    private final String name;

    private GraphMode(int code, String name) {
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

    public boolean maintaining() {
        return this == RESTORING || this == MERGING;
    }

    public boolean loading() {
        return this == LOADING;
    }
}
