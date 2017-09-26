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

package com.baidu.hugegraph.config;

import static com.baidu.hugegraph.config.OptionChecker.disallowEmpty;
import static com.baidu.hugegraph.config.OptionChecker.rangeInt;


public class CoreOptions extends OptionHolder {

    private CoreOptions() {
        super();
    }

    private static volatile CoreOptions instance;

    public static CoreOptions Instance() {
        if (instance == null) {
            synchronized (CoreOptions.class) {
                if (instance == null) {
                    instance = new CoreOptions();
                    instance.registerOptions();
                }
            }
        }
        return instance;
    }

    // TODO: could move to dist package
    public static final ConfigOption<String> BACKENDS = new ConfigOption<>(
            "backends",
            "[]",
            true,
            "The all data store type.",
            disallowEmpty(String.class));

    public static final ConfigOption<String> BACKEND = new ConfigOption<>(
            "backend",
            "memory",
            true,
            "The data store type.",
            disallowEmpty(String.class));

    public static final ConfigOption<String> STORE = new ConfigOption<>(
            "store",
            "hugegraph",
            true,
            "The database name like Cassandra Keyspace.",
            disallowEmpty(String.class));

    public static final ConfigOption<String> STORE_SCHEMA = new ConfigOption<>(
            "store.schema",
            "huge_schema",
            true,
            "The schema table name, which store meta data.",
            disallowEmpty(String.class));

    public static final ConfigOption<String> STORE_GRAPH = new ConfigOption<>(
            "store.graph",
            "huge_graph",
            true,
            "The graph table name, which store vertex, edge and property.",
            disallowEmpty(String.class));

    public static final ConfigOption<String> STORE_INDEX = new ConfigOption<>(
            "store.index",
            "huge_index",
            true,
            "The index table name, which store index data of vertex, edge.",
            disallowEmpty(String.class));

    public static final ConfigOption<String> SERIALIZER = new ConfigOption<>(
            "serializer",
            "text",
            true,
            "The serializer for backend store, like: text/binary/cassandra",
            disallowEmpty(String.class));

    public static final ConfigOption<String> DEFAULT_VERTEX_LABEL =  new ConfigOption<>(
            "vertex.default_label",
            "vertex",
            true,
            "The default vertex label.",
            disallowEmpty(String.class));

    public static final ConfigOption<String> GRAPHS = new ConfigOption<>(
            "graphs",
            "hugegraph:conf/hugegraph.properties",
            true,
            "The map of graphs' name and config file.",
            disallowEmpty(String.class));

    public static final ConfigOption<Integer> SCHEMA_CACHE_CAPACITY = new ConfigOption<>(
            "schema.cache_capacity",
            (1024 * 1024 * 1),
            true,
            "The max cache size(items) of schema data.",
            rangeInt(1, Integer.MAX_VALUE));

    public static final ConfigOption<Integer> SCHEMA_CACHE_EXPIRE = new ConfigOption<>(
            "schema.cache_expire",
            (60 * 30),
            true,
            "The expire time in seconds of schema data.",
            rangeInt(0, Integer.MAX_VALUE));

    public static final ConfigOption<Integer> GRAPH_CACHE_CAPACITY = new ConfigOption<>(
            "graph.cache_capacity",
            (1024 * 1024 * 10),
            true,
            "The max cache size(items) of graph data(vertex/edge).",
            rangeInt(1, Integer.MAX_VALUE));

    public static final ConfigOption<Integer> GRAPH_CACHE_EXPIRE = new ConfigOption<>(
            "graph.cache_expire",
            (60 * 10),
            true,
            "The expire time in seconds of graph data(vertex/edge).",
            rangeInt(0, Integer.MAX_VALUE));

    public static final ConfigOption<String> SCHEMA_ILLEGAL_NAME_REGEX = new ConfigOption<>(
            "schema.illegal_name_regex",
            "\\s+|~.*",
            true,
            "The regex expression that specified the illegal format for " +
            "schema name",
            disallowEmpty(String.class));
}
