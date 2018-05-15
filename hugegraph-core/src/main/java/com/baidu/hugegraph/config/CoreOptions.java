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

    public static synchronized CoreOptions instance() {
        if (instance == null) {
            instance = new CoreOptions();
            // Should initialize all static members first, then register.
            instance.registerOptions();
        }
        return instance;
    }

    public static final ConfigOption<String> GREMLIN_GRAPH =
            new ConfigOption<>(
                    "gremlin.graph",
                    "Gremlin entrence to create graph.",
                    disallowEmpty(),
                    "com.baidu.hugegraph.HugeFactory"
            );

    public static final ConfigOption<String> BACKEND =
            new ConfigOption<>(
                    "backend",
                    "The data store type.",
                    disallowEmpty(),
                    "memory"
            );

    public static final ConfigOption<String> STORE =
            new ConfigOption<>(
                    "store",
                    "The database name like Cassandra Keyspace.",
                    disallowEmpty(),
                    "hugegraph"
            );

    public static final ConfigOption<String> STORE_SCHEMA =
            new ConfigOption<>(
                    "store.schema",
                    "The schema table name, which store meta data.",
                    disallowEmpty(),
                    "schema"
            );

    public static final ConfigOption<String> STORE_GRAPH =
            new ConfigOption<>(
                    "store.graph",
                    "The graph table name, which store vertex, edge and property.",
                    disallowEmpty(),
                    "graph"
            );

    public static final ConfigOption<String> SERIALIZER =
            new ConfigOption<>(
                    "serializer",
                    "The serializer for backend store, like: text/binary/cassandra.",
                    disallowEmpty(),
                    "text"
            );

    public static final ConfigOption<Integer> RATE_LIMIT =
            new ConfigOption<>(
                    "rate_limit",
                    "The max rate(items/s) to add/update/delete vertices/edges.",
                    rangeInt(0, Integer.MAX_VALUE),
                    0
            );

    public static final ConfigOption<String> VERTEX_DEFAULT_LABEL =
            new ConfigOption<>(
                    "vertex.default_label",
                    "The default vertex label.",
                    disallowEmpty(),
                    "vertex"
            );

    public static final ConfigOption<Integer> VERTEX_TX_CAPACITY =
            new ConfigOption<>(
                    "vertex.tx_capacity",
                    "The max size(items) of vertices(uncommitted) in transaction.",
                    rangeInt(1, 1000000),
                    10000
            );

    public static final ConfigOption<Integer> EDGE_TX_CAPACITY =
            new ConfigOption<>(
                    "edge.tx_capacity",
                    "The max size(items) of edges(uncommitted) in transaction.",
                    rangeInt(1, 1000000),
                    10000
            );

    /**
     * The schema name rule:
     * 1、Not allowed end with spaces
     * 2、Not allowed start with '~'
     */
    public static final ConfigOption<String> SCHEMA_ILLEGAL_NAME_REGEX =
            new ConfigOption<>(
                    "schema.illegal_name_regex",
                    "The regex specified the illegal format for schema name.",
                    disallowEmpty(),
                    ".*\\s+$|~.*"
            );

    public static final ConfigOption<Integer> SCHEMA_CACHE_CAPACITY =
            new ConfigOption<>(
                    "schema.cache_capacity",
                    "The max cache size(items) of schema data.",
                    rangeInt(1, Integer.MAX_VALUE),
                    (1024 * 1024 * 1)
            );

    public static final ConfigOption<Integer> GRAPH_CACHE_CAPACITY =
            new ConfigOption<>(
                    "graph.cache_capacity",
                    "The max cache size(items) of graph data(vertex/edge).",
                    rangeInt(1, Integer.MAX_VALUE),
                    (1024 * 1024 * 10)
            );

    public static final ConfigOption<Integer> GRAPH_CACHE_EXPIRE =
            new ConfigOption<>(
                    "graph.cache_expire",
                    "The expire time in seconds of graph data(vertex/edge).",
                    rangeInt(0, Integer.MAX_VALUE),
                    (60 * 10)
            );

    public static final ConfigOption<Long> SNOWFLAKE_WORKER_ID =
            new ConfigOption<>(
                    "snowflake.worker_id",
                    "The worker id of snowflake id generator.",
                    disallowEmpty(),
                    0L
            );

    public static final ConfigOption<Long> SNOWFLAKE_DATACENTER_ID =
            new ConfigOption<>(
                    "snowflake.datecenter_id",
                    "The datacenter id of snowflake id generator.",
                    disallowEmpty(),
                    0L
            );

    public static final ConfigOption<Boolean> SNOWFLAKE_FORCE_STRING =
            new ConfigOption<>(
                    "snowflake.force_string",
                    "Whether to force the snowflake long id to be a string.",
                    disallowEmpty(),
                    false
            );
}
