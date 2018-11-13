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

    public static final ConfigOption<String> STORE_SYSTEM =
            new ConfigOption<>(
                    "store.system",
                    "The system table name, which store system data.",
                    disallowEmpty(),
                    "system"
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

    public static final ConfigOption<Boolean> VERTEX_CHECK_CUSTOMIZED_ID_EXIST =
            new ConfigOption<>(
                    "vertex.check_customzied_id_exist",
                    "Whether to check the vertices exist for those using " +
                    "customized id strategy",
                    disallowEmpty(),
                    true
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
                    "The max cache size(items) of schema cache.",
                    rangeInt(1, Integer.MAX_VALUE),
                    100000
            );

    public static final ConfigOption<Integer> VERTEX_CACHE_CAPACITY =
            new ConfigOption<>(
                    "vertex.cache_capacity",
                    "The max cache size(items) of vertex cache.",
                    rangeInt(1, Integer.MAX_VALUE),
                    (1000 * 1000 * 10)
            );

    public static final ConfigOption<Integer> VERTEX_CACHE_EXPIRE =
            new ConfigOption<>(
                    "vertex.cache_expire",
                    "The expire time in seconds of vertex cache.",
                    rangeInt(0, Integer.MAX_VALUE),
                    (60 * 10)
            );

    public static final ConfigOption<Integer> EDGE_CACHE_CAPACITY =
            new ConfigOption<>(
                    "edge.cache_capacity",
                    "The max cache size(items) of edge cache.",
                    rangeInt(1, Integer.MAX_VALUE),
                    (1000 * 1000 * 1)
            );

    public static final ConfigOption<Integer> EDGE_CACHE_EXPIRE =
            new ConfigOption<>(
                    "edge.cache_expire",
                    "The expire time in seconds of edge cache.",
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

    public static final ConfigOption<String> TEXT_ANALYZER =
            new ConfigOption<>(
                    "search.text_analyzer",
                    "Choose a text analyzer for searching the " +
                    "vertex/edge properties, available type are " +
                    "[word, ansj, hanlp, smartcn, jieba, jcseg, " +
                    "mmseg4j, ikanalyzer]",
                    disallowEmpty(),
                    "ikanalyzer"
            );

    public static final ConfigOption<String> TEXT_ANALYZER_MODE =
            new ConfigOption<>(
                    "search.text_analyzer_mode",
                    "Specify the mode for the text analyzer, " +
                    "the available mode of analyzer are " +
                    "{word: [MaximumMatching, ReverseMaximumMatching, " +
                            "MinimumMatching, ReverseMinimumMatching, " +
                            "BidirectionalMaximumMatching, " +
                            "BidirectionalMinimumMatching, " +
                            "BidirectionalMaximumMinimumMatching, " +
                            "FullSegmentation, MinimalWordCount, " +
                            "MaxNgramScore, PureEnglish], " +
                    "ansj: [BaseAnalysis, IndexAnalysis, ToAnalysis, " +
                           "NlpAnalysis], " +
                    "hanlp: [standard, nlp, index, nShort, shortest, speed], " +
                    "smartcn: [], " +
                    "jieba: [SEARCH, INDEX], " +
                    "jcseg: [Simple, Complex], " +
                    "mmseg4j: [Simple, Complex, MaxWord], " +
                    "ikanalyzer: [smart, max_word]" +
                    "}",
                    disallowEmpty(),
                    "smart"
            );
    public static final ConfigOption<Boolean> SCHEMA_SYNC_DELETION =
            new ConfigOption<>(
                    "schema.sync_deletion",
                    "Whether to delete schema synchronously.",
                    disallowEmpty(),
                    false
            );
}
