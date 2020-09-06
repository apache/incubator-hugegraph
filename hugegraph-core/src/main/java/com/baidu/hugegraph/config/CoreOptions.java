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

import static com.baidu.hugegraph.backend.tx.GraphTransaction.COMMIT_BATCH;
import static com.baidu.hugegraph.config.OptionChecker.allowValues;
import static com.baidu.hugegraph.config.OptionChecker.disallowEmpty;
import static com.baidu.hugegraph.config.OptionChecker.positiveInt;
import static com.baidu.hugegraph.config.OptionChecker.rangeInt;

import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.util.Bytes;

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
                    "s"
            );

    public static final ConfigOption<String> STORE_SCHEMA =
            new ConfigOption<>(
                    "store.schema",
                    "The schema table name, which store meta data.",
                    disallowEmpty(),
                    "m"
            );

    public static final ConfigOption<String> STORE_GRAPH =
            new ConfigOption<>(
                    "store.graph",
                    "The graph table name, which store vertex, edge and property.",
                    disallowEmpty(),
                    "g"
            );

    public static final ConfigOption<String> SERIALIZER =
            new ConfigOption<>(
                    "serializer",
                    "The serializer for backend store, like: text/binary/cassandra.",
                    disallowEmpty(),
                    "text"
            );

    public static final ConfigOption<Boolean> RAFT_MODE =
            new ConfigOption<>(
                    "raft.mode",
                    "Whether the backend storage works in raft mode.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Boolean> RAFT_SAFE_READ =
            new ConfigOption<>(
                    "raft.safe_read",
                    "Whether to use linearly consistent read.",
                    disallowEmpty(),
                    true
            );

    public static final ConfigOption<Boolean> RAFT_USE_SNAPSHOT =
            new ConfigOption<>(
                    "raft.use_snapshot",
                    "Whether to use snapshot.",
                    disallowEmpty(),
                    true
            );

    public static final ConfigOption<Integer> RAFT_QUEUE_SIZE =
            new ConfigOption<>(
                    "raft.queue_size",
                    "The disruptor buffers size for jraft RaftNode, " +
                    "StateMachine and LogManager",
                    positiveInt(),
                    8192
            );

    public static final ConfigOption<String> RAFT_ENDPOINT =
            new ConfigOption<>(
                    "raft.endpoint",
                    "The peerid of current raft node.",
                    disallowEmpty(),
                    "127.0.0.1:8281"
            );

    public static final ConfigOption<String> RAFT_GROUP_PEERS =
            new ConfigOption<>(
                    "raft.group_peers",
                    "The peers of current raft group.",
                    disallowEmpty(),
                    "127.0.0.1:8281,127.0.0.1:8282,127.0.0.1:8283"
            );

    public static final ConfigOption<String> RAFT_PATH =
            new ConfigOption<>(
                    "raft.path",
                    "The log path of current raft node.",
                    disallowEmpty(),
                    "./raftlog"
            );

    public static final ConfigOption<Integer> RAFT_ELECTION_TIMEOUT =
            new ConfigOption<>(
                    "raft.election_timeout",
                    "Timeout in milliseconds to launch a round of election.",
                    rangeInt(0, Integer.MAX_VALUE),
                    3000
            );

    public static final ConfigOption<Integer> RAFT_SNAPSHOT_INTERVAL =
            new ConfigOption<>(
                    "raft.snapshot_interval",
                    "The interval in seconds to trigger snapshot save.",
                    rangeInt(0, Integer.MAX_VALUE),
                    3600
            );

    public static final ConfigOption<Integer> RAFT_BACKEND_THREADS =
            new ConfigOption<>(
                    "raft.backend_threads",
                    "The thread number used to apply task to bakcend.",
                    rangeInt(0, Integer.MAX_VALUE),
                    8
            );

    public static final ConfigOption<Integer> RATE_LIMIT_WRITE =
            new ConfigOption<>(
                    "rate_limit.write",
                    "The max rate(items/s) to add/update/delete vertices/edges.",
                    rangeInt(0, Integer.MAX_VALUE),
                    0
            );

    public static final ConfigOption<Integer> RATE_LIMIT_READ =
            new ConfigOption<>(
                    "rate_limit.read",
                    "The max rate(times/s) to execute query of vertices/edges.",
                    rangeInt(0, Integer.MAX_VALUE),
                    0
            );

    public static final ConfigOption<Long> TASK_WAIT_TIMEOUT =
            new ConfigOption<>(
                    "task.wait_timeout",
                    "Timeout in seconds for waiting for the task to complete," +
                    "such as when truncating or clearing the backend.",
                    rangeInt(0L, Long.MAX_VALUE),
                    10L
            );

    public static final ConfigOption<Long> TASK_INPUT_SIZE_LIMIT =
            new ConfigOption<>(
                    "task.input_size_limit",
                    "The job input size limit in bytes.",
                    rangeInt(0L, Bytes.GB),
                    16 * Bytes.MB
            );

    public static final ConfigOption<Long> TASK_RESULT_SIZE_LIMIT =
            new ConfigOption<>(
                    "task.result_size_limit",
                    "The job result size limit in bytes.",
                    rangeInt(0L, Bytes.GB),
                    16 * Bytes.MB
            );

    public static final ConfigOption<Long> CONNECTION_DETECT_INTERVAL =
            new ConfigOption<>(
                    "store.connection_detect_interval",
                    "The interval in seconds for detecting connections, " +
                    "if the idle time of a connection exceeds this value, " +
                    "detect it and reconnect if needed before using, " +
                    "value 0 means detecting every time.",
                    rangeInt(0L, Long.MAX_VALUE),
                    600L
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
                    "vertex.check_customized_id_exist",
                    "Whether to check the vertices exist for those using " +
                    "customized id strategy",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Boolean> VERTEX_ADJACENT_VERTEX_EXIST =
            new ConfigOption<>(
                    "vertex.check_adjacent_vertex_exist",
                    "Whether to check the adjacent vertices of edges exist",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Boolean> VERTEX_ADJACENT_VERTEX_LAZY =
            new ConfigOption<>(
                    "vertex.lazy_load_adjacent_vertex",
                    "Whether to lazy load adjacent vertices of edges",
                    disallowEmpty(),
                    true
            );

    public static final ConfigOption<Integer> VERTEX_PART_EDGE_COMMIT_SIZE =
            new ConfigOption<>(
                    "vertex.part_edge_commit_size",
                    "Whether to enable the mode to commit part of edges of vertex, " +
                    "enabled if commit size > 0, 0 meas disabled.",
                    rangeInt(0, (int) Query.DEFAULT_CAPACITY),
                    5000
            );

    public static final ConfigOption<Boolean> QUERY_IGNORE_INVALID_DATA =
            new ConfigOption<>(
                    "query.ignore_invalid_data",
                    "Whether to ignore invalid data of vertex or edge.",
                    disallowEmpty(),
                    true
            );

    public static final ConfigOption<Integer> QUERY_BATCH_SIZE =
            new ConfigOption<>(
                    "query.batch_size",
                    "The size of each batch when querying by batch.",
                    rangeInt(1, (int) Query.DEFAULT_CAPACITY),
                    1000
            );

    public static final ConfigOption<Integer> QUERY_PAGE_SIZE =
            new ConfigOption<>(
                    "query.page_size",
                    "The size of each page when querying by paging.",
                    rangeInt(1, (int) Query.DEFAULT_CAPACITY),
                    500
            );

    public static final ConfigOption<Integer> QUERY_INDEX_INTERSECT_THRESHOLD =
            new ConfigOption<>(
                    "query.index_intersect_threshold",
                    "The maximum number of intermediate results to intersect " +
                    "indexes when querying by multiple single index properties.",
                    rangeInt(1, (int) Query.DEFAULT_CAPACITY),
                    1000
            );

    public static final ConfigOption<Boolean> QUERY_RAMTABLE_ENABLE =
            new ConfigOption<>(
                    "query.ramtable_enable",
                    "Whether to enable ramtable for query of adjacent edges.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<Long> QUERY_RAMTABLE_VERTICES_CAPACITY =
            new ConfigOption<>(
                    "query.ramtable_vertices_capacity",
                    "The maximum number of vertices in ramtable, " +
                    "generally the largest vertex id is used as capacity.",
                    rangeInt(1L, Integer.MAX_VALUE * 2L),
                    10000000L
            );

    public static final ConfigOption<Integer> QUERY_RAMTABLE_EDGES_CAPACITY =
            new ConfigOption<>(
                    "query.ramtable_edges_capacity",
                    "The maximum number of edges in ramtable, " +
                    "include OUT and IN edges.",
                    rangeInt(1, Integer.MAX_VALUE),
                    20000000
            );

    public static final ConfigOption<Integer> VERTEX_TX_CAPACITY =
            new ConfigOption<>(
                    "vertex.tx_capacity",
                    "The max size(items) of vertices(uncommitted) in transaction.",
                    rangeInt(COMMIT_BATCH, 1000000),
                    10000
            );

    public static final ConfigOption<Integer> EDGE_TX_CAPACITY =
            new ConfigOption<>(
                    "edge.tx_capacity",
                    "The max size(items) of edges(uncommitted) in transaction.",
                    rangeInt(COMMIT_BATCH, 1000000),
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

    public static final ConfigOption<Long> SCHEMA_CACHE_CAPACITY =
            new ConfigOption<>(
                    "schema.cache_capacity",
                    "The max cache size(items) of schema cache.",
                    rangeInt(0L, Long.MAX_VALUE),
                    10000L
            );

    public static final ConfigOption<Boolean> TASK_SYNC_DELETION =
            new ConfigOption<>(
                    "task.sync_deletion",
                    "Whether to delete schema or expired data synchronously.",
                    disallowEmpty(),
                    false
            );

    public static final ConfigOption<String> VERTEX_CACHE_TYPE =
            new ConfigOption<>(
                    "vertex.cache_type",
                    "The type of vertex cache, allowed values are [l1, l2].",
                    allowValues("l1", "l2"),
                    "l1"
            );

    public static final ConfigOption<Integer> EXPIRED_DELETE_BATCH =
            new ConfigOption<>(
                    "expired.delete_batch",
                    "The batch size used to delete expired data.",
                    rangeInt(1, 500),
                    1
            );

    public static final ConfigOption<Long> VERTEX_CACHE_CAPACITY =
            new ConfigOption<>(
                    "vertex.cache_capacity",
                    "The max cache size(items) of vertex cache.",
                    rangeInt(0L, Long.MAX_VALUE),
                    (1000 * 1000 * 10L)
            );

    public static final ConfigOption<Integer> VERTEX_CACHE_EXPIRE =
            new ConfigOption<>(
                    "vertex.cache_expire",
                    "The expire time in seconds of vertex cache.",
                    rangeInt(0, Integer.MAX_VALUE),
                    (60 * 10)
            );

    public static final ConfigOption<String> EDGE_CACHE_TYPE =
            new ConfigOption<>(
                    "edge.cache_type",
                    "The type of edge cache, allowed values are [l1, l2].",
                    allowValues("l1", "l2"),
                    "l1"
            );

    public static final ConfigOption<Long> EDGE_CACHE_CAPACITY =
            new ConfigOption<>(
                    "edge.cache_capacity",
                    "The max cache size(items) of edge cache.",
                    rangeInt(0L, Long.MAX_VALUE),
                    (1000 * 1000 * 1L)
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

    public static final ConfigOption<String> COMPUTER_CONFIG =
            new ConfigOption<>(
                    "computer.config",
                    "The config file path of computer job",
                    disallowEmpty(),
                    "./conf/computer.yaml"
            );
}
