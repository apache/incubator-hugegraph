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

package com.baidu.hugegraph.backend.store.cassandra;

import static com.baidu.hugegraph.config.OptionChecker.allowValues;
import static com.baidu.hugegraph.config.OptionChecker.disallowEmpty;
import static com.baidu.hugegraph.config.OptionChecker.rangeInt;

import com.baidu.hugegraph.config.ConfigOption;
import com.baidu.hugegraph.config.OptionHolder;

public class CassandraOptions extends OptionHolder {

    private CassandraOptions() {
        super();
    }

    private static volatile CassandraOptions instance;

    public static synchronized CassandraOptions instance() {
        if (instance == null) {
            instance = new CassandraOptions();
            instance.registerOptions();
        }
        return instance;
    }

    public static final ConfigOption<String> CASSANDRA_HOST =
            new ConfigOption<>(
                    "cassandra.host",
                    "The seeds hostname or ip address of cassandra cluster.",
                    disallowEmpty(),
                    "localhost"
            );

    public static final ConfigOption<Integer> CASSANDRA_PORT =
            new ConfigOption<>(
                    "cassandra.port",
                    "The seeds port address of cassandra cluster.",
                    rangeInt(1024, 10000),
                    9042
            );

    public static final ConfigOption<String> CASSANDRA_USERNAME =
            new ConfigOption<>(
                    "cassandra.username",
                    "The username to use to login to cassandra cluster.",
                    ""
            );

    public static final ConfigOption<String> CASSANDRA_PASSWORD =
            new ConfigOption<>(
                    "cassandra.password",
                    "The password corresponding to cassandra.username.",
                    ""
            );

    public static final ConfigOption<Integer> CASSANDRA_CONN_TIMEOUT =
            new ConfigOption<>(
                    "cassandra.connect_timeout",
                    "The cassandra driver connect server timeout(seconds).",
                    rangeInt(1, 30),
                    5
            );

    public static final ConfigOption<Integer> CASSANDRA_READ_TIMEOUT =
            new ConfigOption<>(
                    "cassandra.read_timeout",
                    "The cassandra driver read from server timeout(seconds).",
                    rangeInt(1, 120),
                    20
            );

    public static final ConfigOption<String> CASSANDRA_STRATEGY =
            new ConfigOption<>(
                    "cassandra.keyspace.strategy",
                    "The keyspace strategy.",
                    disallowEmpty(),
                    "SimpleStrategy"
            );

    public static final ConfigOption<Integer> CASSANDRA_REPLICATION =
            new ConfigOption<>(
                    "cassandra.keyspace.replication",
                    "The keyspace replication factor.",
                    rangeInt(1, 100),
                    3
            );

    public static final ConfigOption<String> CASSANDRA_COMPRESSION =
            new ConfigOption<>(
                    "cassandra.compression_type",
                    "The compression algorithm of cassandra transport: none/snappy/lz4.",
                    allowValues("none", "snappy", "lz4"),
                    "none"
            );

    public static final ConfigOption<Integer> CASSANDRA_JMX_PORT =
            new ConfigOption<>(
                    "cassandra.jmx_port",
                    "The port of JMX API service for cassandra",
                    rangeInt(1, 65535),
                    7199
            );
}
