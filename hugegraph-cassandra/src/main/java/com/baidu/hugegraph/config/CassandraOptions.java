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

public class CassandraOptions extends OptionHolder {

    private CassandraOptions() {
        super();
    }

    private static volatile CassandraOptions instance;

    public static CassandraOptions Instance() {
        if (instance == null) {
            synchronized (CassandraOptions.class) {
                if (instance == null) {
                    instance = new CassandraOptions();
                    instance.registerOptions();
                }
            }
        }
        return instance;
    }

    public static final ConfigOption<String> CASSANDRA_HOST = new ConfigOption<>(
            "cassandra.host",
            "localhost",
            true,
            "The seeds hostname or ip address of cassandra cluster.",
            disallowEmpty(String.class)
    );

    public static final ConfigOption<Integer> CASSANDRA_PORT = new ConfigOption<>(
            "cassandra.port",
            9042,
            true,
            "The seeds port address of cassandra cluster.",
            rangeInt(1024, 10000)
    );

    public static final ConfigOption<Integer> CASSANDRA_CONN_TIMEOUT =
            new ConfigOption<>(
                "cassandra.connect_time_out",
                5,
                true,
                "The cassandra driver connect server time out(seconds).",
                rangeInt(1, 30)
    );

    public static final ConfigOption<Integer> CASSANDRA_READ_TIMEOUT =
            new ConfigOption<>(
                "cassandra.read_time_out",
                20,
                true,
                "The cassandra driver read from server time out(seconds).",
                rangeInt(1, 120)
            );

    public static final ConfigOption<String> CASSANDRA_STRATEGY = new ConfigOption<>(
            "cassandra.keyspace.strategy",
            "SimpleStrategy",
            true,
            "The keyspace strategy",
            disallowEmpty(String.class)
    );

    public static final ConfigOption<Integer> CASSANDRA_REPLICATION = new ConfigOption<>(
            "cassandra.keyspace.replication",
            3,
            true,
            "The keyspace replication factor",
            rangeInt(1, 100)
    );

}
