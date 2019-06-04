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

package com.baidu.hugegraph.backend.store.mysql;

import static com.baidu.hugegraph.config.OptionChecker.disallowEmpty;
import static com.baidu.hugegraph.config.OptionChecker.rangeInt;

import com.baidu.hugegraph.config.ConfigOption;
import com.baidu.hugegraph.config.OptionHolder;

public class MysqlOptions extends OptionHolder {

    protected MysqlOptions() {
        super();
    }

    private static volatile MysqlOptions instance;

    public static synchronized MysqlOptions instance() {
        if (instance == null) {
            instance = new MysqlOptions();
            instance.registerOptions();
        }
        return instance;
    }

    public static final ConfigOption<String> JDBC_DRIVER =
            new ConfigOption<>(
                    "jdbc.driver",
                    "The JDBC driver class to connect database.",
                    disallowEmpty(),
                    "com.mysql.jdbc.Driver"
            );

    public static final ConfigOption<String> JDBC_URL =
            new ConfigOption<>(
                    "jdbc.url",
                    "The url of database in JDBC format.",
                    disallowEmpty(),
                    "jdbc:mysql://127.0.0.1:3306"
            );

    public static final ConfigOption<String> JDBC_USERNAME =
            new ConfigOption<>(
                    "jdbc.username",
                    "The username to login database.",
                    disallowEmpty(),
                    "root"
            );

    public static final ConfigOption<String> JDBC_PASSWORD =
            new ConfigOption<>(
                    "jdbc.password",
                    "The password corresponding to jdbc.username.",
                    null,
                    ""
            );

    public static final ConfigOption<Integer> JDBC_RECONNECT_MAX_TIMES =
            new ConfigOption<>(
                    "jdbc.reconnect_max_times",
                    "The reconnect times when the database connection fails.",
                    rangeInt(1, 10),
                    3
            );

    public static final ConfigOption<Integer> JDBC_RECONNECT_INTERVAL =
            new ConfigOption<>(
                    "jdbc.reconnect_interval",
                    "The interval(seconds) between reconnections when the " +
                    "database connection fails.",
                    rangeInt(1, 10),
                    3
            );

    public static final ConfigOption<String> SSL_MODE =
            new ConfigOption<>(
                    "jdbc.ssl_mode",
                    "The SSL mode of connections with database.",
                    disallowEmpty(),
                    "disable"
            );

    public static final ConfigOption<String> STORAGE_ENGINE =
            new ConfigOption<>(
                    "mysql.engine",
                    "the storage engine to store graph data/schema, innodb or rocksdb.",
                    disallowEmpty(),
                    "InnoDB"
            );
}
