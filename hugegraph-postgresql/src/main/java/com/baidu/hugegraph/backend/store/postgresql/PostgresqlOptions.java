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

package com.baidu.hugegraph.backend.store.postgresql;

import com.baidu.hugegraph.backend.store.mysql.MysqlOptions;
import com.baidu.hugegraph.config.ConfigOption;

import static com.baidu.hugegraph.config.OptionChecker.disallowEmpty;

public class PostgresqlOptions extends MysqlOptions {

    private PostgresqlOptions() {
        super();
    }

    private static volatile PostgresqlOptions instance;

    public static synchronized PostgresqlOptions instance() {
        if (instance == null) {
            instance = new PostgresqlOptions();
            instance.registerOptions();
        }
        return instance;
    }

    public static final ConfigOption<String> JDBC_DRIVER =
            new ConfigOption<>(
                    "jdbc.driver",
                    "The JDBC driver class to connect database.",
                    disallowEmpty(),
                    "org.postgresql.Driver"
            );

    public static final ConfigOption<String> JDBC_URL =
            new ConfigOption<>(
                    "jdbc.url",
                    "The url of database in JDBC format.",
                    disallowEmpty(),
                    "jdbc:postgresql://127.0.0.1:5432/"
            );

    public static final ConfigOption<String> JDBC_USERNAME =
            new ConfigOption<>(
                    "jdbc.username",
                    "The username to login database.",
                    disallowEmpty(),
                    "postgres"
            );
}
