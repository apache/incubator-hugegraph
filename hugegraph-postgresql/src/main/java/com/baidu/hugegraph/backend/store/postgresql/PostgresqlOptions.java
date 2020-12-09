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

import static com.baidu.hugegraph.config.OptionChecker.disallowEmpty;

import com.baidu.hugegraph.backend.store.mysql.MysqlOptions;
import com.baidu.hugegraph.config.ConfigOption;

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

    public static final ConfigOption<String> POSTGRESQL_CONNECT_DATABASE =
            new ConfigOption<>(
                    "postgresql.connect_database",
                    "The database used to connect when not specify database.",
                    disallowEmpty(),
                    "template1"
            );
}
