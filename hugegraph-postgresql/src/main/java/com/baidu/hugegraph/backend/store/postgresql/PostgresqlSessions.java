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

import java.sql.Connection;
import java.sql.SQLException;

import org.postgresql.util.PSQLException;
import org.slf4j.Logger;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.store.mysql.MysqlSessions;
import com.baidu.hugegraph.backend.store.mysql.MysqlStore;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.Log;

public class PostgresqlSessions extends MysqlSessions {

    private static final Logger LOG = Log.logger(MysqlStore.class);

    private static final String COCKROACH_DB_CREATE =
            "CREATE DATABASE %s ENCODING='UTF-8'";
    private static final String POSTGRESQL_DB_CREATE = COCKROACH_DB_CREATE +
            " TEMPLATE=template0 LC_COLLATE='C' LC_CTYPE='C';";

    public PostgresqlSessions(HugeConfig config, String database, String store) {
        super(config, database, store);
    }

    @Override
    public void createDatabase() {
        // Create database with non-database-session
        LOG.debug("Create database: {}", this.database());

        String sql = String.format(POSTGRESQL_DB_CREATE, this.database());
        try (Connection conn = this.openWithoutDB(0)) {
            try {
                conn.createStatement().execute(sql);
            } catch (PSQLException e) {
                // CockroackDB not support 'template' args of CREATE DATABASE
                if (e.getMessage().contains("syntax error at or near " +
                                            "\"template\"")) {
                    sql = String.format(COCKROACH_DB_CREATE, this.database());
                    conn.createStatement().execute(sql);
                }
            }
        } catch (SQLException e) {
            if (!e.getMessage().endsWith("already exists")) {
                throw new BackendException("Failed to create database '%s'", e,
                                           this.database());
            }
            // Ignore exception if database already exists
        }
    }
}
