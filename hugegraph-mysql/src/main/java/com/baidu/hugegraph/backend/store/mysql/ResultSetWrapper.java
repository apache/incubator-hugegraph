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

import com.baidu.hugegraph.backend.BackendException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ResultSetWrapper implements AutoCloseable {

    private final ResultSet resultSet;
    private final Statement statement;

    public ResultSetWrapper(ResultSet resultSet, Statement statement) {
        this.resultSet = resultSet;
        this.statement = statement;
    }

    public boolean next() throws SQLException {
        return !this.resultSet.isClosed() && this.resultSet.next();
    }

    public void close() {
        try {
            if (this.resultSet != null) {
                this.resultSet.close();
            }
        } catch (SQLException e) {
            throw new BackendException("Failed to close ResultSet", e);
        } finally {
            try {
                if (this.statement != null) {
                    this.statement.close();
                }
            } catch (SQLException e) {
                throw new BackendException("Failed to close Statement", e);
            }
        }
    }

    public ResultSet resultSet() {
        return resultSet;
    }
}
