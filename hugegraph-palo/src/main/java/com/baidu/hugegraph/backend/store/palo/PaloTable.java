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

package com.baidu.hugegraph.backend.store.palo;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.store.TableDefine;
import com.baidu.hugegraph.backend.store.mysql.MysqlBackendEntry;
import com.baidu.hugegraph.backend.store.mysql.MysqlSessions;
import com.baidu.hugegraph.backend.store.mysql.MysqlStore;
import com.baidu.hugegraph.backend.store.mysql.MysqlTable;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.Log;

public abstract class PaloTable extends MysqlTable {

    private static final Logger LOG = Log.logger(MysqlStore.class);

    public PaloTable(String table) {
        super(table);
    }

    @Override
    public void createTable(MysqlSessions.Session session,
                            TableDefine tableDefine) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS ");
        sql.append(this.table()).append(" (");
        // Add columns
        int i = 0;
        for (Map.Entry<HugeKeys, String> entry :
             tableDefine.columns().entrySet()) {
            sql.append(formatKey(entry.getKey()));
            sql.append(" ");
            sql.append(entry.getValue());
            if (++i != tableDefine.columns().size()) {
                sql.append(", ");
            }
        }
        sql.append(")");
        // Unique keys
        sql.append(" UNIQUE KEY(");
        i = 0;
        for (HugeKeys key : tableDefine.keys()) {
            sql.append(formatKey(key));
            if (++i != tableDefine.keys().size()) {
                sql.append(", ");
            }
        }
        sql.append(")");
        // Hash keys
        sql.append(" DISTRIBUTED BY HASH(");
        i = 0;
        for (HugeKeys key : tableDefine.keys()) {
            sql.append(formatKey(key));
            if (++i != tableDefine.keys().size()) {
                sql.append(", ");
            }
        }
        sql.append(");");
        // TODO: 'replication_num(default=3)’ can be a configuration

        LOG.debug("Create table: {}", sql);
        try {
            session.execute(sql.toString());
        } catch (SQLException e) {
            throw new BackendException("Failed to create table with '%s'",
                                       e, sql);
        }
    }

    @Override
    protected void appendPartition(StringBuilder delete) {
        delete.append(" PARTITION ").append(this.table());
    }

    @Override
    public void insert(MysqlSessions.Session session,
                       MysqlBackendEntry.Row entry) {
        assert session instanceof PaloSessions.Session;
        PaloSessions.Session paloSession = (PaloSessions.Session) session;

        Set<HugeKeys> columnNames = this.tableDefine().columnNames();
        // Ensure column order match with table define
        List<Object> columnValues = new ArrayList<>(columnNames.size());
        for (HugeKeys key : columnNames) {
            columnValues.add(entry.column(key));
        }
        String insert = StringUtils.join(columnValues, "\t");
        paloSession.add(this.table(), insert);
    }
}
