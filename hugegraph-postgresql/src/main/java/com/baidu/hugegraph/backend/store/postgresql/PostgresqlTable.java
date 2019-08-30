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

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.util.Strings;

import com.baidu.hugegraph.backend.store.mysql.MysqlBackendEntry;
import com.baidu.hugegraph.backend.store.mysql.MysqlTable;
import com.baidu.hugegraph.backend.store.mysql.MysqlSessions.Session;
import com.baidu.hugegraph.type.define.HugeKeys;

public abstract class PostgresqlTable extends MysqlTable {

    private String insertTemplate = null;
    private String orderByKeys = null;

    public PostgresqlTable(String table) {
        super(table);
    }

    @Override
    protected String buildDropTemplate() {
        return String.format("DROP TABLE IF EXISTS %s CASCADE;", this.table());
    }

    @Override
    protected String buildTruncateTemplate() {
        return String.format("TRUNCATE TABLE %s CASCADE;", this.table());
    }

    @Override
    protected String engine(Session session) {
        return Strings.EMPTY;
    }

    @Override
    protected List<Object> buildInsertObjects(MysqlBackendEntry.Row entry) {
        List<Object> objects = new ArrayList<>();
        objects.addAll(super.buildInsertObjects(entry));
        objects.addAll(super.buildInsertObjects(entry));
        return objects;
    }

    @Override
    protected String buildInsertTemplate(MysqlBackendEntry.Row entry) {
        if (this.insertTemplate != null) {
            return this.insertTemplate;
        }

        StringBuilder insert = new StringBuilder();
        insert.append("INSERT INTO ").append(this.table()).append(" (");

        int i = 0;
        int size = entry.columns().size();
        for (HugeKeys key : entry.columns().keySet()) {
            insert.append(formatKey(key));
            if (++i != size) {
                insert.append(", ");
            }
        }
        insert.append(") VALUES (");

        for (i = 0; i < size; i++) {
            insert.append("?");
            if (i != size - 1) {
                insert.append(", ");
            }
        }
        insert.append(")");

        i = 0;
        size = this.tableDefine().keys().size();
        insert.append(" ON CONFLICT (");
        for (HugeKeys key : this.tableDefine().keys()) {
            insert.append(formatKey(key));
            if (++i != size) {
                insert.append(", ");
            }
        }
        insert.append(")");

        i = 0;
        size = entry.columns().keySet().size();
        insert.append(" DO UPDATE SET ");
        for (HugeKeys key : entry.columns().keySet()) {
            insert.append(formatKey(key)).append(" = ?");
            if (++i != size) {
                insert.append(", ");
            }
        }

        this.insertTemplate = insert.toString();
        return this.insertTemplate;
    }

    // Set order-by to keep results order consistence for PostgreSQL result
    @Override
    protected String orderByKeys() {
        if (this.orderByKeys != null) {
            return this.orderByKeys;
        }
        int i = 0;
        int size = this.tableDefine().keys().size();
        StringBuilder select = new StringBuilder(" ORDER BY ");
        for (HugeKeys hugeKey : this.tableDefine().keys()) {
            String key = formatKey(hugeKey);
            select.append(key).append(" ");
            select.append("ASC ");
            if (++i != size) {
                select.append(", ");
            }
        }
        this.orderByKeys = select.toString();
        return this.orderByKeys;
    }
}
