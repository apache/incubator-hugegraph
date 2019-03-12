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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.postgresql.core.Utils;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.TableDefine;
import com.baidu.hugegraph.backend.store.mysql.MysqlBackendEntry;
import com.baidu.hugegraph.backend.store.mysql.MysqlEntryIterator;
import com.baidu.hugegraph.backend.store.mysql.MysqlSessions.Session;
import com.baidu.hugegraph.backend.store.mysql.MysqlTable;
import com.baidu.hugegraph.backend.store.mysql.WhereBuilder;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.Log;

public abstract class PostgresqlTable extends MysqlTable {

    private static final Logger LOG = Log.logger(PostgresqlStore.class);

    private String insertTemplate;

    public PostgresqlTable(String table) {
        super(table);
    }

    @Override
    protected void createTable(Session session, TableDefine tableDefine) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS ");
        sql.append(this.table()).append(" (");
        // Add columns
        for (Map.Entry<HugeKeys, String> entry :
             tableDefine.columns().entrySet()) {
            sql.append(formatKey(entry.getKey()));
            sql.append(" ");
            sql.append(entry.getValue());
            sql.append(", ");
        }
        // Specified primary keys
        sql.append(" PRIMARY KEY (");
        int i = 0;
        int size = tableDefine.keys().size();
        for (HugeKeys key : tableDefine.keys()) {
            sql.append(formatKey(key));
            if (++i != size) {
                sql.append(", ");
            }
        }
        sql.append("))");

        LOG.debug("Create table: {}", sql);
        try {
            session.execute(sql.toString());
        } catch (SQLException e) {
            throw new BackendException("Failed to create table with '%s'",
                                       e, sql);
        }
    }

    @Override
    protected void dropTable(Session session) {
        LOG.debug("Drop table: {}", this.table());
        String sql = String.format("DROP TABLE IF EXISTS %s CASCADE;",
                                   this.table());
        try {
            session.execute(sql);
        } catch (SQLException e) {
            throw new BackendException("Failed to drop table with '%s'",
                                       e, sql);
        }
    }

    @Override
    protected void truncateTable(Session session) {
        LOG.debug("Truncate table: {}", this.table());
        String sql = String.format("TRUNCATE TABLE %s CASCADE;", this.table());
        try {
            session.execute(sql);
        } catch (SQLException e) {
            throw new BackendException("Failed to truncate table with '%s'",
                                       e, sql);
        }
    }

    /**
     * Insert an entire row
     */
    @Override
    public void insert(Session session, MysqlBackendEntry.Row entry) {
        String template = this.buildInsertTemplate(entry);

        PreparedStatement insertStmt;
        try {
            // Create or get insert prepare statement
            insertStmt = session.prepareStatement(template);
            int i = 1;
            int size = entry.columns().size();
            for (Object object : entry.columns().values()) {
                if (object.equals("\u0000")) {
                    object = "";
                }
                insertStmt.setObject(i, object);
                insertStmt.setObject(size + i++, object);
            }
        } catch (SQLException e) {
            throw new BackendException("Failed to prepare statement '%s'" +
                                       "for entry: %s", template, entry);
        }
        session.add(insertStmt);
    }

    @Override
    public void delete(Session session, MysqlBackendEntry.Row entry) {
        List<HugeKeys> idNames = this.idColumnName();
        String template = this.buildDeleteTemplate(idNames);

        PreparedStatement deleteStmt;
        try {
            deleteStmt = session.prepareStatement(template);
            if (entry.columns().isEmpty()) {
                // Delete just by id
                List<Long> idValues = this.idColumnValue(entry);
                assert idNames.size() == idValues.size();

                for (int i = 0, n = idNames.size(); i < n; i++) {
                    deleteStmt.setObject(i + 1, idValues.get(i));
                }
            } else {
                // Delete just by column keys(must be id columns)
                for (int i = 0, n = idNames.size(); i < n; i++) {
                    HugeKeys key = idNames.get(i);
                    Object value = entry.column(key);
                    if (value != null && value.equals("\u0000")) {
                        value = "\'\'";
                    }

                    deleteStmt.setObject(i + 1, value);
                }
            }
        } catch (SQLException e) {
            throw new BackendException("Failed to prepare statement '%s'" +
                                       "with entry columns %s",
                                       template, entry.columns().values());
        }
        session.add(deleteStmt);
    }

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

    protected void wrapPage(StringBuilder select, Query query) {
        String page = query.page();
        // It's the first time if page is empty
        if (!page.isEmpty()) {
            MysqlEntryIterator.PageState
                    pageState = MysqlEntryIterator.PageState.fromString(page);
            Map<HugeKeys, Object> columns = pageState.columns();

            List<HugeKeys> idColumnNames = this.idColumnName();
            List<Object> values = new ArrayList<>(idColumnNames.size());
            for (HugeKeys key : idColumnNames) {
                values.add(columns.get(key));
            }

            // Need add `where` to `select` when query is IdQuery
            boolean startWithWhere = query.conditions().isEmpty();
            WhereBuilder where = new WhereBuilder(startWithWhere);
            where.gte(formatKeys(idColumnNames), values);
            if (!startWithWhere) {
                select.append(" AND");
            }
            select.append(where.build());
        }

        int i = 0;
        int size = this.tableDefine().keys().size();

        // Set order-by to keep results order consistence for result
        select.append(" ORDER BY ");
        for (HugeKeys hugeKey : this.tableDefine().keys()) {
            String key = formatKey(hugeKey);
            select.append(key).append(" ");
            select.append("ASC ");
            if (++i != size) {
                select.append(", ");
            }
        }

        assert query.limit() != Query.NO_LIMIT;
        // Fetch `limit + 1` records for judging whether reached the last page
        select.append(" limit ");
        select.append(query.limit() + 1);
        select.append(";");
    }

    @Override
    protected StringBuilder relation2Sql(Condition.Relation relation) {
        String key = relation.serialKey().toString();
        Object value = relation.serialValue();

        value = serializeValue(value);

        StringBuilder sql = new StringBuilder(32);
        sql.append(key);
        switch (relation.relation()) {
            case EQ:
                sql.append(" = ").append(value);
                break;
            case NEQ:
                sql.append(" != ").append(value);
                break;
            case GT:
                sql.append(" > ").append(value);
                break;
            case GTE:
                sql.append(" >= ").append(value);
                break;
            case LT:
                sql.append(" < ").append(value);
                break;
            case LTE:
                sql.append(" <= ").append(value);
                break;
            case IN:
                sql.append(" IN (");
                List<?> values = (List<?>) value;
                for (int i = 0, n = values.size(); i < n; i++) {
                    sql.append(serializeValue(values.get(i)));
                    if (i != n - 1) {
                        sql.append(", ");
                    }
                }
                sql.append(")");
                break;
            case CONTAINS:
            case CONTAINS_KEY:
            case SCAN:
            default:
                throw new AssertionError("Unsupported relation: " + relation);
        }
        return sql;
    }

    protected static Object serializeValue(Object value) {
        if (value instanceof Id) {
            value = ((Id) value).asObject();
        }
        if (value instanceof String) {
            if (value == "\u0000") {
                return "\'\'";
            }
            StringBuilder builder = new StringBuilder(32);
            builder.append('\'');
            try {
                Utils.escapeLiteral(builder, (String) value, false);
            } catch (SQLException e) {
                throw new HugeException("Failed to escape '%s'", e, value);
            }
            builder.append('\'');
            value = builder.toString();
        }
        return value;
    }
}
