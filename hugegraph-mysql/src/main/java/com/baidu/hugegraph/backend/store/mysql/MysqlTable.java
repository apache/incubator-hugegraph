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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendTable;
import com.baidu.hugegraph.backend.store.TableDefine;
import com.baidu.hugegraph.backend.store.mysql.MysqlEntryIterator.PageState;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.iterator.ExtendableIterator;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.ImmutableList;

public abstract class MysqlTable extends BackendTable<MysqlSessions.Session,
                                                      MysqlBackendEntry.Row> {

    private static final Logger LOG = Log.logger(MysqlStore.class);

    // The template for insert and delete statements
    private String insertTemplate;
    private String deleteTemplate;

    public MysqlTable(String table) {
        super(table);
        this.insertTemplate = null;
        this.deleteTemplate = null;
    }

    public abstract TableDefine tableDefine();

    @Override
    public void init(MysqlSessions.Session session) {
        this.createTable(session, this.tableDefine());
    }

    @Override
    public void clear(MysqlSessions.Session session) {
        this.dropTable(session);
    }

    public void truncate(MysqlSessions.Session session) {
        this.truncateTable(session);
    }

    protected void createTable(MysqlSessions.Session session,
                               TableDefine tableDefine) {
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
        for (HugeKeys key : tableDefine.keys()) {
            sql.append(key);
            if (++i != tableDefine.keys().size()) {
                sql.append(", ");
            }
        }

        sql.append(")) ENGINE=InnoDB DEFAULT CHARSET=utf8;");

        LOG.debug("Create table: {}", sql);
        try {
            session.execute(sql.toString());
        } catch (SQLException e) {
            throw new BackendException("Failed to create table with '%s'",
                                       e, sql);
        }
    }

    protected void dropTable(MysqlSessions.Session session) {
        LOG.debug("Drop table: {}", this.table());
        String sql = String.format("DROP TABLE IF EXISTS %s;", this.table());
        try {
            session.execute(sql);
        } catch (SQLException e) {
            throw new BackendException("Failed to drop table with '%s'",
                                       e, sql);
        }
    }

    protected void truncateTable(MysqlSessions.Session session) {
        LOG.debug("Truncate table: {}", this.table());
        String sql = String.format("TRUNCATE TABLE %s;", this.table());
        try {
            session.execute(sql);
        } catch (SQLException e) {
            throw new BackendException("Failed to truncate table with '%s'",
                                       e, sql);
        }
    }

    protected List<HugeKeys> idColumnName() {
        return this.tableDefine().keys();
    }

    protected List<Long> idColumnValue(MysqlBackendEntry.Row entry) {
        return ImmutableList.of(entry.id().asLong());
    }

    protected List<Object> idColumnValue(Id id) {
        return ImmutableList.of(id.asObject());
    }

    protected String buildInsertTemplate(MysqlBackendEntry.Row entry) {
        if (this.insertTemplate != null) {
            return this.insertTemplate;
        }

        StringBuilder insert = new StringBuilder();
        insert.append("REPLACE INTO ").append(this.table()).append(" (");

        int i = 0;
        int n = entry.columns().size();
        for (HugeKeys key : entry.columns().keySet()) {
            insert.append(formatKey(key));
            if (++i != n) {
                insert.append(", ");
            }
        }
        insert.append(") VALUES (");
        // Fill with '?'
        for (i = 0; i < n; i++) {
            insert.append("?");
            if (i != n - 1) {
                insert.append(", ");
            }
        }
        insert.append(")");

        this.insertTemplate = insert.toString();
        return this.insertTemplate;
    }

    protected String buildDeleteTemplate(List<HugeKeys> idNames) {
        if (this.deleteTemplate != null) {
            return this.deleteTemplate;
        }

        StringBuilder delete = new StringBuilder();
        delete.append("DELETE FROM ").append(this.table());
        this.appendPartition(delete);

        WhereBuilder where = new WhereBuilder();
        where.and(formatKeys(idNames), "?");
        delete.append(where.build());

        this.deleteTemplate = delete.toString();
        return this.deleteTemplate;
    }

    /**
     * Insert an entire row
     */
    @Override
    public void insert(MysqlSessions.Session session,
                       MysqlBackendEntry.Row entry) {
        String template = this.buildInsertTemplate(entry);

        PreparedStatement insertStmt;
        try {
            // Create or get insert prepare statement
            insertStmt = session.prepareStatement(template);
            int i = 1;
            for (Object object : entry.columns().values()) {
                insertStmt.setObject(i++, object);
            }
        } catch (SQLException e) {
            throw new BackendException("Failed to prepare statement '%s'" +
                                       "for entry: %s", template, entry);
        }
        session.add(insertStmt);
    }

    @Override
    public void delete(MysqlSessions.Session session,
                       MysqlBackendEntry.Row entry) {
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

    @Override
    public void append(MysqlSessions.Session session,
                       MysqlBackendEntry.Row entry) {
        this.insert(session, entry);
    }

    @Override
    public void eliminate(MysqlSessions.Session session,
                          MysqlBackendEntry.Row entry) {
        this.delete(session, entry);
    }

    @Override
    public Iterator<BackendEntry> query(MysqlSessions.Session session,
                                        Query query) {
        ExtendableIterator<BackendEntry> rs = new ExtendableIterator<>();

        if (query.limit() == 0 && query.limit() != Query.NO_LIMIT) {
            LOG.debug("Return empty result(limit=0) for query {}", query);
            return rs;
        }

        List<StringBuilder> selections = this.query2Select(this.table(), query);
        try {
            for (StringBuilder selection : selections) {
                ResultSet results = session.select(selection.toString());
                rs.extend(this.results2Entries(query, results));
            }
        } catch (SQLException e) {
            throw new BackendException("Failed to query [%s]", e, query);
        }

        LOG.debug("Return {} for query {}", rs, query);
        return rs;
    }

    protected List<StringBuilder> query2Select(String table, Query query) {
        // Set table
        StringBuilder select = new StringBuilder(64);
        select.append("SELECT * FROM ").append(table);

        // Is query by id?
        List<StringBuilder> ids = this.queryId2Select(query, select);

        List<StringBuilder> selections;

        if (query.conditions().isEmpty()) {
            // Query only by id
            LOG.debug("Query only by id(s): {}", ids);
            selections = ids;
        } else {
            selections = new ArrayList<>(ids.size());
            for (StringBuilder selection : ids) {
                // Query by condition
                selections.addAll(this.queryCondition2Select(query, selection));
            }
            LOG.debug("Query by conditions: {}", selections);
        }
        // Set page, order-by and limit
        for (StringBuilder selection : selections) {
            if (!query.orders().isEmpty()) {
                this.wrapOrderBy(selection, query);
            }
            if (query.paging()) {
                this.wrapPage(selection, query);
            } else if (query.limit() != Query.NO_LIMIT || query.offset() > 0) {
                this.wrapOffset(selection, query);
            }
        }

        return selections;
    }

    protected List<StringBuilder> queryId2Select(Query query,
                                                 StringBuilder select) {
        // Query by id(s)
        if (query.ids().isEmpty()) {
            return ImmutableList.of(select);
        }

        List<HugeKeys> nameParts = this.idColumnName();

        List<List<Object>> ids = new ArrayList<>(query.ids().size());
        for (Id id : query.ids()) {
            List<Object> idParts = this.idColumnValue(id);
            if (nameParts.size() != idParts.size()) {
                throw new NotFoundException(
                          "Unsupported ID format: '%s' (should contain %s)",
                          id, nameParts);
            }
            ids.add(idParts);
        }

        // Query only by partition-key
        if (nameParts.size() == 1) {
            List<Object> values = new ArrayList<>(ids.size());
            for (List<Object> objects : ids) {
                assert objects.size() == 1;
                values.add(objects.get(0));
            }

            WhereBuilder where = new WhereBuilder();
            where.in(formatKey(nameParts.get(0)), values);
            select.append(where.build());
            return ImmutableList.of(select);
        }

        /*
         * Query by partition-key + clustering-key
         * NOTE: Error if multi-column IN clause include partition key:
         * error: multi-column relations can only be applied to clustering
         * columns when using: select.where(QueryBuilder.in(names, idList));
         * So we use multi-query instead of IN
         */
        List<StringBuilder> selections = new ArrayList<>(ids.size());
        for (List<Object> objects : ids) {
            assert nameParts.size() == objects.size();
            StringBuilder idSelection = new StringBuilder(select);
            /*
             * NOTE: concat with AND relation, like:
             * "pk = id and ck1 = v1 and ck2 = v2"
             */
            WhereBuilder where = new WhereBuilder();
            where.and(formatKeys(nameParts), objects);

            idSelection.append(where.build());
            selections.add(idSelection);
        }
        return selections;
    }

    protected List<StringBuilder> queryCondition2Select(Query query,
                                                        StringBuilder select) {
        // Query by conditions
        Set<Condition> conditions = query.conditions();
        List<StringBuilder> clauses = new ArrayList<>(conditions.size());
        for (Condition condition : conditions) {
            clauses.add(this.condition2Sql(condition));
        }
        WhereBuilder where = new WhereBuilder();
        where.and(clauses);
        select.append(where.build());
        return ImmutableList.of(select);
    }

    protected StringBuilder condition2Sql(Condition condition) {
        switch (condition.type()) {
            case AND:
                Condition.And and = (Condition.And) condition;
                StringBuilder left = this.condition2Sql(and.left());
                StringBuilder right = this.condition2Sql(and.right());
                int size = left.length() + right.length() + " AND ".length();
                StringBuilder sql = new StringBuilder(size);
                sql.append(left).append(" AND ").append(right);
                return sql;
            case OR:
                throw new BackendException("Not support OR currently");
            case RELATION:
                Condition.Relation r = (Condition.Relation) condition;
                return this.relation2Sql(r);
            default:
                final String msg = "Unsupported condition: " + condition;
                throw new AssertionError(msg);
        }
    }

    protected StringBuilder relation2Sql(Condition.Relation relation) {
        String key = relation.serialKey().toString();
        Object value = relation.serialValue();

        // Serialize value (TODO: should move to Serializer)
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

    protected void wrapOrderBy(StringBuilder select, Query query) {
        int size = query.orders().size();
        assert size > 0;

        int i = 0;
        // Set order-by
        select.append(" ORDER BY ");
        for (Map.Entry<HugeKeys, Query.Order> order :
             query.orders().entrySet()) {
            String key = formatKey(order.getKey());
            Query.Order value = order.getValue();
            select.append(key).append(" ");
            if (value == Query.Order.ASC) {
                select.append("ASC");
            } else {
                assert value == Query.Order.DESC;
                select.append("DESC");
            }
            if (++i != size) {
                select.append(", ");
            }
        }
    }

    protected void wrapPage(StringBuilder select, Query query) {
        String page = query.page();
        // It's the first time if page is empty
        if (!page.isEmpty()) {
            PageState pageState = PageState.fromString(page);
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
            select.append(where.build());
        }

        assert query.limit() != Query.NO_LIMIT;
        // Fetch `limit + 1` records for judging whether reached the last page
        select.append(" limit ");
        select.append(query.limit() + 1);
        select.append(";");
    }

    protected void wrapOffset(StringBuilder select, Query query) {
        assert query.limit() >= 0;
        assert query.offset() >= 0;
        // Set limit and offset
        select.append(" limit ");
        select.append(query.limit());
        select.append(" offset ");
        select.append(query.offset());
        select.append(";");
    }

    private static Object serializeValue(Object value) {
        if (value instanceof Id) {
            value = ((Id) value).asObject();
        }
        if (value instanceof String) {
            value = MysqlUtil.escapeString((String) value);
        }
        return value;
    }

    protected Iterator<BackendEntry> results2Entries(Query query,
                                                     ResultSet results) {
        return new MysqlEntryIterator(results, query, this::mergeEntries);
    }

    protected BackendEntry mergeEntries(BackendEntry e1, BackendEntry e2) {
        // Return the next entry (not merged)
        return e2;
    }

    protected void appendPartition(StringBuilder delete) {
        // pass
    }

    public static String formatKey(HugeKeys key) {
        return key.name();
    }

    public static HugeKeys parseKey(String name) {
        return HugeKeys.valueOf(name.toUpperCase());
    }

    public static List<String> formatKeys(List<HugeKeys> keys) {
        List<String> names = new ArrayList<>(keys.size());
        for (HugeKeys key : keys) {
            names.add(formatKey(key));
        }
        return names;
    }
}
