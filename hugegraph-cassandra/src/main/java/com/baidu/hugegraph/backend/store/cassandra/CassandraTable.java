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

package com.baidu.hugegraph.backend.store.cassandra;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.page.PageState;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.Condition.Relation;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.query.Query.Order;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendTable;
import com.baidu.hugegraph.backend.store.Shard;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.iterator.ExtendableIterator;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.CopyUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.PagingStateException;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Clauses;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.SchemaStatement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public abstract class CassandraTable
                extends BackendTable<CassandraSessionPool.Session,
                                     CassandraBackendEntry.Row> {

    private static final Logger LOG = Log.logger(CassandraStore.class);
    private static final int MAX_ELEMENTS_IN_CLAUSE = 65535;

    public CassandraTable(String table) {
        super(table);
    }

    @Override
    protected void registerMetaHandlers() {
        this.registerMetaHandler("splits", (session, meta, args) -> {
            E.checkArgument(args.length == 1,
                            "The args count of %s must be 1", meta);
            long splitSize = (long) args[0];
            CassandraShard spliter = new CassandraShard(session,
                                                        session.keyspace(),
                                                        this.table());
            return spliter.getSplits(0, splitSize);
        });
    }

    @Override
    public Iterator<BackendEntry> query(CassandraSessionPool.Session session,
                                        Query query) {
        ExtendableIterator<BackendEntry> rs = new ExtendableIterator<>();

        if (query.limit() == 0 && query.limit() != Query.NO_LIMIT) {
            LOG.debug("Return empty result(limit=0) for query {}", query);
            return rs;
        }

        List<Select> selections = this.query2Select(this.table(), query);
        try {
            for (Select selection : selections) {
                ResultSet results = session.query(selection);
                rs.extend(this.results2Entries(query, results));
            }
        } catch (DriverException e) {
            throw new BackendException("Failed to query [%s]", e, query);
        }

        LOG.debug("Return {} for query {}", rs, query);
        return rs;
    }

    protected List<Select> query2Select(String table, Query query) {
        // Set table
        Select select = QueryBuilder.select().from(table);

        // NOTE: Cassandra does not support query.offset()
        if (query.offset() != 0) {
            LOG.debug("Query offset is not supported on Cassandra store " +
                      "currently, it will be replaced by [0, offset + limit)");
        }

        // Set order-by
        for (Map.Entry<HugeKeys, Order> order : query.orders().entrySet()) {
            String name = formatKey(order.getKey());
            if (order.getValue() == Order.ASC) {
                select.orderBy(QueryBuilder.asc(name));
            } else {
                assert order.getValue() == Order.DESC;
                select.orderBy(QueryBuilder.desc(name));
            }
        }

        // Is query by id?
        List<Select> ids = this.queryId2Select(query, select);

        if (query.conditions().isEmpty()) {
            // Query only by id
            this.setPageState(query, ids);
            LOG.debug("Query only by id(s): {}", ids);
            return ids;
        } else {
            List<Select> conds = new ArrayList<>(ids.size());
            for (Select selection : ids) {
                // Query by condition
                conds.addAll(this.queryCondition2Select(query, selection));
            }
            this.setPageState(query, conds);
            LOG.debug("Query by conditions: {}", conds);
            return conds;
        }
    }

    protected void setPageState(Query query, List<Select> selects) {
        if (query.limit() == Query.NO_LIMIT) {
            return;
        }
        for (Select select : selects) {
            long total = query.total();
            String page = query.page();
            if (page == null) {
                // Set limit
                select.limit((int) total);
            } else {
                select.setFetchSize((int) total);
                // It's the first time if page is empty
                if (!page.isEmpty()) {
                    byte[] position = PageState.fromString(page).position();
                    try {
                        select.setPagingState(PagingState.fromBytes(position));
                    } catch (PagingStateException e) {
                        throw new BackendException(e);
                    }
                }
            }
        }
    }

    protected List<Select> queryId2Select(Query query, Select select) {
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
            List<Object> idList = new ArrayList<>(ids.size());
            for (List<Object> id : ids) {
                assert id.size() == 1;
                idList.add(id.get(0));
            }
            return this.ids2IdSelects(select, nameParts.get(0), idList);
        }

        /*
         * Query by partition-key + clustering-key
         * NOTE: Error if multi-column IN clause include partition key:
         * error: multi-column relations can only be applied to clustering
         * columns when using: select.where(QueryBuilder.in(names, idList));
         * So we use multi-query instead of IN
         */
        List<Select> selects = new ArrayList<>(ids.size());
        for (List<Object> id : ids) {
            assert nameParts.size() == id.size();
            Select idSelect = cloneSelect(select, this.table());
            /*
             * NOTE: concat with AND relation, like:
             * "pk = id and ck1 = v1 and ck2 = v2"
             */
            for (int i = 0, n = nameParts.size(); i < n; i++) {
                idSelect.where(formatEQ(nameParts.get(i), id.get(i)));
            }
            selects.add(idSelect);
        }
        return selects;
    }

    protected Collection<Select> queryCondition2Select(Query query,
                                                       Select select) {
        // Query by conditions
        Set<Condition> conditions = query.conditions();
        for (Condition condition : conditions) {
            Clause clause = condition2Cql(condition);
            select.where(clause);
            if (Clauses.needAllowFiltering(clause)) {
                select.allowFiltering();
            }
        }
        return ImmutableList.of(select);
    }

    protected Clause condition2Cql(Condition condition) {
        switch (condition.type()) {
            case AND:
                Condition.And and = (Condition.And) condition;
                Clause left = condition2Cql(and.left());
                Clause right = condition2Cql(and.right());
                return Clauses.and(left, right);
            case OR:
                throw new BackendException("Not support OR currently");
            case RELATION:
                Condition.Relation r = (Condition.Relation) condition;
                return relation2Cql(r);
            default:
                final String msg = "Unsupported condition: " + condition;
                throw new AssertionError(msg);
        }
    }

    protected Clause relation2Cql(Relation relation) {
        String key = relation.serialKey().toString();
        Object value = relation.serialValue();

        switch (relation.relation()) {
            case EQ:
                return QueryBuilder.eq(key, value);
            case GT:
                return QueryBuilder.gt(key, value);
            case GTE:
                return QueryBuilder.gte(key, value);
            case LT:
                return QueryBuilder.lt(key, value);
            case LTE:
                return QueryBuilder.lte(key, value);
            case IN:
                return QueryBuilder.in(key, value);
            case CONTAINS:
                return QueryBuilder.contains(key, value);
            case CONTAINS_KEY:
                return QueryBuilder.containsKey(key, value);
            case SCAN:
                String[] col = pkColumnName().stream()
                                             .map(pk -> formatKey(pk))
                                             .toArray(String[]::new);
                Shard shard = (Shard) value;
                Object start = QueryBuilder.raw(shard.start());
                Object end = QueryBuilder.raw(shard.end());
                return Clauses.and(
                        QueryBuilder.gte(QueryBuilder.token(col), start),
                        QueryBuilder.lt(QueryBuilder.token(col), end));
            /*
             * Currently we can't support LIKE due to error:
             * "cassandra no viable alternative at input 'like'..."
             */
            // case LIKE:
            //    return QueryBuilder.like(key, value);
            case NEQ:
            default:
                throw new AssertionError("Unsupported relation: " + relation);
        }
    }

    private List<Select> ids2IdSelects(Select select, HugeKeys key,
                                       List<Object> ids) {
        int size = ids.size();
        List<Select> selects = new ArrayList<>();
        for (int i = 0, j; i < size; i = j) {
            j = Math.min(i + MAX_ELEMENTS_IN_CLAUSE, size);
            Select idSelect = cloneSelect(select, this.table());
            idSelect.where(QueryBuilder.in(formatKey(key), ids.subList(i, j)));
            selects.add(idSelect);
        }
        return selects;
    }

    protected static Select cloneSelect(Select select, String table) {
        // NOTE: there is no Select.clone(), just use copy instead
        return CopyUtil.copy(select, QueryBuilder.select().from(table));
    }

    protected Iterator<BackendEntry> results2Entries(Query q, ResultSet r) {
        return new CassandraEntryIterator(r, q, (e1, row) -> {
            CassandraBackendEntry e2 = row2Entry(q.resultType(), row);
            return this.mergeEntries(e1, e2);
        });
    }

    private static CassandraBackendEntry row2Entry(HugeType type, Row row) {
        CassandraBackendEntry entry = new CassandraBackendEntry(type);

        List<Definition> cols = row.getColumnDefinitions().asList();
        for (Definition col : cols) {
            String name = col.getName();
            Object value = row.getObject(name);
            entry.column(CassandraTable.parseKey(name), value);
        }

        return entry;
    }

    protected List<HugeKeys> pkColumnName() {
        return idColumnName();
    }

    protected List<HugeKeys> idColumnName() {
        return ImmutableList.of(HugeKeys.ID);
    }

    protected List<Object> idColumnValue(Id id) {
        return ImmutableList.of(id.asObject());
    }

    protected List<Long> idColumnValue(CassandraBackendEntry.Row entry) {
        return ImmutableList.of(entry.id().asLong());
    }

    protected List<HugeKeys> modifiableColumnName() {
        return ImmutableList.of(HugeKeys.PROPERTIES);
    }

    protected BackendEntry mergeEntries(BackendEntry e1, BackendEntry e2) {
        // Return the next entry (not merged)
        return e2;
    }

    public static final String formatKey(HugeKeys key) {
        return key.name();
    }

    public static final HugeKeys parseKey(String name) {
        return HugeKeys.valueOf(name.toUpperCase());
    }

    public static final Clause formatEQ(HugeKeys key, Object value) {
        return QueryBuilder.eq(formatKey(key), value);
    }

    /**
     * Insert an entire row
     */
    @Override
    public void insert(CassandraSessionPool.Session session,
                       CassandraBackendEntry.Row entry) {
        assert entry.columns().size() > 0;
        Insert insert = QueryBuilder.insertInto(this.table());

        for (Map.Entry<HugeKeys, Object> c : entry.columns().entrySet()) {
            insert.value(formatKey(c.getKey()), c.getValue());
        }

        session.add(insert);
    }

    /**
     * Append several elements to the collection column of a row
     */
    @Override
    public void append(CassandraSessionPool.Session session,
                       CassandraBackendEntry.Row entry) {

        List<HugeKeys> idNames = this.idColumnName();
        List<HugeKeys> colNames = this.modifiableColumnName();

        Map<HugeKeys, Object> columns = entry.columns();

        Update update = QueryBuilder.update(table());

        for (HugeKeys key : colNames) {
            if (!columns.containsKey(key)) {
                continue;
            }

            String name = formatKey(key);
            Object value = columns.get(key);

            if (value instanceof Map) {
                update.with(QueryBuilder.putAll(name, (Map<?, ?>) value));
            } else if (value instanceof List) {
                update.with(QueryBuilder.appendAll(name, (List<?>) value));
            } else {
                update.with(QueryBuilder.append(name, value));
            }
        }

        for (HugeKeys idName : idNames) {
            assert columns.containsKey(idName);
            update.where(formatEQ(idName, columns.get(idName)));
        }

        session.add(update);
    }

    /**
     * Eliminate several elements from the collection column of a row
     */
    @Override
    public void eliminate(CassandraSessionPool.Session session,
                          CassandraBackendEntry.Row entry) {

        List<HugeKeys> idNames = this.idColumnName();
        List<HugeKeys> colNames = this.modifiableColumnName();

        Map<HugeKeys, Object> columns = entry.columns();

        // Update by id
        Update update = QueryBuilder.update(table());

        for (HugeKeys key : colNames) {
            /*
             * NOTE: eliminate from map<text, text> should just pass key,
             * if use the following statement:
             * UPDATE vertices SET PROPERTIES=PROPERTIES-{'city':'"Wuhan"'}
             * WHERE LABEL='person' AND PRIMARY_VALUES='josh';
             * it will throw a cassandra exception:
             * Invalid map literal for properties of typefrozen<set<text>>
             */
            if (!columns.containsKey(key)) {
                continue;
            }

            String name = formatKey(key);
            Object value = columns.get(key);
            if (value instanceof Map) {
                @SuppressWarnings("rawtypes")
                Set<?> keySet = ((Map) value).keySet();
                update.with(QueryBuilder.removeAll(name, keySet));
            } else if (value instanceof Set) {
                update.with(QueryBuilder.removeAll(name, (Set<?>) value));
            } else if (value instanceof List) {
                Set<?> keySet = new HashSet<>((List<?>) value);
                update.with(QueryBuilder.removeAll(name, keySet));
            } else {
                update.with(QueryBuilder.remove(name, value));
            }
        }

        for (HugeKeys idName : idNames) {
            assert columns.containsKey(idName);
            update.where(formatEQ(idName, columns.get(idName)));
        }

        session.add(update);
    }

    /**
     * Delete an entire row
     */
    @Override
    public void delete(CassandraSessionPool.Session session,
                       CassandraBackendEntry.Row entry) {
        List<HugeKeys> idNames = this.idColumnName();
        Delete delete = QueryBuilder.delete().from(this.table());

        if (entry.columns().isEmpty()) {
            // Delete just by id
            List<Long> idValues = this.idColumnValue(entry);
            assert idNames.size() == idValues.size();

            for (int i = 0, n = idNames.size(); i < n; i++) {
                delete.where(formatEQ(idNames.get(i), idValues.get(i)));
            }
        } else {
            // Delete just by column keys(must be id columns)
            for (HugeKeys idName : idNames) {
                // TODO: should support other filters (like containsKey)
                delete.where(formatEQ(idName, entry.column(idName)));
            }
            /*
             * TODO: delete by id + keys(like index element-ids -- it seems
             * has been replaced by eliminate() method)
             */
        }

        session.add(delete);
    }

    protected void createTable(CassandraSessionPool.Session session,
                               ImmutableMap<HugeKeys, DataType> partitionKeys,
                               ImmutableMap<HugeKeys, DataType> clusteringKeys,
                               ImmutableMap<HugeKeys, DataType> columns) {

        Create table = SchemaBuilder.createTable(this.table()).ifNotExists();

        for (Map.Entry<HugeKeys, DataType> entry : partitionKeys.entrySet()) {
            table.addPartitionKey(formatKey(entry.getKey()), entry.getValue());
        }
        for (Map.Entry<HugeKeys, DataType> entry : clusteringKeys.entrySet()) {
            table.addClusteringColumn(formatKey(entry.getKey()),
                                      entry.getValue());
        }
        for (Map.Entry<HugeKeys, DataType> entry : columns.entrySet()) {
            table.addColumn(formatKey(entry.getKey()), entry.getValue());
        }

        LOG.debug("Create table: {}", table);
        session.execute(table);
    }

    protected void dropTable(CassandraSessionPool.Session session) {
        LOG.debug("Drop table: {}", this.table());
        session.execute(SchemaBuilder.dropTable(this.table()).ifExists());
    }

    protected void truncateTable(CassandraSessionPool.Session session) {
        LOG.debug("Truncate table: {}", this.table());
        session.execute(QueryBuilder.truncate(this.table()));
    }

    protected void createIndex(CassandraSessionPool.Session session,
                               String indexLabel, HugeKeys column) {
        String indexName = joinTableName(this.table(), indexLabel);
        SchemaStatement index = SchemaBuilder.createIndex(indexName)
                                             .ifNotExists()
                                             .onTable(this.table())
                                             .andColumn(formatKey(column));
        LOG.debug("Create index: {}", index);
        session.execute(index);
    }

    @Override
    public void clear(CassandraSessionPool.Session session) {
        this.dropTable(session);
    }

    public void truncate(CassandraSessionPool.Session session) {
        this.truncateTable(session);
    }
}
