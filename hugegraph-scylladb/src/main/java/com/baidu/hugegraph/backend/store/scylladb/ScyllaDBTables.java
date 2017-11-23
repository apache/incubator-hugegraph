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

package com.baidu.hugegraph.backend.store.scylladb;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

import org.apache.tinkerpop.gremlin.structure.Direction;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.cassandra.CassandraBackendEntry;
import com.baidu.hugegraph.backend.store.cassandra.CassandraSessionPool;
import com.baidu.hugegraph.backend.store.cassandra.CassandraTable;
import com.baidu.hugegraph.backend.store.cassandra.CassandraTables;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class ScyllaDBTables {

    private static final String ELEMENT_IDS =
                                CassandraTable.formatKey(HugeKeys.ELEMENT_IDS);

    /**
     * Create index label table for vertex/edge
     * @param session DB session
     * @param table index table name
     */
    private static void createIndexTable(CassandraSessionPool.Session session,
                                         String table) {
        Create tableBuilder = SchemaBuilder.createTable(table).ifNotExists();
        tableBuilder.addPartitionKey(CassandraTable.formatKey(HugeKeys.LABEL),
                                     DataType.text());
        tableBuilder.addColumn(CassandraTable.formatKey(HugeKeys.ELEMENT_IDS),
                               DataType.set(DataType.text()));
        session.execute(tableBuilder);
    }

    /**
     * Append data to label index table
     */
    private static void appendLabelIndex(CassandraSessionPool.Session session,
                                         String table,
                                         CassandraBackendEntry.Row entry) {
        Update update = QueryBuilder.update(table);

        update.with(QueryBuilder.append(ELEMENT_IDS, entry.id().asString()));
        update.where(CassandraTable.formatEQ(HugeKeys.LABEL,
                                             entry.column(HugeKeys.LABEL)));
        session.add(update);
    }

    /**
     * Remove data from label index table
     */
    private static void removeLabelIndex(CassandraSessionPool.Session session,
                                         String table,
                                         CassandraBackendEntry.Row entry) {
        Update update = QueryBuilder.update(table);

        Object label = entry.column(HugeKeys.LABEL);
        if (label == null) {
            // Maybe delete edges by edge label(passed by id)
            assert entry.id().asString().indexOf(':') < 0 : entry;
            return;
        }
        update.with(QueryBuilder.remove(ELEMENT_IDS, entry.id().asString()));
        update.where(CassandraTable.formatEQ(HugeKeys.LABEL, label));
        session.add(update);
    }

    /**
     * Query data from label index table if just want to query by label
     */
    private static Query queryByLabelIndex(
            CassandraSessionPool.Session session, String table, Query query) {
        Set<Condition> conditions = query.conditions();

        if (!(query instanceof ConditionQuery) || conditions.isEmpty()) {
            return query;
        }

        ConditionQuery cq = (ConditionQuery) query;
        String label = (String) cq.condition(HugeKeys.LABEL);
        if (label != null && cq.allSysprop() && conditions.size() == 1 &&
            cq.containsCondition(HugeKeys.LABEL, Condition.RelationType.EQ)) {

            Set<String> ids = queryByLabelIndex(session, table, label);
            if (ids.isEmpty()) {
                // Not found data with the specified label
                return null;
            }

            cq.resetConditions();
            for (String id : ids) {
                cq.query(IdGenerator.of(id));
            }
        }
        return query;
    }

    private static Set<String> queryByLabelIndex(
            CassandraSessionPool.Session session, String table, String label) {

        Select select = QueryBuilder.select().from(table);
        select.where(CassandraTable.formatEQ(HugeKeys.LABEL, label));

        try {
            Iterator<Row> it = session.execute(select).iterator();
            if (!it.hasNext()) {
                return ImmutableSet.of();
            }
            Set<String> ids = it.next().getSet(ELEMENT_IDS, String.class);
            assert !it.hasNext();
            return ids;
        } catch (DriverException e) {
            throw new BackendException("Failed to query by label '%s'",
                                       e, label);
        }
    }

    public static class Vertex extends CassandraTables.Vertex {

        private static final String LABEL_INDEX_TABLE = "vertex_label_index";

        @Override
        protected void createIndex(CassandraSessionPool.Session session,
                                   String indexName,
                                   HugeKeys column) {
            createIndexTable(session, LABEL_INDEX_TABLE);
        }

        @Override
        protected void dropTable(CassandraSessionPool.Session session) {
            session.execute(SchemaBuilder.dropTable(LABEL_INDEX_TABLE)
                                         .ifExists());
            super.dropTable(session);
        }

        @Override
        public void insert(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            super.insert(session, entry);
            appendLabelIndex(session, LABEL_INDEX_TABLE, entry);
        }

        @Override
        public void delete(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            removeLabelIndex(session, LABEL_INDEX_TABLE, entry);
            super.delete(session, entry);
        }

        @Override
        public Iterator<BackendEntry> query(
               CassandraSessionPool.Session session,
               Query query) {
            query = queryByLabelIndex(session, LABEL_INDEX_TABLE, query);
            if (query == null) {
                return ImmutableList.<BackendEntry>of().iterator();
            }
            return super.query(session, query);
        }
    }

    public static class Edge extends CassandraTables.Edge {

        private static final String LABEL_INDEX_TABLE = "edge_label_index";

        @Override
        protected void createIndex(CassandraSessionPool.Session session,
                                   String indexName,
                                   HugeKeys column) {
            createIndexTable(session, LABEL_INDEX_TABLE);
        }

        @Override
        protected void dropTable(CassandraSessionPool.Session session) {
            session.execute(SchemaBuilder.dropTable(LABEL_INDEX_TABLE)
                                         .ifExists());
            super.dropTable(session);
        }

        @Override
        public void insert(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            super.insert(session, entry);
            appendLabelIndex(session, LABEL_INDEX_TABLE, entry);
        }

        @Override
        public void delete(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            removeLabelIndex(session, LABEL_INDEX_TABLE, entry);
            super.delete(session, entry);
        }

        @Override
        protected void deleteEdgesByLabel(CassandraSessionPool.Session session,
                                          String label) {
            // Query edge id(s) by label index
            Set<String> ids = queryByLabelIndex(session,
                                                LABEL_INDEX_TABLE,
                                                label);
            if (ids.isEmpty()) {
                return;
            }

            // Delete index
            Delete del = QueryBuilder.delete().from(LABEL_INDEX_TABLE);
            del.where(formatEQ(HugeKeys.LABEL, label));
            session.add(del);

            // Delete edges by id(s)
            List<HugeKeys> idNames = idColumnName();

            BiConsumer<Id, Direction> deleteEdge = (id, direction) -> {
                List<String> idValues = idColumnValue(id, direction);
                assert idNames.size() == idValues.size();

                Delete delete = QueryBuilder.delete().from(this.table());
                for (int i = 0, n = idNames.size(); i < n; i++) {
                    delete.where(formatEQ(idNames.get(i), idValues.get(i)));
                }
                session.add(delete);
            };

            for (String s : ids) {
                Id id = IdGenerator.of(s);
                deleteEdge.accept(id, Direction.OUT);
                deleteEdge.accept(id, Direction.IN);
            }
        }

        @Override
        public Iterator<BackendEntry> query(
               CassandraSessionPool.Session session, Query query) {
            query = queryByLabelIndex(session, LABEL_INDEX_TABLE, query);
            if (query == null) {
                return ImmutableList.<BackendEntry>of().iterator();
            }
            return super.query(session, query);
        }
    }
}
