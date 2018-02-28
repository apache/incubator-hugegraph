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
import java.util.function.Consumer;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.id.IdUtil;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.cassandra.CassandraBackendEntry;
import com.baidu.hugegraph.backend.store.cassandra.CassandraSessionPool;
import com.baidu.hugegraph.backend.store.cassandra.CassandraTable;
import com.baidu.hugegraph.backend.store.cassandra.CassandraTables;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.type.define.SerialEnum;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
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

    private static final String NAME =
                                CassandraTable.formatKey(HugeKeys.NAME);
    private static final String LABEL =
                                CassandraTable.formatKey(HugeKeys.LABEL);
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
        tableBuilder.addPartitionKey(LABEL, DataType.cint());
        tableBuilder.addColumn(ELEMENT_IDS, DataType.set(DataType.text()));
        session.execute(tableBuilder);
    }

    /**
     * Append data to label index table
     */
    private static void appendLabelIndex(CassandraSessionPool.Session session,
                                         String table,
                                         CassandraBackendEntry.Row entry) {
        Update update = QueryBuilder.update(table);

        update.with(QueryBuilder.append(ELEMENT_IDS,
                                        IdUtil.writeString(entry.id())));
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
        update.with(QueryBuilder.remove(ELEMENT_IDS,
                                        IdUtil.writeString(entry.id())));
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

        ConditionQuery q = (ConditionQuery) query;
        Id label = (Id) q.condition(HugeKeys.LABEL);
        if (label != null && q.allSysprop() && conditions.size() == 1 &&
            q.containsCondition(HugeKeys.LABEL, Condition.RelationType.EQ)) {

            Set<String> ids = queryByLabelIndex(session, table, label);
            if (ids.isEmpty()) {
                // Not found data with the specified label
                return null;
            }

            q.resetConditions();
            for (String id : ids) {
                /*
                 * NOTE: Do not need to deserialize, because can directly
                 * use the element id to do query from the vertex/edge table
                 */
                q.query(IdGenerator.of(id));
            }
        }
        return query;
    }

    private static Set<String> queryByLabelIndex(
            CassandraSessionPool.Session session, String table, Id label) {

        Select select = QueryBuilder.select().from(table);
        select.where(CassandraTable.formatEQ(HugeKeys.LABEL, label.asLong()));

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

        private static final String LIDX_TABLE = "vertex_label_index";

        @Override
        protected void createIndex(CassandraSessionPool.Session session,
                                   String indexLabel,
                                   HugeKeys column) {
            createIndexTable(session, LIDX_TABLE);
        }

        @Override
        public void dropTable(CassandraSessionPool.Session session) {
            session.execute(SchemaBuilder.dropTable(LIDX_TABLE).ifExists());
            super.dropTable(session);
        }

        @Override
        public void insert(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            super.insert(session, entry);
            appendLabelIndex(session, LIDX_TABLE, entry);
        }

        @Override
        public void delete(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            removeLabelIndex(session, LIDX_TABLE, entry);
            super.delete(session, entry);
        }

        @Override
        public Iterator<BackendEntry> query(
               CassandraSessionPool.Session session,
               Query query) {
            query = queryByLabelIndex(session, LIDX_TABLE, query);
            if (query == null) {
                return ImmutableList.<BackendEntry>of().iterator();
            }
            return super.query(session, query);
        }
    }

    public static class Edge extends CassandraTables.Edge {

        private static final String LIDX_TABLE = "edge_label_index";

        public Edge(Directions direction) {
            super(direction);
        }

        @Override
        protected void createIndex(CassandraSessionPool.Session session,
                                   String indexLabel,
                                   HugeKeys column) {
            createIndexTable(session, LIDX_TABLE);
        }

        @Override
        public void dropTable(CassandraSessionPool.Session session) {
            session.execute(SchemaBuilder.dropTable(LIDX_TABLE).ifExists());
            super.dropTable(session);
        }

        @Override
        public void insert(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            super.insert(session, entry);
            Byte dir = entry.column(HugeKeys.DIRECTION);
            Directions direction = SerialEnum.fromCode(Directions.class, dir);
            if (direction == Directions.OUT) {
                appendLabelIndex(session, LIDX_TABLE, entry);
            }
        }

        @Override
        public void delete(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            removeLabelIndex(session, LIDX_TABLE, entry);
            super.delete(session, entry);
        }

        @Override
        protected void deleteEdgesByLabel(CassandraSessionPool.Session session,
                                          Id label) {

            // Query edge id(s) by label index
            Set<String> ids = queryByLabelIndex(session, LIDX_TABLE, label);
            if (ids.isEmpty()) {
                return;
            }

            // Delete index
            Delete del = QueryBuilder.delete().from(LIDX_TABLE);
            del.where(formatEQ(HugeKeys.LABEL, label.asLong()));
            session.add(del);

            // Delete edges by id(s)
            List<HugeKeys> idNames = idColumnName();

            Consumer<EdgeId> deleteEdge = (id) -> {
                List<Object> idValues = idColumnValue(id);
                assert idNames.size() == idValues.size();

                // Delete edges in OUT and IN table
                String table = table(id.direction());
                Delete delete = QueryBuilder.delete().from(table);
                for (int i = 0, n = idNames.size(); i < n; i++) {
                    delete.where(formatEQ(idNames.get(i), idValues.get(i)));
                }
                session.add(delete);
            };

            for (String idValue : ids) {
                Id rawId = IdUtil.readString(idValue);
                EdgeId id = EdgeId.parse(rawId.asString()).directed(true);
                assert id.direction() == Directions.OUT;
                deleteEdge.accept(id);
                deleteEdge.accept(id.switchDirection());
            }
        }

        @Override
        public Iterator<BackendEntry> query(
               CassandraSessionPool.Session session, Query query) {
            query = queryByLabelIndex(session, LIDX_TABLE, query);

            if (query == null) {
                return ImmutableList.<BackendEntry>of().iterator();
            }
            return super.query(session, query);
        }

        public static Edge out() {
            return new Edge(Directions.OUT);
        }

        public static Edge in() {
            return new Edge(Directions.IN);
        }
    }

    private static class Schema {

        // Index table name
        private String table;

        public Schema(String table) {
            this.table = table;
        }

        public void createIndex(CassandraSessionPool.Session session) {
            Create create = SchemaBuilder.createTable(this.table)
                                         .ifNotExists();
            create.addPartitionKey(NAME, DataType.text());
            create.addColumn(ELEMENT_IDS, DataType.set(DataType.cint()));
            session.execute(create);
        }

        public void dropTable(CassandraSessionPool.Session session) {
            session.execute(SchemaBuilder.dropTable(this.table).ifExists());
        }

        public void insert(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            Update update = QueryBuilder.update(this.table);

            update.with(QueryBuilder.add(ELEMENT_IDS, entry.id().asLong()));
            update.where(CassandraTable.formatEQ(HugeKeys.NAME,
                                                 entry.column(HugeKeys.NAME)));
            session.add(update);
        }

        public void delete(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry,
                           String mainTable) {
            // Get name from main table by id
            Select select = QueryBuilder.select().from(mainTable);
            select.where(CassandraTable.formatEQ(HugeKeys.ID,
                                                 entry.id().asLong()));
            ResultSet resultSet = session.execute(select);
            List<Row> rows = resultSet.all();
            if (rows.size() == 0) {
                return;
            }

            String name = rows.get(0).getString(HugeKeys.NAME.string());

            Update update = QueryBuilder.update(this.table);
            update.with(QueryBuilder.remove(ELEMENT_IDS, entry.id().asLong()));
            update.where(CassandraTable.formatEQ(HugeKeys.NAME, name));
            session.add(update);
        }

        public Query query(CassandraSessionPool.Session session, Query query) {
            Set<Condition> conditions = query.conditions();

            if (!(query instanceof ConditionQuery) || conditions.isEmpty()) {
                return query;
            }

            ConditionQuery q = ((ConditionQuery) query).copy();
            String name = (String) q.condition(HugeKeys.NAME);
            if (name != null && q.allSysprop() && conditions.size() == 1 &&
                q.containsCondition(HugeKeys.NAME, Condition.RelationType.EQ)) {

                Set<Integer> ids = queryByNameIndex(session, this.table, name);
                if (ids.isEmpty()) {
                    // Not found data with the specified label
                    return null;
                }

                q.resetConditions();
                for (Integer id : ids) {
                    q.query(IdGenerator.of(id));
                }
            }
            return q;
        }

        private static Set<Integer> queryByNameIndex(
                CassandraSessionPool.Session session,
                String table, String name) {

            Select select = QueryBuilder.select().from(table);
            select.where(CassandraTable.formatEQ(HugeKeys.NAME, name));

            try {
                Iterator<Row> it = session.execute(select).iterator();
                if (!it.hasNext()) {
                    return ImmutableSet.of();
                }
                Set<Integer> ids = it.next().getSet(ELEMENT_IDS, Integer.class);
                assert !it.hasNext();
                return ids;
            } catch (DriverException e) {
                throw new BackendException("Failed to query by name '%s'",
                                           e, name);
            }
        }
    }

    public static class VertexLabel extends CassandraTables.VertexLabel {

        private final Schema schema = new Schema("vl_name_index");

        @Override
        protected void createIndex(CassandraSessionPool.Session session,
                                   String indexLabel,
                                   HugeKeys column) {
            this.schema.createIndex(session);
        }

        @Override
        public void dropTable(CassandraSessionPool.Session session) {
            this.schema.dropTable(session);
            super.dropTable(session);
        }

        @Override
        public void insert(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            super.insert(session, entry);
            this.schema.insert(session, entry);
        }

        @Override
        public void delete(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            this.schema.delete(session, entry, this.table());
            super.delete(session, entry);
        }

        @Override
        public Iterator<BackendEntry> query(
               CassandraSessionPool.Session session, Query query) {
            query = this.schema.query(session, query);
            if (query == null) {
                return ImmutableList.<BackendEntry>of().iterator();
            }
            return super.query(session, query);
        }
    }

    public static class EdgeLabel extends CassandraTables.EdgeLabel {

        private final Schema schema = new Schema("el_name_index");

        @Override
        protected void createIndex(CassandraSessionPool.Session session,
                                   String indexLabel,
                                   HugeKeys column) {
            this.schema.createIndex(session);
        }

        @Override
        public void dropTable(CassandraSessionPool.Session session) {
            this.schema.dropTable(session);
            super.dropTable(session);
        }

        @Override
        public void insert(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            super.insert(session, entry);
            this.schema.insert(session, entry);
        }

        @Override
        public void delete(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            this.schema.delete(session, entry, this.table());
            super.delete(session, entry);
        }

        @Override
        public Iterator<BackendEntry> query(
               CassandraSessionPool.Session session, Query query) {
            query = this.schema.query(session, query);
            if (query == null) {
                return ImmutableList.<BackendEntry>of().iterator();
            }
            return super.query(session, query);
        }
    }

    public static class PropertyKey extends CassandraTables.PropertyKey {

        private final Schema schema = new Schema("pk_name_index");

        @Override
        protected void createIndex(CassandraSessionPool.Session session,
                                   String indexLabel,
                                   HugeKeys column) {
            this.schema.createIndex(session);
        }

        @Override
        public void dropTable(CassandraSessionPool.Session session) {
            this.schema.dropTable(session);
            super.dropTable(session);
        }

        @Override
        public void insert(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            super.insert(session, entry);
            this.schema.insert(session, entry);
        }

        @Override
        public void delete(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            this.schema.delete(session, entry, this.table());
            super.delete(session, entry);
        }

        @Override
        public Iterator<BackendEntry> query(
               CassandraSessionPool.Session session, Query query) {
            query = this.schema.query(session, query);
            if (query == null) {
                return ImmutableList.<BackendEntry>of().iterator();
            }
            return super.query(session, query);
        }
    }

    public static class IndexLabel extends CassandraTables.IndexLabel {

        private final Schema schema = new Schema("il_name_index");

        @Override
        protected void createIndex(CassandraSessionPool.Session session,
                                   String indexLabel,
                                   HugeKeys column) {
            this.schema.createIndex(session);
        }

        @Override
        public void dropTable(CassandraSessionPool.Session session) {
            this.schema.dropTable(session);
            super.dropTable(session);
        }

        @Override
        public void insert(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            super.insert(session, entry);
            this.schema.insert(session, entry);
        }

        @Override
        public void delete(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            this.schema.delete(session, entry, this.table());
            super.delete(session, entry);
        }

        @Override
        public Iterator<BackendEntry> query(
               CassandraSessionPool.Session session, Query query) {
            query = this.schema.query(session, query);
            if (query == null) {
                return ImmutableList.<BackendEntry>of().iterator();
            }
            return super.query(session, query);
        }
    }
}
