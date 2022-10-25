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

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.cassandra.CassandraSessionPool;
import com.baidu.hugegraph.backend.store.cassandra.CassandraTable;
import com.baidu.hugegraph.backend.store.cassandra.CassandraTables;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.datastax.driver.core.querybuilder.Select;

public class ScyllaDBTablesWithMV {

    private static boolean isQueryBySpecifiedKey(Query query, HugeKeys key) {
        Collection<Condition> conditions = query.conditions();
        if (query instanceof ConditionQuery && !conditions.isEmpty()) {
            ConditionQuery cq = (ConditionQuery) query;
            Object value = cq.condition(key);
            return value != null && cq.allSysprop() &&
                   conditions.size() == 1 &&
                   cq.containsRelation(key, Condition.RelationType.EQ);
        }
        return false;
    }

    private static boolean isQueryByLabel(Query query) {
        return isQueryBySpecifiedKey(query, HugeKeys.LABEL);
    }

    private static boolean isQueryByName(Query query) {
        return isQueryBySpecifiedKey(query, HugeKeys.NAME);
    }

    private static String mvNameTable(String table) {
        return "mv_name2" + table;
    }

    private static String mvLabelTable(String table) {
        return "mv_label2" + table;
    }

    private static void createSchemaIndexTable(
                        CassandraSessionPool.Session session,
                        String mvName, String table) {
        final String NAME = CassandraTable.formatKey(HugeKeys.NAME);
        final String ID = CassandraTable.formatKey(HugeKeys.ID);
        String cql = String.format(
                     "CREATE MATERIALIZED VIEW IF NOT EXISTS %s AS " +
                     "  SELECT * FROM %s " +
                     "  WHERE %s IS NOT NULL " +
                     "  PRIMARY KEY(%s, %s)",
                     mvName, table, NAME,
                     NAME, ID);
        session.execute(cql);
    }

    private static void dropIndexTable(CassandraSessionPool.Session session,
                                       String mvName) {
        String cql = String.format("DROP MATERIALIZED VIEW IF EXISTS %s",
                                   mvName);
        session.execute(cql);
    }

    public static class PropertyKey extends CassandraTables.PropertyKey {

        private final String mvName2Pk = mvNameTable(this.table());

        @Override
        protected void createIndex(CassandraSessionPool.Session session,
                                   String indexLabel,
                                   HugeKeys column) {
            createSchemaIndexTable(session, mvName2Pk, this.table());
        }

        @Override
        protected void dropTable(CassandraSessionPool.Session session) {
            dropIndexTable(session, mvName2Pk);
            super.dropTable(session);
        }

        @Override
        protected List<Select> query2Select(String table, Query query) {
            if (isQueryByName(query)) {
                // Query from materialized view
                return super.query2Select(mvName2Pk, query);
            }
            return super.query2Select(table, query);
        }
    }

    public static class VertexLabel extends CassandraTables.VertexLabel {

        private final String mvName2Vl = mvNameTable(this.table());

        @Override
        protected void createIndex(CassandraSessionPool.Session session,
                                   String indexLabel,
                                   HugeKeys column) {
            createSchemaIndexTable(session, mvName2Vl, this.table());
        }

        @Override
        protected void dropTable(CassandraSessionPool.Session session) {
            dropIndexTable(session, mvName2Vl);
            super.dropTable(session);
        }

        @Override
        protected List<Select> query2Select(String table, Query query) {
            if (isQueryByName(query)) {
                // Query from materialized view
                return super.query2Select(mvName2Vl, query);
            }
            return super.query2Select(table, query);
        }
    }

    public static class EdgeLabel extends CassandraTables.EdgeLabel {

        private final String mvName2El = mvNameTable(this.table());

        @Override
        protected void createIndex(CassandraSessionPool.Session session,
                                   String indexLabel,
                                   HugeKeys column) {
            createSchemaIndexTable(session, mvName2El, this.table());
        }

        @Override
        protected void dropTable(CassandraSessionPool.Session session) {
            dropIndexTable(session, mvName2El);
            super.dropTable(session);
        }

        @Override
        protected List<Select> query2Select(String table, Query query) {
            if (isQueryByName(query)) {
                // Query from materialized view
                return super.query2Select(mvName2El, query);
            }
            return super.query2Select(table, query);
        }
    }

    public static class IndexLabel extends CassandraTables.IndexLabel {

        private final String mvName2Il = mvNameTable(this.table());

        @Override
        protected void createIndex(CassandraSessionPool.Session session,
                                   String indexLabel,
                                   HugeKeys column) {
            createSchemaIndexTable(session, mvName2Il, this.table());
        }

        @Override
        protected void dropTable(CassandraSessionPool.Session session) {
            dropIndexTable(session, mvName2Il);
            super.dropTable(session);
        }

        @Override
        protected List<Select> query2Select(String table, Query query) {
            if (isQueryByName(query)) {
                // Query from materialized view
                return super.query2Select(mvName2Il, query);
            }
            return super.query2Select(table, query);
        }
    }

    public static class Vertex extends CassandraTables.Vertex {

        private final String mvLabel2Vertex = mvLabelTable(this.table());

        public Vertex(String store) {
            super(store);
        }

        @Override
        protected void createIndex(CassandraSessionPool.Session session,
                                   String indexLabel,
                                   HugeKeys column) {
            final String LABEL = CassandraTable.formatKey(HugeKeys.LABEL);
            final String ID = CassandraTable.formatKey(HugeKeys.ID);
            String cql = String.format(
                         "CREATE MATERIALIZED VIEW IF NOT EXISTS %s AS " +
                         "  SELECT * FROM %s " +
                         "  WHERE %s IS NOT NULL " +
                         "  PRIMARY KEY(%s, %s)",
                         mvLabel2Vertex, this.table(), LABEL, LABEL, ID);
            session.execute(cql);
        }

        @Override
        protected void dropTable(CassandraSessionPool.Session session) {
            dropIndexTable(session, mvLabel2Vertex);
            super.dropTable(session);
        }


        /**
         * Query data from label index table if just want to query by label
         */
        @Override
        protected List<Select> query2Select(String table, Query query) {
            if (isQueryByLabel(query)) {
                // Query from materialized view
                return super.query2Select(mvLabel2Vertex, query);
            }
            return super.query2Select(table, query);
        }
    }

    public static class Edge extends CassandraTables.Edge {

        private final String mvLabel2Edge = mvLabelTable(this.table());

        private static final String LABEL = CassandraTable.formatKey(HugeKeys.LABEL);
        private final List<String> keys = this.idColumnName().stream()
                                          .filter(k -> k != HugeKeys.LABEL)
                                          .map(k -> CassandraTable.formatKey(k))
                                          .collect(Collectors.toList());
        private final String prKeys = this.keys.stream()
                                      .collect(Collectors.joining(","));
        private final String prkeysNn = this.keys.stream().collect(
                             Collectors.joining(" IS NOT NULL AND "));

        public Edge(String store, Directions direction) {
            super(store, direction);
        }

        @Override
        protected void createIndex(CassandraSessionPool.Session session,
                                   String indexLabel,
                                   HugeKeys column) {
            String cql = String.format(
                         "CREATE MATERIALIZED VIEW IF NOT EXISTS %s AS " +
                         "  SELECT * FROM %s " +
                         "  WHERE %s IS NOT NULL AND %s IS NOT NULL " +
                         "  PRIMARY KEY(%s, %s)",
                         mvLabel2Edge, this.table(),
                         this.LABEL, this.prkeysNn,
                         this.LABEL, this.prKeys);
            session.execute(cql);
        }

        @Override
        protected void dropTable(CassandraSessionPool.Session session) {
            dropIndexTable(session, mvLabel2Edge);
            super.dropTable(session);
        }

        /**
         * Query data from label index table if just want to query by label
         */
        @Override
        protected List<Select> query2Select(String table, Query query) {
            if (isQueryByLabel(query)) {
                // Query from materialized view
                return super.query2Select(mvLabel2Edge, query);
            }
            return super.query2Select(table, query);
        }

        @Override
        protected String labelIndexTable() {
            return mvLabel2Edge;
        }

        public static Edge out(String store) {
            return new Edge(store, Directions.OUT);
        }

        public static Edge in(String store) {
            return new Edge(store, Directions.IN);
        }
    }
}
