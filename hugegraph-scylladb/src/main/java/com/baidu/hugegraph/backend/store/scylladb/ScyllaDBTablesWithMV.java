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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.cassandra.CassandraSessionPool;
import com.baidu.hugegraph.backend.store.cassandra.CassandraTable;
import com.baidu.hugegraph.backend.store.cassandra.CassandraTables;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.datastax.driver.core.querybuilder.Select;

public class ScyllaDBTablesWithMV {

    protected static boolean isQueryByLabel(Query query) {
        Set<Condition> conditions = query.conditions();
        if (query instanceof ConditionQuery && !conditions.isEmpty()) {
            ConditionQuery cq = (ConditionQuery) query;
            Id label = (Id) cq.condition(HugeKeys.LABEL);
            if (label != null && cq.allSysprop() &&
                conditions.size() == 1 &&
                cq.containsCondition(HugeKeys.LABEL,
                                     Condition.RelationType.EQ)) {
                return true;
            }
        }
        return false;
    }

    public static class Vertex extends CassandraTables.Vertex {

        private static final String MV_LABEL2VERTEX = "mv_label2vertex";

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
                         MV_LABEL2VERTEX, this.table(), LABEL,
                         LABEL, ID);
            session.execute(cql);
        }

        @Override
        public void dropTable(CassandraSessionPool.Session session) {
            String cql = String.format(
                         "DROP MATERIALIZED VIEW IF EXISTS %s",
                         MV_LABEL2VERTEX);
            session.execute(cql);
            super.dropTable(session);
        }

        /**
         * Query data from label index table if just want to query by label
         */
        @Override
        protected List<Select> query2Select(String table, Query query) {
            if (isQueryByLabel(query)) {
                // Query from materialized view
                return super.query2Select(MV_LABEL2VERTEX, query);
            }
            return super.query2Select(table, query);
        }
    }

    public static class Edge extends CassandraTables.Edge {

        private static final String MV_LABEL2EDGE = "mv_label2edge";

        private final String LABEL = CassandraTable.formatKey(HugeKeys.LABEL);
        private final List<String> KEYS = this.idColumnName().stream()
                                          .filter(k -> k != HugeKeys.LABEL)
                                          .map(k -> CassandraTable.formatKey(k))
                                          .collect(Collectors.toList());
        private final String PRKEYS = this.KEYS.stream()
                                      .collect(Collectors.joining(","));
        private final String PRKEYS_NN = this.KEYS.stream().collect(
                             Collectors.joining(" IS NOT NULL AND "));

        @Override
        protected void createIndex(CassandraSessionPool.Session session,
                                   String indexLabel,
                                   HugeKeys column) {
            String cql = String.format(
                         "CREATE MATERIALIZED VIEW IF NOT EXISTS %s AS " +
                         "  SELECT * FROM %s " +
                         "  WHERE %s IS NOT NULL AND %s IS NOT NULL " +
                         "  PRIMARY KEY(%s, %s)",
                         MV_LABEL2EDGE, this.table(),
                         this.LABEL, this.PRKEYS_NN,
                         this.LABEL, this.PRKEYS);
            session.execute(cql);
        }

        @Override
        public void dropTable(CassandraSessionPool.Session session) {
            String cql = String.format(
                         "DROP MATERIALIZED VIEW IF EXISTS %s",
                         MV_LABEL2EDGE);
            session.execute(cql);
            super.dropTable(session);
        }

        /**
         * Query data from label index table if just want to query by label
         */
        @Override
        protected List<Select> query2Select(String table, Query query) {
            if (isQueryByLabel(query)) {
                // Query from materialized view
                return super.query2Select(MV_LABEL2EDGE, query);
            }
            return super.query2Select(table, query);
        }
    }
}
