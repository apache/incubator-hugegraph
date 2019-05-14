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

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.TableDefine;
import com.baidu.hugegraph.backend.store.mysql.MysqlBackendEntry;
import com.baidu.hugegraph.backend.store.mysql.MysqlSessions.Session;
import com.baidu.hugegraph.backend.store.mysql.MysqlTables;
import com.baidu.hugegraph.backend.store.mysql.MysqlTables.MysqlTableTemplate;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.HugeKeys;

import jersey.repackaged.com.google.common.collect.ImmutableMap;

import static com.baidu.hugegraph.backend.store.mysql.MysqlTables.BOOLEAN;
import static com.baidu.hugegraph.backend.store.mysql.MysqlTables.DOUBLE;
import static com.baidu.hugegraph.backend.store.mysql.MysqlTables.INT;
import static com.baidu.hugegraph.backend.store.mysql.MysqlTables.LARGE_TEXT;
import static com.baidu.hugegraph.backend.store.mysql.MysqlTables.MID_TEXT;
import static com.baidu.hugegraph.backend.store.mysql.MysqlTables.SMALL_TEXT;
import static com.baidu.hugegraph.backend.store.mysql.MysqlTables.TINYINT;

public class PostgresqlTables {

    private static final Map<String, String> TYPES_MAPPING =
            ImmutableMap.<String, String>builder()
                    .put(BOOLEAN, "BOOL")
                    .put(TINYINT, INT)
                    .put(DOUBLE, "FLOAT")
                    .put(SMALL_TEXT, "VARCHAR(255)")
                    .put(MID_TEXT, "VARCHAR(1024)")
                    .put(LARGE_TEXT, "VARCHAR(65533)")
                    .build();

    public static class PostgresqlTableTemplate extends PostgresqlTable {

        protected MysqlTableTemplate template;

        public PostgresqlTableTemplate(MysqlTableTemplate template) {
            super(template.table());
            this.template = template;
        }

        @Override
        public TableDefine tableDefine() {
            return this.template.tableDefine();
        }
    }

    public static class Counters extends PostgresqlTableTemplate {

        public Counters() {
            super(new MysqlTables.Counters(TYPES_MAPPING));
        }

        public long getCounter(Session session, HugeType type) {
            MysqlTables.Counters table = (MysqlTables.Counters) this.template;
            return table.getCounter(session, type);
        }

        public void increaseCounter(Session session, HugeType type,
                                    long increment) {
            String update = String.format(
                            "INSERT INTO %s (%s, %s) VALUES ('%s', %s) " +
                            "ON CONFLICT (%s) DO UPDATE SET ID = %s.ID + %s;",
                            this.table(), formatKey(HugeKeys.SCHEMA_TYPE),
                            formatKey(HugeKeys.ID), type.name(), increment,
                            formatKey(HugeKeys.SCHEMA_TYPE),
                            this.table(), increment);
            try {
                session.execute(update);
            } catch (SQLException e) {
                throw new BackendException(
                          "Failed to update counters with type '%s'", e, type);
            }
        }
    }

    public static class VertexLabel extends PostgresqlTableTemplate {

        public VertexLabel() {
            super(new MysqlTables.VertexLabel(TYPES_MAPPING));
        }
    }

    public static class EdgeLabel extends PostgresqlTableTemplate {

        public EdgeLabel() {
            super(new MysqlTables.EdgeLabel(TYPES_MAPPING));
        }
    }

    public static class PropertyKey extends PostgresqlTableTemplate {

        public PropertyKey() {
            super(new MysqlTables.PropertyKey(TYPES_MAPPING));
        }
    }

    public static class IndexLabel extends PostgresqlTableTemplate {

        public IndexLabel() {
            super(new MysqlTables.IndexLabel(TYPES_MAPPING));
        }
    }

    public static class Vertex extends PostgresqlTableTemplate {

        public static final String TABLE = "vertices";

        public Vertex(String store) {
            super(new MysqlTables.Vertex(store, TYPES_MAPPING));
        }
    }

    public static class Edge extends PostgresqlTableTemplate {

        public Edge(String store, Directions direction) {
            super(new MysqlTables.Edge(store, direction, TYPES_MAPPING));
        }

        @Override
        protected List<Object> idColumnValue(Id id) {
            MysqlTables.Edge table = (MysqlTables.Edge) this.template;
            return table.idColumnValue(id);
        }

        @Override
        public void delete(Session session, MysqlBackendEntry.Row entry) {
            MysqlTables.Edge table = (MysqlTables.Edge) this.template;
            table.delete(session, entry);
        }

        @Override
        protected BackendEntry mergeEntries(BackendEntry e1, BackendEntry e2) {
            MysqlTables.Edge table = (MysqlTables.Edge) this.template;
            return table.mergeEntries(e1, e2);
        }
    }

    public static class SecondaryIndex extends PostgresqlTableTemplate {

        public static final String TABLE = "secondary_indexes";

        public SecondaryIndex(String store) {
            super(new MysqlTables.SecondaryIndex(store, TABLE, TYPES_MAPPING));
        }

        public SecondaryIndex(String store, String table) {
            super(new MysqlTables.SecondaryIndex(store, table, TYPES_MAPPING));
        }

        protected final String entryId(MysqlBackendEntry entry) {
            return ((MysqlTables.SecondaryIndex) this.template).entryId(entry);
        }
    }

    public static class SearchIndex extends SecondaryIndex {

        public static final String TABLE = "search_indexes";

        public SearchIndex(String store) {
            super(store, TABLE);
        }
    }

    public static class RangeIndex extends PostgresqlTableTemplate {

        public RangeIndex(String store) {
            super(new MysqlTables.RangeIndex(store, TYPES_MAPPING));
        }

        protected final String entryId(MysqlBackendEntry entry) {
            return ((MysqlTables.RangeIndex) this.template).entryId(entry);
        }
    }
}
