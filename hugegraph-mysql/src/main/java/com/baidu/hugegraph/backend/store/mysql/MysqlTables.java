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
import java.util.Arrays;
import java.util.List;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.id.IdUtil;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class MysqlTables {

    private static final String INT = "INT";

    private static final String DATATYPE_PK = "INT";
    private static final String DATATYPE_SL = "INT"; // VL/EL
    private static final String DATATYPE_IL = "INT";

    private static final String BOOLEAN = "BOOLEAN";
    private static final String TINYINT = "TINYINT";
    private static final String DOUBLE = "DOUBLE";
    private static final String VARCHAR = "VARCHAR(255)";
    private static final String SMALL_JSON = "VARCHAR(1024)";
    private static final String LARGE_JSON = "TEXT";

    public static class Counters extends MysqlTable {

        public static final String TABLE = "counters";

        public static final int MAX_TIMES = 10;

        public Counters() {
            super(TABLE);
        }

        @Override
        public void init(MysqlSessions.Session session) {
            ImmutableMap<HugeKeys, String> columns = ImmutableMap.of(
                    HugeKeys.SCHEMA_TYPE, VARCHAR,
                    HugeKeys.ID, INT
            );
            ImmutableSet<HugeKeys> pkeys = ImmutableSet.of(HugeKeys.SCHEMA_TYPE);
            this.createTable(session, columns, pkeys);
        }

        public synchronized Id nextId(MysqlSessions.Session session,
                                      HugeType type) {

            String schemaCol = formatKey(HugeKeys.SCHEMA_TYPE);
            String idCol = formatKey(HugeKeys.ID);

            String select = String.format("SELECT ID FROM %s WHERE %s = '%s';",
                                          TABLE, schemaCol, type.name());
            /*
             * If the current schema record does not exist then insert,
             * otherwise update it.
             */
            String update = String.format("INSERT INTO %s VALUES ('%s', 1) " +
                                          "ON DUPLICATE KEY UPDATE " +
                                          "ID = ID + 1;", TABLE, type.name());

            // Do get-increase-get-compare operation
            long counter = 0L;
            long expect = -1L;

            for (int i = 0; i < MAX_TIMES; i++) {
                try {
                    ResultSet resultSet = session.select(select);
                    if (resultSet.next()) {
                        counter = resultSet.getLong(idCol);
                    }
                } catch (SQLException e) {
                    throw new BackendException("Failed to get id from " +
                                               "counters with schema type '%s'",
                                               e, type);
                }

                if (counter == expect) {
                    break;
                }
                // Increase local counter
                expect = counter + 1L;
                // Increase remote counter
                try {
                    session.execute(update);
                } catch (SQLException e) {
                    throw new BackendException("Failed to update counter " +
                                               "for schema type '%s'", e, type);
                }
            }

            E.checkState(counter != 0L, "Please check whether MySQL is OK");
            E.checkState(counter == expect, "MySQL is busy please try again");
            return IdGenerator.of(expect);
        }
    }

    public static class VertexLabel extends MysqlTable {

        public static final String TABLE = "vertex_labels";

        public VertexLabel() {
            super(TABLE);
        }

        @Override
        public void init(MysqlSessions.Session session) {
            ImmutableMap<HugeKeys, String> columns = ImmutableMap
                    .<HugeKeys, String>builder()
                    .put(HugeKeys.ID, DATATYPE_SL)
                    .put(HugeKeys.NAME, VARCHAR)
                    .put(HugeKeys.ID_STRATEGY, TINYINT)
                    .put(HugeKeys.PRIMARY_KEYS, SMALL_JSON)
                    .put(HugeKeys.NULLABLE_KEYS, SMALL_JSON)
                    .put(HugeKeys.INDEX_LABELS, SMALL_JSON)
                    .put(HugeKeys.PROPERTIES, SMALL_JSON)
                    .put(HugeKeys.ENABLE_LABEL_INDEX, BOOLEAN)
                    .put(HugeKeys.USER_DATA, LARGE_JSON)
                    .build();
            ImmutableSet<HugeKeys> pkeys = ImmutableSet.of(HugeKeys.ID);
            this.createTable(session, columns, pkeys);
        }
    }

    public static class EdgeLabel extends MysqlTable {

        public static final String TABLE = "edge_labels";

        public EdgeLabel() {
            super(TABLE);
        }

        @Override
        public void init(MysqlSessions.Session session) {
            ImmutableMap<HugeKeys, String> columns = ImmutableMap
                    .<HugeKeys, String>builder()
                    .put(HugeKeys.ID, DATATYPE_SL)
                    .put(HugeKeys.NAME, VARCHAR)
                    .put(HugeKeys.FREQUENCY, TINYINT)
                    .put(HugeKeys.SOURCE_LABEL, DATATYPE_SL)
                    .put(HugeKeys.TARGET_LABEL, DATATYPE_SL)
                    .put(HugeKeys.SORT_KEYS, SMALL_JSON)
                    .put(HugeKeys.NULLABLE_KEYS, SMALL_JSON)
                    .put(HugeKeys.INDEX_LABELS, SMALL_JSON)
                    .put(HugeKeys.PROPERTIES, SMALL_JSON)
                    .put(HugeKeys.ENABLE_LABEL_INDEX, BOOLEAN)
                    .put(HugeKeys.USER_DATA, LARGE_JSON)
                    .build();
            ImmutableSet<HugeKeys> pkeys = ImmutableSet.of(HugeKeys.ID);
            this.createTable(session, columns, pkeys);
        }
    }

    public static class PropertyKey extends MysqlTable {

        public static final String TABLE = "property_keys";

        public PropertyKey() {
            super(TABLE);
        }

        @Override
        public void init(MysqlSessions.Session session) {
            ImmutableMap<HugeKeys, String> columns = ImmutableMap
                    .<HugeKeys, String>builder()
                    .put(HugeKeys.ID, DATATYPE_PK)
                    .put(HugeKeys.NAME, VARCHAR)
                    .put(HugeKeys.DATA_TYPE, TINYINT)
                    .put(HugeKeys.CARDINALITY, TINYINT)
                    .put(HugeKeys.PROPERTIES, SMALL_JSON)
                    .put(HugeKeys.USER_DATA, LARGE_JSON)
                    .build();
            ImmutableSet<HugeKeys> pkeys = ImmutableSet.of(HugeKeys.ID);
            this.createTable(session, columns, pkeys);
        }
    }

    public static class IndexLabel extends MysqlTable {

        public static final String TABLE = "index_labels";

        public IndexLabel() {
            super(TABLE);
        }

        @Override
        public void init(MysqlSessions.Session session) {
            ImmutableMap<HugeKeys, String> columns = ImmutableMap
                    .<HugeKeys, String>builder()
                    .put(HugeKeys.ID, DATATYPE_IL)
                    .put(HugeKeys.BASE_TYPE, TINYINT)
                    .put(HugeKeys.BASE_VALUE, DATATYPE_SL)
                    .put(HugeKeys.NAME, VARCHAR)
                    .put(HugeKeys.INDEX_TYPE, TINYINT)
                    .put(HugeKeys.FIELDS, SMALL_JSON)
                    .build();
            ImmutableSet<HugeKeys> pkeys = ImmutableSet.of(HugeKeys.ID);
            this.createTable(session, columns, pkeys);
        }
    }

    public static class Vertex extends MysqlTable {

        public static final String TABLE = "vertices";

        public Vertex() {
            super(TABLE);
        }

        @Override
        public void init(MysqlSessions.Session session) {
            ImmutableMap<HugeKeys, String> columns = ImmutableMap.of(
                    HugeKeys.ID, VARCHAR,
                    HugeKeys.LABEL, DATATYPE_SL,
                    HugeKeys.PROPERTIES, LARGE_JSON
            );
            ImmutableSet<HugeKeys> pkeys = ImmutableSet.of(HugeKeys.ID);
            this.createTable(session, columns, pkeys);
        }
    }

    public static class Edge extends MysqlTable {

        public static final String TABLE = "edges";

        private String deleteByLabelTemplate;

        public Edge() {
            super(TABLE);
            this.deleteByLabelTemplate = String.format(
                                         "DELETE FROM %s WHERE %s = ?;",
                                         TABLE, formatKey(HugeKeys.LABEL));
        }

        @Override
        public void init(MysqlSessions.Session session) {
            ImmutableMap<HugeKeys, String> columns = ImmutableMap
                    .<HugeKeys, String>builder()
                    .put(HugeKeys.OWNER_VERTEX, VARCHAR)
                    .put(HugeKeys.DIRECTION, TINYINT)
                    .put(HugeKeys.LABEL, DATATYPE_SL)
                    .put(HugeKeys.SORT_VALUES, VARCHAR)
                    .put(HugeKeys.OTHER_VERTEX, VARCHAR)
                    .put(HugeKeys.PROPERTIES, LARGE_JSON)
                    .build();
            ImmutableSet<HugeKeys> pkeys = ImmutableSet.of(
                    HugeKeys.OWNER_VERTEX,
                    HugeKeys.DIRECTION,
                    HugeKeys.LABEL,
                    HugeKeys.SORT_VALUES,
                    HugeKeys.OTHER_VERTEX
            );
            this.createTable(session, columns, pkeys);
        }

        @Override
        protected List<HugeKeys> idColumnName() {
            return Arrays.asList(EdgeId.KEYS);
        }

        @Override
        protected List<Object> idColumnValue(Id id) {
            EdgeId edgeId;
            if (!(id instanceof EdgeId)) {
                String[] idParts = EdgeId.split(id);
                if (idParts.length == 1) {
                    // Delete edge by label
                    return Arrays.asList((Object[]) idParts);
                }
                id = IdUtil.readString(id.asString());
                edgeId = EdgeId.parse(id.asString());
            } else {
                edgeId = (EdgeId) id;
            }

            List<Object> list = new ArrayList<>(5);
            list.add(IdUtil.writeString(edgeId.ownerVertexId()));
            list.add(edgeId.direction().code());
            list.add(edgeId.edgeLabelId().asLong());
            list.add(edgeId.sortValues());
            list.add(IdUtil.writeString(edgeId.otherVertexId()));
            return list;
        }

        @Override
        public void delete(MysqlSessions.Session session,
                           MysqlBackendEntry.Row entry) {
            // Let super class do delete if not deleting edge by label
            List<Object> idParts = this.idColumnValue(entry.id());
            if (idParts.size() > 1 || entry.columns().size() > 0) {
                super.delete(session, entry);
                return;
            }

            // The only element is label
            this.deleteEdgesByLabel(session, entry.id());
        }

        protected void deleteEdgesByLabel(MysqlSessions.Session session,
                                          Id label) {
            PreparedStatement deleteStmt;
            try {
                // Create or get insert prepare statement
                deleteStmt = session.prepareStatement(
                                     this.deleteByLabelTemplate);
                // Delete edges
                deleteStmt.setObject(1, label.asLong());
            } catch (SQLException e) {
                throw new BackendException("Failed to prepare statment '%s'",
                                           this.deleteByLabelTemplate);
            }
            session.add(deleteStmt);
        }

        @Override
        protected BackendEntry mergeEntries(BackendEntry e1, BackendEntry e2) {
            // Merge edges into vertex
            // TODO: merge rows before calling row2Entry()

            MysqlBackendEntry current = (MysqlBackendEntry) e1;
            MysqlBackendEntry next = (MysqlBackendEntry) e2;

            E.checkState(current == null || current.type() == HugeType.VERTEX,
                         "The current entry must be null or VERTEX");
            E.checkState(next != null && next.type() == HugeType.EDGE,
                         "The next entry must be EDGE");

            if (current != null) {
                Id nextVertexId = IdGenerator.of(
                                  next.<String>column(HugeKeys.OWNER_VERTEX));
                if (current.id().equals(nextVertexId)) {
                    current.subRow(next.row());
                    return current;
                }
            }

            return this.wrapByVertex(next);
        }

        private MysqlBackendEntry wrapByVertex(MysqlBackendEntry edge) {
            assert edge.type() == HugeType.EDGE;
            String ownerVertex = edge.column(HugeKeys.OWNER_VERTEX);
            E.checkState(ownerVertex != null, "Invalid backend entry");
            Id vertexId = IdGenerator.of(ownerVertex);
            MysqlBackendEntry vertex = new MysqlBackendEntry(HugeType.VERTEX,
                                                             vertexId);

            vertex.column(HugeKeys.ID, ownerVertex);
            vertex.column(HugeKeys.PROPERTIES, "");

            vertex.subRow(edge.row());
            return vertex;
        }
    }

    public static abstract class Index extends MysqlTable {

        public Index(String table) {
            super(table);
        }

        @Override
        protected BackendEntry mergeEntries(BackendEntry e1, BackendEntry e2) {
            MysqlBackendEntry current = (MysqlBackendEntry) e1;
            MysqlBackendEntry next = (MysqlBackendEntry) e2;

            E.checkState(current == null || current.type().isIndex(),
                         "The current entry must be null or INDEX");
            E.checkState(next != null && next.type().isIndex(),
                         "The next entry must be INDEX");

            if (current != null) {
                String currentId = this.entryId(current);
                String nextId = this.entryId(next);
                if (currentId.equals(nextId)) {
                    current.subRow(next.row());
                    return current;
                }
            }
            return next;
        }

        protected abstract String entryId(MysqlBackendEntry entry);
    }

    public static class SecondaryIndex extends Index {

        public static final String TABLE = "secondary_indexes";

        public SecondaryIndex() {
            super(TABLE);
        }

        @Override
        public void init(MysqlSessions.Session session) {
            ImmutableMap<HugeKeys, String> columns = ImmutableMap.of(
                    HugeKeys.FIELD_VALUES, VARCHAR,
                    HugeKeys.INDEX_LABEL_ID, DATATYPE_IL,
                    HugeKeys.ELEMENT_IDS, VARCHAR
            );
            ImmutableSet<HugeKeys> pkeys = ImmutableSet.of(
                    HugeKeys.FIELD_VALUES,
                    HugeKeys.INDEX_LABEL_ID,
                    HugeKeys.ELEMENT_IDS
            );
            this.createTable(session, columns, pkeys);
        }

        @Override
        protected List<HugeKeys> idColumnName() {
            return ImmutableList.of(HugeKeys.FIELD_VALUES,
                                    HugeKeys.INDEX_LABEL_ID,
                                    HugeKeys.ELEMENT_IDS);
        }

        @Override
        protected final String entryId(MysqlBackendEntry entry) {
            String fieldValues = entry.column(HugeKeys.FIELD_VALUES);
            Integer labelId = entry.column(HugeKeys.INDEX_LABEL_ID);
            return SplicingIdGenerator.concat(fieldValues, labelId.toString());
        }
    }

    public static class RangeIndex extends Index {

        public static final String TABLE = "range_indexes";

        public RangeIndex() {
            super(TABLE);
        }

        @Override
        public void init(MysqlSessions.Session session) {
            ImmutableMap<HugeKeys, String> columns = ImmutableMap.of(
                    HugeKeys.INDEX_LABEL_ID, DATATYPE_IL,
                    HugeKeys.FIELD_VALUES, DOUBLE,
                    HugeKeys.ELEMENT_IDS, VARCHAR
            );
            ImmutableSet<HugeKeys> pkeys = ImmutableSet.of(
                    HugeKeys.INDEX_LABEL_ID,
                    HugeKeys.FIELD_VALUES,
                    HugeKeys.ELEMENT_IDS
            );
            this.createTable(session, columns, pkeys);
        }

        @Override
        protected List<HugeKeys> idColumnName() {
            return ImmutableList.of(HugeKeys.INDEX_LABEL_ID,
                                    HugeKeys.FIELD_VALUES,
                                    HugeKeys.ELEMENT_IDS);
        }

        @Override
        protected final String entryId(MysqlBackendEntry entry) {
            Double fieldValue = entry.<Double>column(HugeKeys.FIELD_VALUES);
            Integer labelId = entry.column(HugeKeys.INDEX_LABEL_ID);
            return SplicingIdGenerator.concat(labelId.toString(),
                                              fieldValue.toString());
        }
    }
}
