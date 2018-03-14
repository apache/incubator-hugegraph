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
import com.baidu.hugegraph.backend.store.TableDefine;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;

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

    public static class MysqlTableTemplate extends MysqlTable {

        protected TableDefine define;

        public MysqlTableTemplate(String table) {
            super(table);
        }

        @Override
        public TableDefine tableDefine() {
            return this.define;
        }
    }

    public static class Counters extends MysqlTableTemplate {

        public static final String TABLE = "counters";

        public static final int MAX_TIMES = 10;

        public Counters() {
            super(TABLE);

            this.define = new TableDefine();
            this.define.column(HugeKeys.SCHEMA_TYPE, VARCHAR);
            this.define.column(HugeKeys.ID, INT);
            this.define.keys(HugeKeys.SCHEMA_TYPE);
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

    public static class VertexLabel extends MysqlTableTemplate {

        public static final String TABLE = "vertex_labels";

        public VertexLabel() {
            super(TABLE);

            this.define = new TableDefine();
            this.define.column(HugeKeys.ID, DATATYPE_SL);
            this.define.column(HugeKeys.NAME, VARCHAR);
            this.define.column(HugeKeys.ID_STRATEGY, TINYINT);
            this.define.column(HugeKeys.PRIMARY_KEYS, SMALL_JSON);
            this.define.column(HugeKeys.PROPERTIES, SMALL_JSON);
            this.define.column(HugeKeys.NULLABLE_KEYS, SMALL_JSON);
            this.define.column(HugeKeys.INDEX_LABELS, SMALL_JSON);
            this.define.column(HugeKeys.ENABLE_LABEL_INDEX, BOOLEAN);
            this.define.column(HugeKeys.USER_DATA, LARGE_JSON);
            this.define.keys(HugeKeys.ID);
        }
    }

    public static class EdgeLabel extends MysqlTableTemplate {

        public static final String TABLE = "edge_labels";

        public EdgeLabel() {
            super(TABLE);

            this.define = new TableDefine();
            this.define.column(HugeKeys.ID, DATATYPE_SL);
            this.define.column(HugeKeys.NAME, VARCHAR);
            this.define.column(HugeKeys.FREQUENCY, TINYINT);
            this.define.column(HugeKeys.SOURCE_LABEL, DATATYPE_SL);
            this.define.column(HugeKeys.TARGET_LABEL, DATATYPE_SL);
            this.define.column(HugeKeys.SORT_KEYS, SMALL_JSON);
            this.define.column(HugeKeys.PROPERTIES, SMALL_JSON);
            this.define.column(HugeKeys.NULLABLE_KEYS, SMALL_JSON);
            this.define.column(HugeKeys.INDEX_LABELS, SMALL_JSON);
            this.define.column(HugeKeys.ENABLE_LABEL_INDEX, BOOLEAN);
            this.define.column(HugeKeys.USER_DATA, LARGE_JSON);
            this.define.keys(HugeKeys.ID);
        }
    }

    public static class PropertyKey extends MysqlTableTemplate {

        public static final String TABLE = "property_keys";

        public PropertyKey() {
            super(TABLE);

            this.define = new TableDefine();
            this.define.column(HugeKeys.ID, DATATYPE_PK);
            this.define.column(HugeKeys.NAME, VARCHAR);
            this.define.column(HugeKeys.DATA_TYPE, TINYINT);
            this.define.column(HugeKeys.CARDINALITY, TINYINT);
            this.define.column(HugeKeys.PROPERTIES, SMALL_JSON);
            this.define.column(HugeKeys.USER_DATA, LARGE_JSON);
            this.define.keys(HugeKeys.ID);
        }
    }

    public static class IndexLabel extends MysqlTableTemplate {

        public static final String TABLE = "index_labels";

        public IndexLabel() {
            super(TABLE);

            this.define = new TableDefine();
            this.define.column(HugeKeys.ID, DATATYPE_IL);
            this.define.column(HugeKeys.NAME, VARCHAR);
            this.define.column(HugeKeys.BASE_TYPE, TINYINT);
            this.define.column(HugeKeys.BASE_VALUE, DATATYPE_SL);
            this.define.column(HugeKeys.INDEX_TYPE, TINYINT);
            this.define.column(HugeKeys.FIELDS, SMALL_JSON);
            this.define.keys(HugeKeys.ID);
        }
    }

    public static class Vertex extends MysqlTableTemplate {

        public static final String TABLE = "vertices";

        public Vertex(String store) {
            super(joinTableName(store, TABLE));

            this.define = new TableDefine();
            this.define.column(HugeKeys.ID, VARCHAR);
            this.define.column(HugeKeys.LABEL, DATATYPE_SL);
            this.define.column(HugeKeys.PROPERTIES, LARGE_JSON);
            this.define.keys(HugeKeys.ID);
        }
    }

    public static class Edge extends MysqlTableTemplate {

        public static final String TABLE_PREFIX = "edges";

        private final Directions direction;
        private final String delByLabelTemplate;

        protected Edge(String store, Directions direction) {
            super(joinTableName(store, table(direction)));

            this.direction = direction;
            this.delByLabelTemplate = String.format(
                                      "DELETE FROM %s WHERE %s = ?;",
                                      this.table(), formatKey(HugeKeys.LABEL));

            this.define = new TableDefine();
            this.define.column(HugeKeys.OWNER_VERTEX, VARCHAR);
            this.define.column(HugeKeys.DIRECTION, TINYINT);
            this.define.column(HugeKeys.LABEL, DATATYPE_SL);
            this.define.column(HugeKeys.SORT_VALUES, VARCHAR);
            this.define.column(HugeKeys.OTHER_VERTEX, VARCHAR);
            this.define.column(HugeKeys.PROPERTIES, LARGE_JSON);
            this.define.keys(HugeKeys.OWNER_VERTEX, HugeKeys.DIRECTION,
                             HugeKeys.LABEL, HugeKeys.SORT_VALUES,
                             HugeKeys.OTHER_VERTEX);
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

            E.checkState(edgeId.direction() == this.direction,
                         "Can't query %s edges from %s edges table",
                         edgeId.direction(), this.direction);

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

        private void deleteEdgesByLabel(MysqlSessions.Session session,
                                        Id label) {
            PreparedStatement deleteStmt;
            try {
                // Create or get delete prepare statement
                deleteStmt = session.prepareStatement(this.delByLabelTemplate);
                // Delete edges
                deleteStmt.setObject(1, label.asLong());
            } catch (SQLException e) {
                throw new BackendException("Failed to prepare statement '%s'",
                                           this.delByLabelTemplate);
            }
            session.add(deleteStmt);
        }

        @Override
        protected BackendEntry mergeEntries(BackendEntry e1, BackendEntry e2) {
            // Merge edges into vertex
            // TODO: merge rows before calling row2Entry()

            MysqlBackendEntry current = (MysqlBackendEntry) e1;
            MysqlBackendEntry next = (MysqlBackendEntry) e2;

            E.checkState(current == null || current.type().isVertex(),
                         "The current entry must be null or VERTEX");
            E.checkState(next != null && next.type().isEdge(),
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
            assert edge.type().isEdge();
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

        public static String table(Directions direction) {
            assert direction == Directions.OUT || direction == Directions.IN;
            return TABLE_PREFIX + "_" + direction.string();
        }

        public static MysqlTable out(String store) {
            return new Edge(store, Directions.OUT);
        }

        public static MysqlTable in(String store) {
            return new Edge(store, Directions.IN);
        }
    }

    public abstract static class Index extends MysqlTableTemplate {

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

        public SecondaryIndex(String store) {
            super(joinTableName(store, TABLE));

            this.define = new TableDefine();
            this.define.column(HugeKeys.FIELD_VALUES, VARCHAR);
            this.define.column(HugeKeys.INDEX_LABEL_ID, DATATYPE_IL);
            this.define.column(HugeKeys.ELEMENT_IDS, VARCHAR);
            this.define.keys(HugeKeys.FIELD_VALUES,
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

        public RangeIndex(String store) {
            super(joinTableName(store, TABLE));

            this.define = new TableDefine();
            this.define.column(HugeKeys.INDEX_LABEL_ID, DATATYPE_IL);
            this.define.column(HugeKeys.FIELD_VALUES, DOUBLE);
            this.define.column(HugeKeys.ELEMENT_IDS, VARCHAR);
            this.define.keys(HugeKeys.INDEX_LABEL_ID,
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
