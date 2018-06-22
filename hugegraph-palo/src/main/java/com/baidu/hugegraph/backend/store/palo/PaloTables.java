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

package com.baidu.hugegraph.backend.store.palo;

import java.sql.PreparedStatement;
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
import com.baidu.hugegraph.backend.store.mysql.MysqlBackendEntry;
import com.baidu.hugegraph.backend.store.mysql.MysqlSessions;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;

public class PaloTables {

    private static final String NOT_NULL = "NOT NULL";
    private static final String DEFAULT_EMPTY = "DEFAULT ''";

    private static final String DATATYPE_PK = "INT";
    private static final String DATATYPE_SL = "INT"; // VL/EL
    private static final String DATATYPE_IL = "INT";

    private static final String TINYINT = "TINYINT";
    private static final String INT = "INT";
    private static final String DECIMAL = "DECIMAL(27, 9)";
    private static final String VARCHAR = "VARCHAR(255)";
    private static final String TEXT = "VARCHAR(65533)";

    public static class PaloTableTemplate extends PaloTable {

        protected TableDefine define;

        public PaloTableTemplate(String table) {
            super(table);
        }

        @Override
        public TableDefine tableDefine() {
            return this.define;
        }
    }

    public static class VertexLabel extends PaloTableTemplate {

        public static final String TABLE = "vertex_labels";

        public VertexLabel() {
            super(TABLE);
            this.define = new TableDefine();
            this.define.column(HugeKeys.ID, DATATYPE_SL, NOT_NULL);
            this.define.column(HugeKeys.NAME, VARCHAR, NOT_NULL);
            this.define.column(HugeKeys.ID_STRATEGY, TINYINT, NOT_NULL);
            this.define.column(HugeKeys.PRIMARY_KEYS, VARCHAR, DEFAULT_EMPTY);
            this.define.column(HugeKeys.PROPERTIES, VARCHAR, DEFAULT_EMPTY);
            this.define.column(HugeKeys.NULLABLE_KEYS, VARCHAR, DEFAULT_EMPTY);
            this.define.column(HugeKeys.INDEX_LABELS, VARCHAR, DEFAULT_EMPTY);
            this.define.column(HugeKeys.ENABLE_LABEL_INDEX, TINYINT, NOT_NULL);
            this.define.column(HugeKeys.USER_DATA, VARCHAR, DEFAULT_EMPTY);
            this.define.column(HugeKeys.STATUS, TINYINT, NOT_NULL);
            // Unique keys/hash keys
            this.define.keys(HugeKeys.ID);
        }
    }

    public static class EdgeLabel extends PaloTableTemplate {

        public static final String TABLE = "edge_labels";

        public EdgeLabel() {
            super(TABLE);
            this.define = new TableDefine();
            this.define.column(HugeKeys.ID, DATATYPE_SL, NOT_NULL);
            this.define.column(HugeKeys.NAME, VARCHAR, NOT_NULL);
            this.define.column(HugeKeys.FREQUENCY, TINYINT, NOT_NULL);
            this.define.column(HugeKeys.SOURCE_LABEL, INT, NOT_NULL);
            this.define.column(HugeKeys.TARGET_LABEL, INT, NOT_NULL);
            this.define.column(HugeKeys.SORT_KEYS, VARCHAR, DEFAULT_EMPTY);
            this.define.column(HugeKeys.PROPERTIES, VARCHAR, DEFAULT_EMPTY);
            this.define.column(HugeKeys.NULLABLE_KEYS, VARCHAR, DEFAULT_EMPTY);
            this.define.column(HugeKeys.INDEX_LABELS, VARCHAR, DEFAULT_EMPTY);
            this.define.column(HugeKeys.ENABLE_LABEL_INDEX, TINYINT, NOT_NULL);
            this.define.column(HugeKeys.USER_DATA, VARCHAR, DEFAULT_EMPTY);
            this.define.column(HugeKeys.STATUS, TINYINT, NOT_NULL);
            // Unique keys/hash keys
            this.define.keys(HugeKeys.ID);
        }
    }

    public static class PropertyKey extends PaloTableTemplate {

        public static final String TABLE = "property_keys";

        public PropertyKey() {
            super(TABLE);
            this.define = new TableDefine();
            this.define.column(HugeKeys.ID, DATATYPE_PK, NOT_NULL);
            this.define.column(HugeKeys.NAME, VARCHAR, NOT_NULL);
            this.define.column(HugeKeys.DATA_TYPE, TINYINT, NOT_NULL);
            this.define.column(HugeKeys.CARDINALITY, TINYINT, NOT_NULL);
            this.define.column(HugeKeys.PROPERTIES, VARCHAR, DEFAULT_EMPTY);
            this.define.column(HugeKeys.USER_DATA, VARCHAR, DEFAULT_EMPTY);
            this.define.column(HugeKeys.STATUS, TINYINT, NOT_NULL);
            // Unique keys/hash keys
            this.define.keys(HugeKeys.ID);
        }
    }

    public static class IndexLabel extends PaloTableTemplate {

        public static final String TABLE = "index_labels";

        public IndexLabel() {
            super(TABLE);
            this.define = new TableDefine();
            this.define.column(HugeKeys.ID, DATATYPE_IL, NOT_NULL);
            this.define.column(HugeKeys.NAME, VARCHAR, NOT_NULL);
            this.define.column(HugeKeys.BASE_TYPE, TINYINT, NOT_NULL);
            this.define.column(HugeKeys.BASE_VALUE, INT, NOT_NULL);
            this.define.column(HugeKeys.INDEX_TYPE, TINYINT, NOT_NULL);
            this.define.column(HugeKeys.FIELDS, VARCHAR, NOT_NULL);
            this.define.column(HugeKeys.STATUS, TINYINT, NOT_NULL);
            // Unique keys/hash keys
            this.define.keys(HugeKeys.ID);
        }
    }

    public static class Vertex extends PaloTableTemplate {

        public static final String TABLE = "vertices";

        public Vertex() {
            super(TABLE);
            this.define = new TableDefine();
            this.define.column(HugeKeys.ID, VARCHAR, NOT_NULL);
            this.define.column(HugeKeys.LABEL, INT, NOT_NULL);
            this.define.column(HugeKeys.PROPERTIES, TEXT, DEFAULT_EMPTY);
            // Unique keys/hash keys
            this.define.keys(HugeKeys.ID);
        }
    }

    /**
     * TODO: How to let Edge extends from PaloTable and MysqlTables.Edge?
     */
    public static class Edge extends PaloTableTemplate {

        public static final String TABLE_PREFIX = "edges";

        private final Directions direction;
        private final String delByLabelTemplate;

        public Edge(Directions direction) {
            super(TABLE_PREFIX + "_" + direction.string());
            assert direction == Directions.OUT || direction == Directions.IN;
            this.direction = direction;
            this.delByLabelTemplate = String.format(
                                      "DELETE FROM %s PARTITION %s WHERE %s = ?;",
                                      this.table(), this.table(),
                                      formatKey(HugeKeys.LABEL));

            this.define = new TableDefine();
            this.define.column(HugeKeys.OWNER_VERTEX, VARCHAR, NOT_NULL);
            this.define.column(HugeKeys.DIRECTION, TINYINT, NOT_NULL);
            this.define.column(HugeKeys.LABEL, INT, NOT_NULL);
            this.define.column(HugeKeys.SORT_VALUES, VARCHAR, NOT_NULL,
                               DEFAULT_EMPTY);
            this.define.column(HugeKeys.OTHER_VERTEX, VARCHAR, NOT_NULL);
            this.define.column(HugeKeys.PROPERTIES, TEXT, DEFAULT_EMPTY);
            // Unique keys/hash keys
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
    }

    public abstract static class Index extends PaloTableTemplate {

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
            this.define = new TableDefine();
            this.define.column(HugeKeys.FIELD_VALUES, VARCHAR, NOT_NULL);
            this.define.column(HugeKeys.INDEX_LABEL_ID, INT, NOT_NULL);
            this.define.column(HugeKeys.ELEMENT_IDS, VARCHAR, NOT_NULL);
            // Unique keys/hash keys
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

        public RangeIndex() {
            super(TABLE);
            this.define = new TableDefine();
            this.define.column(HugeKeys.INDEX_LABEL_ID, INT, NOT_NULL);
            this.define.column(HugeKeys.FIELD_VALUES, DECIMAL, NOT_NULL);
            this.define.column(HugeKeys.ELEMENT_IDS, VARCHAR, NOT_NULL);
            // Unique keys/hash keys
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
