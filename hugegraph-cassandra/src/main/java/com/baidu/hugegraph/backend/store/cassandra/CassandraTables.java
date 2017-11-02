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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class CassandraTables {

    private static final DataType DATATYPE_PK = DataType.cint();
    private static final DataType DATATYPE_SL = DataType.cint(); // VL/EL
    private static final DataType DATATYPE_IL = DataType.cint();

    private static final DataType DATATYPE_UD = DataType.map(DataType.text(),
                                                             DataType.text());

    public static class Counters extends CassandraTable {

        public static final String TABLE = "counters";

        public static final int MAX_TIMES = 1000;

        public Counters() {
            super(TABLE);
        }

        @Override
        public void init(CassandraSessionPool.Session session) {
            ImmutableMap<HugeKeys, DataType> pkeys = ImmutableMap.of(
                    HugeKeys.SCHEMA_TYPE, DataType.text()
            );
            ImmutableMap<HugeKeys, DataType> ckeys = ImmutableMap.of();
            ImmutableMap<HugeKeys, DataType> columns = ImmutableMap.of(
                    HugeKeys.ID, DataType.counter()
            );

            this.createTable(session, pkeys, ckeys, columns);
        }

        public synchronized Id nextId(CassandraSessionPool.Session session,
                                      HugeType type) {
            Clause where = formatEQ(HugeKeys.SCHEMA_TYPE, type.name());
            Select select = QueryBuilder.select(formatKey(HugeKeys.ID))
                                        .from(TABLE);
            select.where(where);

            Update update = QueryBuilder.update(TABLE);
            // Id increase 1
            update.with(QueryBuilder.incr(formatKey(HugeKeys.ID), 1));
            update.where(where);

            // Do get-increase-get-compare operation
            long counter = 0L;
            long expect = -1L;

            for (int i = 0; i < MAX_TIMES; i++) {
                Row row = session.execute(select).one();
                if (row != null) {
                    counter = row.getLong(formatKey(HugeKeys.ID));
                }
                if (counter == expect) {
                    break;
                }
                // Increase local counter
                expect = counter + 1;
                // Increase remote counter
                session.execute(update);
            }

            E.checkState(counter != 0, "Please check whether Cassandra is OK");
            E.checkState(counter == expect,
                         "Cassandra is busy please try again");
            return IdGenerator.of(expect);
        }
    }

    public static class VertexLabel extends CassandraTable {

        public static final String TABLE = "vertex_labels";

        public VertexLabel() {
            super(TABLE);
        }

        @Override
        public void init(CassandraSessionPool.Session session) {
            ImmutableMap<HugeKeys, DataType> pkeys = ImmutableMap.of(
                    HugeKeys.ID, DATATYPE_SL
            );
            ImmutableMap<HugeKeys, DataType> ckeys = ImmutableMap.of();
            ImmutableMap<HugeKeys, DataType> columns = ImmutableMap
                    .<HugeKeys, DataType>builder()
                    .put(HugeKeys.NAME, DataType.text())
                    .put(HugeKeys.ID_STRATEGY, DataType.tinyint())
                    .put(HugeKeys.PRIMARY_KEYS, DataType.list(DATATYPE_PK))
                    .put(HugeKeys.NULLABLE_KEYS, DataType.set(DATATYPE_PK))
                    .put(HugeKeys.INDEX_LABELS, DataType.set(DATATYPE_IL))
                    .put(HugeKeys.PROPERTIES, DataType.set(DATATYPE_PK))
                    .put(HugeKeys.USER_DATA, DATATYPE_UD)
                    .build();

            this.createTable(session, pkeys, ckeys, columns);
            this.createIndex(session, "vertex_label_name_index",
                             HugeKeys.NAME);
        }
    }

    public static class EdgeLabel extends CassandraTable {

        public static final String TABLE = "edge_labels";

        public EdgeLabel() {
            super(TABLE);
        }

        @Override
        public void init(CassandraSessionPool.Session session) {
            ImmutableMap<HugeKeys, DataType> pkeys = ImmutableMap.of(
                    HugeKeys.ID, DATATYPE_SL
            );
            ImmutableMap<HugeKeys, DataType> ckeys = ImmutableMap.of();
            ImmutableMap<HugeKeys, DataType> columns = ImmutableMap
                    .<HugeKeys, DataType>builder()
                    .put(HugeKeys.NAME, DataType.text())
                    .put(HugeKeys.FREQUENCY, DataType.tinyint())
                    .put(HugeKeys.SOURCE_LABEL, DATATYPE_SL)
                    .put(HugeKeys.TARGET_LABEL, DATATYPE_SL)
                    .put(HugeKeys.SORT_KEYS, DataType.list(DATATYPE_PK))
                    .put(HugeKeys.NULLABLE_KEYS, DataType.set(DATATYPE_PK))
                    .put(HugeKeys.INDEX_LABELS, DataType.set(DATATYPE_IL))
                    .put(HugeKeys.PROPERTIES, DataType.set(DATATYPE_PK))
                    .put(HugeKeys.USER_DATA, DATATYPE_UD)
                    .build();

            this.createTable(session, pkeys, ckeys, columns);
            this.createIndex(session, "edge_label_name_index", HugeKeys.NAME);
        }
    }

    public static class PropertyKey extends CassandraTable {

        public static final String TABLE = "property_keys";

        public PropertyKey() {
            super(TABLE);
        }

        @Override
        public void init(CassandraSessionPool.Session session) {
            ImmutableMap<HugeKeys, DataType> pkeys = ImmutableMap.of(
                    HugeKeys.ID, DataType.cint()
            );
            ImmutableMap<HugeKeys, DataType> ckeys = ImmutableMap.of();
            ImmutableMap<HugeKeys, DataType> columns = ImmutableMap.of(
                    HugeKeys.NAME, DataType.text(),
                    HugeKeys.DATA_TYPE, DataType.tinyint(),
                    HugeKeys.CARDINALITY, DataType.tinyint(),
                    HugeKeys.PROPERTIES, DataType.set(DATATYPE_PK),
                    HugeKeys.USER_DATA, DATATYPE_UD
            );

            this.createTable(session, pkeys, ckeys, columns);
            this.createIndex(session, "property_key_name_index",
                             HugeKeys.NAME);
        }
    }

    public static class IndexLabel extends CassandraTable {

        public static final String TABLE = "index_labels";

        public IndexLabel() {
            super(TABLE);
        }

        @Override
        public void init(CassandraSessionPool.Session session) {
            ImmutableMap<HugeKeys, DataType> pkeys = ImmutableMap.of(
                    HugeKeys.ID, DATATYPE_IL
            );
            ImmutableMap<HugeKeys, DataType> ckeys = ImmutableMap.of(
                    HugeKeys.BASE_TYPE, DataType.tinyint(),
                    HugeKeys.BASE_VALUE, DATATYPE_SL
            );
            ImmutableMap<HugeKeys, DataType> columns = ImmutableMap.of(
                    HugeKeys.NAME, DataType.text(),
                    HugeKeys.INDEX_TYPE, DataType.tinyint(),
                    HugeKeys.FIELDS, DataType.list(DATATYPE_PK)
            );

            this.createTable(session, pkeys, ckeys, columns);
            this.createIndex(session, "index_label_name_index", HugeKeys.NAME);
        }
    }

    public static class Vertex extends CassandraTable {

        public static final String TABLE = "vertices";

        public Vertex() {
            super(TABLE);
        }

        @Override
        public void init(CassandraSessionPool.Session session) {
            ImmutableMap<HugeKeys, DataType> pkeys = ImmutableMap.of(
                    HugeKeys.ID, DataType.text()
            );
            ImmutableMap<HugeKeys, DataType> ckeys = ImmutableMap.of();
            ImmutableMap<HugeKeys, DataType> columns = ImmutableMap.of(
                    HugeKeys.LABEL, DATATYPE_SL,
                    HugeKeys.PROPERTIES, DataType.map(DATATYPE_PK,
                                                      DataType.text())
            );

            this.createTable(session, pkeys, ckeys, columns);
            this.createIndex(session, "vertex_label_index", HugeKeys.LABEL);
        }
    }

    public static class Edge extends CassandraTable {

        public static final String TABLE = "edges";

        private static final HugeKeys[] KEYS = new HugeKeys[] {
                HugeKeys.OWNER_VERTEX,
                HugeKeys.DIRECTION,
                HugeKeys.LABEL,
                HugeKeys.SORT_VALUES,
                HugeKeys.OTHER_VERTEX
        };

        public Edge() {
            super(TABLE);
        }

        @Override
        public void init(CassandraSessionPool.Session session) {
            ImmutableMap<HugeKeys, DataType> pkeys = ImmutableMap.of(
                    HugeKeys.OWNER_VERTEX, DataType.text()
            );
            ImmutableMap<HugeKeys, DataType> ckeys = ImmutableMap.of(
                    HugeKeys.DIRECTION, DataType.tinyint(),
                    HugeKeys.LABEL, DATATYPE_SL,
                    HugeKeys.SORT_VALUES, DataType.text(),
                    HugeKeys.OTHER_VERTEX, DataType.text()
            );
            ImmutableMap<HugeKeys, DataType> columns = ImmutableMap.of(
                    HugeKeys.PROPERTIES, DataType.map(DATATYPE_PK,
                                                      DataType.text())
            );

            this.createTable(session, pkeys, ckeys, columns);
            this.createIndex(session, "edge_label_index", HugeKeys.LABEL);
        }

        @Override
        protected List<HugeKeys> pkColumnName() {
            return ImmutableList.of(HugeKeys.OWNER_VERTEX);
        }

        @Override
        protected List<HugeKeys> idColumnName() {
            return Arrays.asList(KEYS);
        }

        @Override
        protected List<Object> idColumnValue(Id id) {
            return idColumnValue(id, Directions.OUT);
        }

        protected List<Object> idColumnValue(Id id, Directions dir) {
            // TODO: improve Id split()
            String[] idParts = SplicingIdGenerator.split(id);

            // Ensure edge id with Direction
            // NOTE: we assume the id without Direction if it contains 4 parts
            // TODO: should move to Serializer
            if (idParts.length == 4) {
                if (dir == Directions.IN) {
                    // Swap source-vertex and target-vertex
                    String tmp = idParts[0];
                    idParts[0] = idParts[3];
                    idParts[3] = tmp;
                }
                List<Object> list = new LinkedList<>(Arrays.asList(idParts));
                list.add(1, dir.code());
                list.remove(2);
                list.add(2, Long.valueOf(idParts[1]));
                return list;
            } else if (idParts.length == 5) {
                List<Object> list = new LinkedList<>(Arrays.asList(idParts));
                list.remove(1);
                HugeType edgeType = HugeType.fromString(idParts[1]);
                list.add(1, Directions.convert(edgeType).code());
                list.remove(2);
                list.add(2, Long.valueOf(idParts[2]));
                return list;
            }

            return Arrays.asList((Object[]) idParts);
        }

        @Override
        public void delete(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            /*
             * TODO: Delete edge by label
             * Need to implement the framework that can delete with query
             * which contains id or condition.
             */

            // Let super class do delete if not deleting edge by label
            List<Object> idParts = this.idColumnValue(entry.id());
            if (idParts.size() > 1 || entry.columns().size() > 0) {
                super.delete(session, entry);
                return;
            }

            // The only element is label
            this.deleteEdgesByLabel(session, entry.id());
        }

        protected void deleteEdgesByLabel(CassandraSessionPool.Session session,
                                          Id label) {
            final String OWNER_VERTEX = formatKey(HugeKeys.OWNER_VERTEX);
            final String DIRECTION = formatKey(HugeKeys.DIRECTION);

            // Query edges by label index
            Select select = QueryBuilder.select().from(this.table());
            select.where(formatEQ(HugeKeys.LABEL, label.asLong()));

            ResultSet rs;
            try {
                rs = session.execute(select);
            } catch (DriverException e) {
                throw new BackendException("Failed to query edges with " +
                          "label '%s' for deleting", label, e);
            }

            // Delete edges
            for (Iterator<Row> it = rs.iterator(); it.hasNext();) {
                Row row = it.next();

                String ownerVertex = row.get(OWNER_VERTEX, String.class);
                Byte direction = row.get(DIRECTION, Byte.class);

                Delete delete = QueryBuilder.delete().from(this.table());
                delete.where(formatEQ(HugeKeys.OWNER_VERTEX, ownerVertex));
                delete.where(formatEQ(HugeKeys.DIRECTION, direction));
                delete.where(formatEQ(HugeKeys.LABEL, label.asLong()));

                session.add(delete);
            }
        }

        @Override
        protected List<BackendEntry> mergeEntries(List<BackendEntry> entries) {
            // TODO: merge rows before calling result2Entry()

            // Merge edges into vertex
            Map<Id, CassandraBackendEntry> vertices = new HashMap<>();

            for (BackendEntry i : entries) {
                CassandraBackendEntry entry = (CassandraBackendEntry) i;
                Id ownerVertexId = IdGenerator.of(
                                   entry.<String>column(HugeKeys.OWNER_VERTEX));
                if (!vertices.containsKey(ownerVertexId)) {
                    CassandraBackendEntry vertex = new CassandraBackendEntry(
                            HugeType.VERTEX, ownerVertexId);

                    vertex.column(HugeKeys.ID,
                                  entry.column(HugeKeys.OWNER_VERTEX));
                    vertex.column(HugeKeys.PROPERTIES, ImmutableMap.of());

                    vertices.put(ownerVertexId, vertex);
                }
                // Add edge into vertex as a sub row
                vertices.get(ownerVertexId).subRow(entry.row());
            }

            return ImmutableList.copyOf(vertices.values());
        }
    }

    public static class SecondaryIndex extends CassandraTable {

        public static final String TABLE = "secondary_indexes";

        public SecondaryIndex() {
            super(TABLE);
        }

        @Override
        public void init(CassandraSessionPool.Session session) {
            ImmutableMap<HugeKeys, DataType> pkeys = ImmutableMap.of(
                    HugeKeys.FIELD_VALUES, DataType.text()
            );
            ImmutableMap<HugeKeys, DataType> ckeys = ImmutableMap.of(
                    HugeKeys.INDEX_LABEL_ID, DATATYPE_IL
            );
            ImmutableMap<HugeKeys, DataType> columns = ImmutableMap.of(
                    HugeKeys.ELEMENT_IDS, DataType.set(DataType.text())
            );

            this.createTable(session, pkeys, ckeys, columns);
        }

        @Override
        protected List<HugeKeys> idColumnName() {
            return ImmutableList.of(HugeKeys.FIELD_VALUES,
                                    HugeKeys.INDEX_LABEL_ID);
        }

        @Override
        protected List<HugeKeys> modifiableColumnName() {
            return ImmutableList.of(HugeKeys.ELEMENT_IDS);
        }

        @Override
        public void delete(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            String fieldValues = entry.column(HugeKeys.FIELD_VALUES);
            if (fieldValues != null) {
                throw new BackendException("SecondaryIndex deletion " +
                          "should just have INDEX_LABEL_ID, " +
                          "but FIELD_VALUES(%s) is provided.", fieldValues);
            }

            Long indexLabel = entry.column(HugeKeys.INDEX_LABEL_ID);
            if (indexLabel == null) {
                throw new BackendException("SecondaryIndex deletion " +
                          "needs INDEX_LABEL_ID, but not provided.");
            }

            Select select = QueryBuilder.select().from(this.table());
            select.where(formatEQ(HugeKeys.INDEX_LABEL_ID, indexLabel));
            select.allowFiltering();

            ResultSet rs;
            try {
                rs = session.execute(select);
            } catch (DriverException e) {
                throw new BackendException("Failed to query secondary " +
                          "indexes with index label id '%s' for deleting",
                          indexLabel, e);
            }

            final String FIELD_VALUES = formatKey(HugeKeys.FIELD_VALUES);
            for (Iterator<Row> it = rs.iterator(); it.hasNext();) {
                fieldValues = it.next().get(FIELD_VALUES, String.class);
                Delete delete = QueryBuilder.delete().from(this.table());
                delete.where(formatEQ(HugeKeys.INDEX_LABEL_ID, indexLabel));
                delete.where(formatEQ(HugeKeys.FIELD_VALUES, fieldValues));
                session.add(delete);
            }
        }

        @Override
        public void insert(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            throw new BackendException(
                      "SecondaryIndex insertion is not supported.");
        }
    }

    public static class RangeIndex extends CassandraTable {

        public static final String TABLE = "range_indexes";

        public RangeIndex() {
            super(TABLE);
        }

        @Override
        public void init(CassandraSessionPool.Session session) {
            ImmutableMap<HugeKeys, DataType> pkeys = ImmutableMap.of(
                    HugeKeys.INDEX_LABEL_ID, DATATYPE_IL
            );
            ImmutableMap<HugeKeys, DataType> ckeys = ImmutableMap.of(
                    HugeKeys.FIELD_VALUES, DataType.decimal()
            );
            ImmutableMap<HugeKeys, DataType> columns = ImmutableMap.of(
                    HugeKeys.ELEMENT_IDS, DataType.set(DataType.text())
            );

            this.createTable(session, pkeys, ckeys, columns);
        }

        @Override
        protected List<HugeKeys> idColumnName() {
            return ImmutableList.of(HugeKeys.INDEX_LABEL_ID,
                                    HugeKeys.FIELD_VALUES);
        }

        @Override
        protected List<HugeKeys> modifiableColumnName() {
            return ImmutableList.of(HugeKeys.ELEMENT_IDS);
        }

        @Override
        public void delete(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            String fieldValues = entry.column(HugeKeys.FIELD_VALUES);
            if (fieldValues != null) {
                throw new BackendException("Range index deletion " +
                          "should just have INDEX_LABEL_ID, " +
                          "but PROPERTY_VALUES(%s) is provided.", fieldValues);
            }

            Long indexLabel = entry.column(HugeKeys.INDEX_LABEL_ID);
            if (indexLabel == null) {
                throw new BackendException("Range index deletion " +
                          "needs INDEX_LABEL_ID, but not provided.");
            }

            Delete delete = QueryBuilder.delete().from(this.table());
            delete.where(formatEQ(HugeKeys.INDEX_LABEL_ID, indexLabel));
            session.add(delete);
        }

        @Override
        public void insert(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            throw new BackendException(
                      "Range index insertion is not supported.");
        }
    }
}
