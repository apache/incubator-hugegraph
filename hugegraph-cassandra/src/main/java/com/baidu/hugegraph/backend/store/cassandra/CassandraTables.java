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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.id.IdUtil;
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

    public static final String LABEL_INDEX = "label_index";
    public static final String NAME_INDEX = "name_index";

    private static final DataType DATATYPE_PK = DataType.cint();
    private static final DataType DATATYPE_SL = DataType.cint(); // VL/EL
    private static final DataType DATATYPE_IL = DataType.cint();

    private static final DataType DATATYPE_UD = DataType.map(DataType.text(),
                                                             DataType.text());

    private static final int COMMIT_DELETE_BATCH = 1000;

    public static class Counters extends CassandraTable {

        public static final String TABLE = "counters";

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

        public long getCounter(CassandraSessionPool.Session session,
                               HugeType type) {
            Clause where = formatEQ(HugeKeys.SCHEMA_TYPE, type.name());
            Select select = QueryBuilder.select(formatKey(HugeKeys.ID))
                                        .from(TABLE);
            select.where(where);
            Row row = session.execute(select).one();
            if (row == null) {
                return 0L;
            } else {
                return row.getLong(formatKey(HugeKeys.ID));
            }
        }

        public void increaseCounter(CassandraSessionPool.Session session,
                                    HugeType type, long increment) {
            Update update = QueryBuilder.update(TABLE);
            update.with(QueryBuilder.incr(formatKey(HugeKeys.ID), increment));
            update.where(formatEQ(HugeKeys.SCHEMA_TYPE, type.name()));
            session.execute(update);
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
                    .put(HugeKeys.ENABLE_LABEL_INDEX, DataType.cboolean())
                    .put(HugeKeys.USER_DATA, DATATYPE_UD)
                    .put(HugeKeys.STATUS, DataType.tinyint())
                    .build();

            this.createTable(session, pkeys, ckeys, columns);
            this.createIndex(session, NAME_INDEX, HugeKeys.NAME);
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
                    .put(HugeKeys.ENABLE_LABEL_INDEX, DataType.cboolean())
                    .put(HugeKeys.USER_DATA, DATATYPE_UD)
                    .put(HugeKeys.STATUS, DataType.tinyint())
                    .build();

            this.createTable(session, pkeys, ckeys, columns);
            this.createIndex(session, NAME_INDEX, HugeKeys.NAME);
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
            ImmutableMap<HugeKeys, DataType> columns = ImmutableMap
                    .<HugeKeys, DataType>builder()
                    .put(HugeKeys.NAME, DataType.text())
                    .put(HugeKeys.DATA_TYPE, DataType.tinyint())
                    .put(HugeKeys.CARDINALITY, DataType.tinyint())
                    .put(HugeKeys.PROPERTIES, DataType.set(DATATYPE_PK))
                    .put(HugeKeys.USER_DATA, DATATYPE_UD)
                    .put(HugeKeys.STATUS, DataType.tinyint())
                    .build();

            this.createTable(session, pkeys, ckeys, columns);
            this.createIndex(session, NAME_INDEX, HugeKeys.NAME);
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
            ImmutableMap<HugeKeys, DataType> ckeys = ImmutableMap.of();
            ImmutableMap<HugeKeys, DataType> columns = ImmutableMap
                    .<HugeKeys, DataType>builder()
                    .put(HugeKeys.NAME, DataType.text())
                    .put(HugeKeys.BASE_TYPE, DataType.tinyint())
                    .put(HugeKeys.BASE_VALUE, DATATYPE_SL)
                    .put(HugeKeys.INDEX_TYPE, DataType.tinyint())
                    .put(HugeKeys.FIELDS, DataType.list(DATATYPE_PK))
                    .put(HugeKeys.STATUS, DataType.tinyint())
                    .build();

            this.createTable(session, pkeys, ckeys, columns);
            this.createIndex(session, NAME_INDEX, HugeKeys.NAME);
        }
    }

    public static class Vertex extends CassandraTable {

        public static final String TABLE = "vertices";

        public Vertex(String store) {
            super(joinTableName(store, TABLE));
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
            this.createIndex(session, LABEL_INDEX, HugeKeys.LABEL);
        }

        @Override
        public void insert(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            Map<HugeKeys, Object> columns = entry.columns();
            assert columns.containsKey(HugeKeys.ID);
            Object id = columns.get(HugeKeys.ID);
            Object label = columns.get(HugeKeys.LABEL);
            E.checkState(label != null,
                         "The label of inserting vertex can't be null");
            Map<?, ?> properties = (Map<?, ?>) columns.get(HugeKeys.PROPERTIES);
            E.checkState(properties != null,
                         "The properties of inserting vertex can't be null");

            Update update = QueryBuilder.update(table());
            update.with(QueryBuilder.set(formatKey(HugeKeys.LABEL), label));
            update.with(QueryBuilder.putAll(formatKey(HugeKeys.PROPERTIES),
                                            properties));
            update.where(formatEQ(HugeKeys.ID, id));

            session.add(update);
        }
    }

    public static class Edge extends CassandraTable {

        public static final String TABLE_PREFIX = "edges";

        private final String store;
        private final Directions direction;

        protected Edge(String store, Directions direction) {
            super(joinTableName(store, table(direction)));
            this.store = store;
            this.direction = direction;
        }

        protected String edgesTable(Directions direction) {
            return joinTableName(this.store, table(direction));
        }

        protected Directions direction() {
            return this.direction;
        }

        protected String labelIndexTable() {
            return this.table();
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

            /*
             * Only out-edges table needs label index because we query edges
             * by label from out-edges table
             */
            if (this.direction == Directions.OUT) {
                this.createIndex(session, LABEL_INDEX, HugeKeys.LABEL);
            }
        }

        @Override
        protected List<HugeKeys> pkColumnName() {
            return ImmutableList.of(HugeKeys.OWNER_VERTEX);
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

            E.checkState(edgeId.direction() == this.direction,
                         "Can't query %s edges from %s edges table",
                         edgeId.direction(), this.direction);

            return idColumnValue(edgeId);
        }

        protected final List<Object> idColumnValue(EdgeId edgeId) {
            List<Object> list = new ArrayList<>(5);
            list.add(IdUtil.writeString(edgeId.ownerVertexId()));
            list.add(edgeId.direction().code());
            list.add(edgeId.edgeLabelId().asLong());
            list.add(edgeId.sortValues());
            list.add(IdUtil.writeString(edgeId.otherVertexId()));
            return list;
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
            // Edges in edges_in table will be deleted when direction is OUT
            if (this.direction == Directions.IN) {
                return;
            }

            final String OWNER_VERTEX = formatKey(HugeKeys.OWNER_VERTEX);
            final String OTHER_VERTEX = formatKey(HugeKeys.OTHER_VERTEX);

            // Query edges by label index
            Select select = QueryBuilder.select().from(this.labelIndexTable());
            select.where(formatEQ(HugeKeys.LABEL, label.asLong()));

            ResultSet rs;
            try {
                rs = session.execute(select);
            } catch (DriverException e) {
                throw new BackendException("Failed to query edges " +
                          "with label '%s' for deleting", e, label);
            }

            // Delete edges
            int count = 0;
            for (Iterator<Row> it = rs.iterator(); it.hasNext();) {
                Row row = it.next();
                // Delete OUT edges from edges_out table
                String ownerVertex = row.get(OWNER_VERTEX, String.class);
                session.add(buildDelete(label, ownerVertex, Directions.OUT));

                // Delete IN edges from edges_in table
                String otherVertex = row.get(OTHER_VERTEX, String.class);
                session.add(buildDelete(label, otherVertex, Directions.IN));

                count += 2;
                if (count >= COMMIT_DELETE_BATCH - 2) {
                    session.commit();
                    count = 0;
                }
            }
        }

        private Delete buildDelete(Id label, String ownerVertex,
                                   Directions direction) {
            Delete delete = QueryBuilder.delete().from(edgesTable(direction));
            delete.where(formatEQ(HugeKeys.OWNER_VERTEX, ownerVertex));
            delete.where(formatEQ(HugeKeys.DIRECTION, direction.code()));
            delete.where(formatEQ(HugeKeys.LABEL, label.asLong()));
            return delete;
        }

        @Override
        protected BackendEntry mergeEntries(BackendEntry e1, BackendEntry e2) {
            // Merge edges into vertex
            // TODO: merge rows before calling row2Entry()

            CassandraBackendEntry current = (CassandraBackendEntry) e1;
            CassandraBackendEntry next = (CassandraBackendEntry) e2;

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

        private CassandraBackendEntry wrapByVertex(CassandraBackendEntry edge) {
            assert edge.type().isEdge();
            String ownerVertex = edge.column(HugeKeys.OWNER_VERTEX);
            E.checkState(ownerVertex != null, "Invalid backend entry");
            Id vertexId = IdGenerator.of(ownerVertex);
            CassandraBackendEntry vertex = new CassandraBackendEntry(
                                               HugeType.VERTEX, vertexId);

            vertex.column(HugeKeys.ID, ownerVertex);
            vertex.column(HugeKeys.PROPERTIES, ImmutableMap.of());

            vertex.subRow(edge.row());
            return vertex;
        }

        private static String table(Directions direction) {
            assert direction == Directions.OUT || direction == Directions.IN;
            return TABLE_PREFIX + "_" + direction.string();
        }

        public static CassandraTable out(String store) {
            return new Edge(store, Directions.OUT);
        }

        public static CassandraTable in(String store) {
            return new Edge(store, Directions.IN);
        }
    }

    public static class SecondaryIndex extends CassandraTable {

        public static final String TABLE = "secondary_indexes";

        public SecondaryIndex(String store) {
            this(store, TABLE);
        }

        protected SecondaryIndex(String store, String table) {
            super(joinTableName(store, table));
        }

        @Override
        public void init(CassandraSessionPool.Session session) {
            ImmutableMap<HugeKeys, DataType> pkeys = ImmutableMap.of(
                    HugeKeys.FIELD_VALUES, DataType.text()
            );
            ImmutableMap<HugeKeys, DataType> ckeys = ImmutableMap.of(
                    HugeKeys.INDEX_LABEL_ID, DATATYPE_IL,
                    HugeKeys.ELEMENT_IDS, DataType.text()
            );
            ImmutableMap<HugeKeys, DataType> columns = ImmutableMap.of();

            this.createTable(session, pkeys, ckeys, columns);
        }

        @Override
        protected List<HugeKeys> idColumnName() {
            return ImmutableList.of(HugeKeys.FIELD_VALUES,
                                    HugeKeys.INDEX_LABEL_ID,
                                    HugeKeys.ELEMENT_IDS);
        }

        @Override
        protected List<HugeKeys> modifiableColumnName() {
            return ImmutableList.of();
        }

        @Override
        public void delete(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            String fieldValues = entry.column(HugeKeys.FIELD_VALUES);
            if (fieldValues != null) {
                super.delete(session, entry);
                return;
            }

            Long indexLabel = entry.column(HugeKeys.INDEX_LABEL_ID);
            if (indexLabel == null) {
                throw new BackendException("SecondaryIndex deletion needs " +
                                           "INDEX_LABEL_ID, but not provided.");
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
            int count = 0;
            for (Iterator<Row> it = rs.iterator(); it.hasNext();) {
                fieldValues = it.next().get(FIELD_VALUES, String.class);
                Delete delete = QueryBuilder.delete().from(this.table());
                delete.where(formatEQ(HugeKeys.INDEX_LABEL_ID, indexLabel));
                delete.where(formatEQ(HugeKeys.FIELD_VALUES, fieldValues));
                session.add(delete);

                if (++count >= COMMIT_DELETE_BATCH) {
                    session.commit();
                    count = 0;
                }
            }
        }

        @Override
        public void insert(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            throw new BackendException(
                      "SecondaryIndex insertion is not supported.");
        }

        @Override
        public void append(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            assert entry.columns().size() == 3;
            super.insert(session, entry);
        }

        @Override
        public void eliminate(CassandraSessionPool.Session session,
                              CassandraBackendEntry.Row entry) {
            assert entry.columns().size() == 3;
            this.delete(session, entry);
        }
    }

    public static class SearchIndex extends SecondaryIndex {

        public static final String TABLE = "search_indexes";

        public SearchIndex(String store) {
            super(store, TABLE);
        }

        @Override
        public void insert(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            throw new BackendException(
                      "SearchIndex insertion is not supported.");
        }
    }

    public abstract static class RangeIndex extends CassandraTable {

        protected RangeIndex(String store, String table) {
            super(joinTableName(store, table));
        }

        @Override
        public void init(CassandraSessionPool.Session session) {
            ImmutableMap<HugeKeys, DataType> pkeys = ImmutableMap.of(
                    HugeKeys.INDEX_LABEL_ID, DATATYPE_IL
            );
            ImmutableMap<HugeKeys, DataType> ckeys = ImmutableMap.of(
                    HugeKeys.FIELD_VALUES, DataType.decimal(),
                    HugeKeys.ELEMENT_IDS, DataType.text()
            );
            ImmutableMap<HugeKeys, DataType> columns = ImmutableMap.of();

            this.createTable(session, pkeys, ckeys, columns);
        }

        @Override
        protected List<HugeKeys> idColumnName() {
            return ImmutableList.of(HugeKeys.INDEX_LABEL_ID,
                                    HugeKeys.FIELD_VALUES,
                                    HugeKeys.ELEMENT_IDS);
        }

        @Override
        protected List<HugeKeys> modifiableColumnName() {
            return ImmutableList.of();
        }

        @Override
        public void delete(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            Object fieldValues = entry.column(HugeKeys.FIELD_VALUES);
            if (fieldValues != null) {
                super.delete(session, entry);
                return;
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
                      "RangeIndex insertion is not supported.");
        }

        @Override
        public void append(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            assert entry.columns().size() == 3;
            super.insert(session, entry);
        }

        @Override
        public void eliminate(CassandraSessionPool.Session session,
                              CassandraBackendEntry.Row entry) {
            assert entry.columns().size() == 3;
            this.delete(session, entry);
        }
    }

    public static class RangeIntIndex extends RangeIndex {

        public static final String TABLE = "range_int_indexes";

        public RangeIntIndex(String store) {
            super(store, TABLE);
        }

        @Override
        public void init(CassandraSessionPool.Session session) {
            ImmutableMap<HugeKeys, DataType> pkeys = ImmutableMap.of(
                    HugeKeys.INDEX_LABEL_ID, DATATYPE_IL
            );
            ImmutableMap<HugeKeys, DataType> ckeys = ImmutableMap.of(
                    HugeKeys.FIELD_VALUES, DataType.cint(),
                    HugeKeys.ELEMENT_IDS, DataType.text()
            );
            ImmutableMap<HugeKeys, DataType> columns = ImmutableMap.of();

            this.createTable(session, pkeys, ckeys, columns);
        }
    }

    public static class RangeFloatIndex extends RangeIndex {

        public static final String TABLE = "range_float_indexes";

        public RangeFloatIndex(String store) {
            super(store, TABLE);
        }

        @Override
        public void init(CassandraSessionPool.Session session) {
            ImmutableMap<HugeKeys, DataType> pkeys = ImmutableMap.of(
                    HugeKeys.INDEX_LABEL_ID, DATATYPE_IL
            );
            ImmutableMap<HugeKeys, DataType> ckeys = ImmutableMap.of(
                    HugeKeys.FIELD_VALUES, DataType.cfloat(),
                    HugeKeys.ELEMENT_IDS, DataType.text()
            );
            ImmutableMap<HugeKeys, DataType> columns = ImmutableMap.of();

            this.createTable(session, pkeys, ckeys, columns);
        }
    }

    public static class RangeLongIndex extends RangeIndex {

        public static final String TABLE = "range_long_indexes";

        public RangeLongIndex(String store) {
            super(store, TABLE);
        }

        @Override
        public void init(CassandraSessionPool.Session session) {
            ImmutableMap<HugeKeys, DataType> pkeys = ImmutableMap.of(
                    HugeKeys.INDEX_LABEL_ID, DATATYPE_IL
            );
            ImmutableMap<HugeKeys, DataType> ckeys = ImmutableMap.of(
                    HugeKeys.FIELD_VALUES, DataType.bigint(),
                    HugeKeys.ELEMENT_IDS, DataType.text()
            );
            ImmutableMap<HugeKeys, DataType> columns = ImmutableMap.of();

            this.createTable(session, pkeys, ckeys, columns);
        }
    }

    public static class RangeDoubleIndex extends RangeIndex {

        public static final String TABLE = "range_double_indexes";

        public RangeDoubleIndex(String store) {
            super(store, TABLE);
        }

        @Override
        public void init(CassandraSessionPool.Session session) {
            ImmutableMap<HugeKeys, DataType> pkeys = ImmutableMap.of(
                    HugeKeys.INDEX_LABEL_ID, DATATYPE_IL
            );
            ImmutableMap<HugeKeys, DataType> ckeys = ImmutableMap.of(
                    HugeKeys.FIELD_VALUES, DataType.cdouble(),
                    HugeKeys.ELEMENT_IDS, DataType.text()
            );
            ImmutableMap<HugeKeys, DataType> columns = ImmutableMap.of();

            this.createTable(session, pkeys, ckeys, columns);
        }
    }

    public static class ShardIndex extends RangeIndex {

        public static final String TABLE = "shard_indexes";

        public ShardIndex(String store) {
            super(store, TABLE);
        }

        @Override
        public void init(CassandraSessionPool.Session session) {
            ImmutableMap<HugeKeys, DataType> pkeys = ImmutableMap.of(
                    HugeKeys.INDEX_LABEL_ID, DATATYPE_IL
            );
            ImmutableMap<HugeKeys, DataType> ckeys = ImmutableMap.of(
                    HugeKeys.FIELD_VALUES, DataType.text(),
                    HugeKeys.ELEMENT_IDS, DataType.text()
            );
            ImmutableMap<HugeKeys, DataType> columns = ImmutableMap.of();

            this.createTable(session, pkeys, ckeys, columns);
        }

        @Override
        public void insert(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            throw new BackendException(
                      "ShardIndex insertion is not supported.");
        }
    }
}
