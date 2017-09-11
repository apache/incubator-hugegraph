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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Direction;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class CassandraTables {

    /***************************** Table defines *****************************/

    public static class VertexLabel extends CassandraTable {

        public static final String TABLE = "vertex_labels";

        public VertexLabel() {
            super(TABLE);
        }

        @Override
        public void init(CassandraSessionPool.Session session) {
            HugeKeys[] columns = new HugeKeys[] {
                    HugeKeys.NAME,
                    HugeKeys.ID_STRATEGY,
                    HugeKeys.PRIMARY_KEYS,
                    HugeKeys.NULLABLE_KEYS,
                    HugeKeys.INDEX_NAMES,
                    HugeKeys.PROPERTIES
            };

            HugeKeys[] primaryKeys = new HugeKeys[] {HugeKeys.NAME};

            super.createTable(session, columns, primaryKeys);
        }
    }

    public static class EdgeLabel extends CassandraTable {

        public static final String TABLE = "edge_labels";

        public EdgeLabel() {
            super(TABLE);
        }

        @Override
        public void init(CassandraSessionPool.Session session) {
            HugeKeys[] columns = new HugeKeys[] {
                    HugeKeys.NAME,
                    HugeKeys.SOURCE_LABEL,
                    HugeKeys.TARGET_LABEL,
                    HugeKeys.FREQUENCY,
                    HugeKeys.SORT_KEYS,
                    HugeKeys.NULLABLE_KEYS,
                    HugeKeys.INDEX_NAMES,
                    HugeKeys.PROPERTIES
            };

            HugeKeys[] primaryKeys = new HugeKeys[] {HugeKeys.NAME};

            super.createTable(session, columns, primaryKeys);
        }
    }

    public static class PropertyKey extends CassandraTable {

        public static final String TABLE = "property_keys";

        public PropertyKey() {
            super(TABLE);
        }

        @Override
        public void init(CassandraSessionPool.Session session) {
            HugeKeys[] columns = new HugeKeys[] {
                    HugeKeys.NAME,
                    HugeKeys.DATA_TYPE,
                    HugeKeys.CARDINALITY,
                    HugeKeys.PROPERTIES
            };

            HugeKeys[] primaryKeys = new HugeKeys[] {HugeKeys.NAME};

            super.createTable(session, columns, primaryKeys);
        }
    }

    public static class IndexLabel extends CassandraTable {

        public static final String TABLE = "index_labels";

        public IndexLabel() {
            super(TABLE);
        }

        @Override
        public void init(CassandraSessionPool.Session session) {
            HugeKeys[] columns = new HugeKeys[] {
                    HugeKeys.NAME,
                    HugeKeys.BASE_TYPE,
                    HugeKeys.BASE_VALUE,
                    HugeKeys.INDEX_TYPE,
                    HugeKeys.FIELDS
            };

            // The base-type and base-value as clustering key
            HugeKeys[] primaryKeys = new HugeKeys[] {
                    HugeKeys.NAME,
                    HugeKeys.BASE_TYPE,
                    HugeKeys.BASE_VALUE
            };

            super.createTable(session, columns, primaryKeys);
        }
    }

    public static class Vertex extends CassandraTable {

        public static final String TABLE = "vertices";

        public Vertex() {
            super(TABLE);
        }

        @Override
        public void init(CassandraSessionPool.Session session) {
            HugeKeys[] columns = new HugeKeys[] {
                    HugeKeys.ID,
                    HugeKeys.LABEL,
                    HugeKeys.PROPERTIES
            };

            DataType[] columnTypes = new DataType[] {
                    DataType.text(),
                    DataType.text(),
                    DataType.map(DataType.text(), DataType.text())
            };

            HugeKeys[] partitionKeys = new HugeKeys[] {
                    HugeKeys.ID
            };

            HugeKeys[] clusterKeys = new HugeKeys[] {
            };

            super.createTable(session, columns, columnTypes,
                              partitionKeys, clusterKeys);
            super.createIndex(session, "vertex_label_index", HugeKeys.LABEL);
        }

        @Override
        protected List<String> idColumnName() {
            return ImmutableList.of(formatKey(HugeKeys.ID));
        }

        @Override
        protected List<BackendEntry> mergeEntries(List<BackendEntry> entries) {
            // Set id for entries
            for (BackendEntry i : entries) {
                CassandraBackendEntry entry = (CassandraBackendEntry) i;
                entry.id(IdGenerator.of(entry.<String>column(HugeKeys.ID)));
            }
            return entries;
        }
    }

    public static class Edge extends CassandraTable {

        public static final String TABLE = "edges";

        private static final HugeKeys[] KEYS = new HugeKeys[] {
                HugeKeys.SOURCE_VERTEX,
                HugeKeys.DIRECTION,
                HugeKeys.LABEL,
                HugeKeys.SORT_VALUES,
                HugeKeys.TARGET_VERTEX
        };

        private static List<String> KEYS_STRING = null;

        public Edge() {
            super(TABLE);
        }

        @Override
        public void init(CassandraSessionPool.Session session) {
            HugeKeys[] columns = new HugeKeys[] {
                    HugeKeys.SOURCE_VERTEX,
                    HugeKeys.DIRECTION,
                    HugeKeys.LABEL,
                    HugeKeys.SORT_VALUES,
                    HugeKeys.TARGET_VERTEX,
                    HugeKeys.PROPERTIES
            };

            DataType[] columnTypes = new DataType[] {
                    DataType.text(),
                    DataType.text(),
                    DataType.text(),
                    DataType.text(),
                    DataType.text(),
                    DataType.map(DataType.text(), DataType.text())
            };

            HugeKeys[] primaryKeys = new HugeKeys[] {
                    HugeKeys.SOURCE_VERTEX,
                    HugeKeys.DIRECTION,
                    HugeKeys.LABEL,
                    HugeKeys.SORT_VALUES,
                    HugeKeys.TARGET_VERTEX
            };

            super.createTable(session, columns, columnTypes, primaryKeys);
            super.createIndex(session, "edge_label_index", HugeKeys.LABEL);
        }

        @Override
        protected List<String> pkColumnName() {
            return ImmutableList.of(formatKey(HugeKeys.SOURCE_VERTEX));
        }

        @Override
        protected List<String> idColumnName() {
            if (KEYS_STRING == null) {
                KEYS_STRING = new ArrayList<>(KEYS.length);
                for (HugeKeys k : KEYS) {
                    KEYS_STRING.add(formatKey(k));
                }
            }
            return KEYS_STRING;
        }

        @Override
        protected List<String> idColumnValue(Id id) {
            // TODO: improve Id split()
            List<String> idParts = ImmutableList.copyOf(
                                   SplicingIdGenerator.split(id));
            // Ensure edge id with Direction
            // NOTE: we assume the id without Direction if it contains 4 parts
            // TODO: should move to Serializer
            if (idParts.size() == 4) {
                idParts = new LinkedList<>(idParts);
                idParts.add(1, Direction.OUT.name());
            }

            return idParts;
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
            List<String> idParts = this.idColumnValue(entry.id());
            if (idParts.size() > 1 || entry.columns().size() > 0) {
                super.delete(session, entry);
                return;
            }

            // The only element is label
            String label = idParts.get(0);

            Select select = QueryBuilder.select().from(this.table());
            select.where(formatEQ(HugeKeys.LABEL, label));

            ResultSet rs;
            try {
                rs = session.execute(select);
            } catch (DriverException e) {
                throw new BackendException("Failed to query edges with " +
                          "label '%s' for deleting", label, e);
            }

            final String LABEL = formatKey(HugeKeys.LABEL);
            final String SOURCE_VERTEX = formatKey(HugeKeys.SOURCE_VERTEX);
            final String DIRECTION = formatKey(HugeKeys.DIRECTION);

            for (Iterator<Row> it = rs.iterator(); it.hasNext();) {
                Row row = it.next();

                assert label.equals(row.get(LABEL, String.class));
                String sourceVertex = row.get(SOURCE_VERTEX, String.class);
                String direction = row.get(DIRECTION, String.class);

                Delete delete = QueryBuilder.delete().from(this.table());
                delete.where(formatEQ(HugeKeys.SOURCE_VERTEX, sourceVertex));
                delete.where(formatEQ(HugeKeys.DIRECTION, direction));
                delete.where(formatEQ(HugeKeys.LABEL, label));

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
                Id srcVertexId = IdGenerator.of(
                                 entry.<String>column(HugeKeys.SOURCE_VERTEX));
                if (!vertices.containsKey(srcVertexId)) {
                    CassandraBackendEntry vertex = new CassandraBackendEntry(
                            HugeType.VERTEX, srcVertexId);

                    vertex.column(HugeKeys.ID,
                                  entry.column(HugeKeys.SOURCE_VERTEX));
                    vertex.column(HugeKeys.PROPERTIES, ImmutableMap.of());

                    vertices.put(srcVertexId, vertex);
                }
                // Add edge into vertex as a sub row
                vertices.get(srcVertexId).subRow(entry.row());
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
            HugeKeys[] columns = new HugeKeys[] {
                    HugeKeys.FIELD_VALUES,
                    HugeKeys.INDEX_LABEL_NAME,
                    HugeKeys.ELEMENT_IDS
            };

            HugeKeys[] primaryKeys = new HugeKeys[] {
                    HugeKeys.FIELD_VALUES,
                    HugeKeys.INDEX_LABEL_NAME
            };

            DataType[] columnTypes = new DataType[] {
                    DataType.text(),
                    DataType.text(),
                    DataType.set(DataType.text())
            };

            super.createTable(session, columns, columnTypes, primaryKeys);
        }

        @Override
        protected List<String> idColumnName() {
            return ImmutableList.of(formatKey(HugeKeys.FIELD_VALUES),
                                    formatKey(HugeKeys.INDEX_LABEL_NAME));
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
                          "should just have INDEX_LABEL_NAME, " +
                          "but FIELD_VALUES(%s) is provided.", fieldValues);
            }

            String indexLabel = entry.column(HugeKeys.INDEX_LABEL_NAME);
            if (indexLabel == null || indexLabel.isEmpty()) {
                throw new BackendException("SecondaryIndex deletion " +
                          "needs INDEX_LABEL_NAME, but not provided.");
            }

            Select select = QueryBuilder.select().from(this.table());
            select.where(formatEQ(HugeKeys.INDEX_LABEL_NAME, indexLabel));
            select.allowFiltering();

            ResultSet rs;
            try {
                rs = session.execute(select);
            } catch (DriverException e) {
                throw new BackendException("Failed to query secondary " +
                          "indexes with index label '%s' for deleting",
                          indexLabel, e);
            }

            final String FIELD_VALUES = formatKey(HugeKeys.FIELD_VALUES);
            for (Iterator<Row> it = rs.iterator(); it.hasNext();) {
                fieldValues = it.next().get(FIELD_VALUES, String.class);
                Delete delete = QueryBuilder.delete().from(this.table());
                delete.where(formatEQ(HugeKeys.INDEX_LABEL_NAME, indexLabel));
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

    public static class SearchIndex extends CassandraTable {

        public static final String TABLE = "search_indexes";

        public SearchIndex() {
            super(TABLE);
        }

        @Override
        public void init(CassandraSessionPool.Session session) {
            HugeKeys[] columns = new HugeKeys[] {
                    HugeKeys.INDEX_LABEL_NAME,
                    HugeKeys.FIELD_VALUES,
                    HugeKeys.ELEMENT_IDS
            };

            HugeKeys[] primaryKeys = new HugeKeys[] {
                    HugeKeys.INDEX_LABEL_NAME,
                    HugeKeys.FIELD_VALUES
            };

            DataType[] columnTypes = new DataType[] {
                    DataType.text(),
                    DataType.decimal(),
                    DataType.set(DataType.text())
            };

            super.createTable(session, columns, columnTypes, primaryKeys);
        }

        @Override
        protected List<String> idColumnName() {
            return ImmutableList.of(formatKey(HugeKeys.INDEX_LABEL_NAME),
                                    formatKey(HugeKeys.FIELD_VALUES));
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
                throw new BackendException("SearchIndex deletion " +
                          "should just have INDEX_LABEL_NAME, " +
                          "but PROPERTY_VALUES(%s) is provided.", fieldValues);
            }

            String indexLabel = entry.column(HugeKeys.INDEX_LABEL_NAME);
            if (indexLabel == null || indexLabel.isEmpty()) {
                throw new BackendException("SearchIndex deletion " +
                          "needs INDEX_LABEL_NAME, but not provided.");
            }

            Delete delete = QueryBuilder.delete().from(this.table());
            delete.where(formatEQ(HugeKeys.INDEX_LABEL_NAME, indexLabel));
            session.add(delete);
        }

        @Override
        public void insert(CassandraSessionPool.Session session,
                           CassandraBackendEntry.Row entry) {
            throw new BackendException(
                      "SearchIndex insertion is not supported.");
        }
    }
}
