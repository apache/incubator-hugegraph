package com.baidu.hugegraph.backend.store.cassandra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Direction;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGeneratorFactory;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.JsonUtil;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class CassandraTables {

    /***************************** table defines *****************************/

    public static class VertexLabel extends CassandraTable {

        public static final String TABLE = "vertex_labels";

        public VertexLabel() {
            super(TABLE);
        }

        @Override
        public void init(Session session) {
            HugeKeys[] columns = new HugeKeys[] {
                    HugeKeys.NAME,
                    HugeKeys.PROPERTIES,
                    HugeKeys.PRIMARY_KEYS,
                    HugeKeys.INDEX_NAMES
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
        public void init(Session session) {
            HugeKeys[] columns = new HugeKeys[] {
                    HugeKeys.NAME,
                    HugeKeys.LINKS,
                    HugeKeys.PROPERTIES,
                    HugeKeys.SORT_KEYS,
                    HugeKeys.FREQUENCY,
                    HugeKeys.INDEX_NAMES
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
        public void init(Session session) {
            HugeKeys[] columns = new HugeKeys[] {
                    HugeKeys.NAME,
                    HugeKeys.DATA_TYPE,
                    HugeKeys.CARDINALITY,
                    HugeKeys.PROPERTIES};

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
        public void init(Session session) {
            HugeKeys[] columns = new HugeKeys[] {
                    HugeKeys.NAME,
                    HugeKeys.BASE_TYPE,
                    HugeKeys.BASE_VALUE,
                    HugeKeys.INDEX_TYPE,
                    HugeKeys.FIELDS
            };

            // base-type and base-value as clustering key
            HugeKeys[] primaryKeys = new HugeKeys[] {
                    HugeKeys.NAME,
                    HugeKeys.BASE_TYPE,
                    HugeKeys.BASE_VALUE,
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
        public void init(Session session) {
            HugeKeys[] columns = new HugeKeys[] {
                    HugeKeys.LABEL,
                    HugeKeys.PRIMARY_VALUES,
                    HugeKeys.PROPERTIES
            };

            DataType[] columnTypes = new DataType[] {
                    DataType.text(),
                    DataType.text(),
                    DataType.map(DataType.text(), DataType.text())
            };

            HugeKeys[] partitionKeys = new HugeKeys[] {
                    HugeKeys.LABEL,
                    HugeKeys.PRIMARY_VALUES
            };

            HugeKeys[] clusterKeys = new HugeKeys[] {};

            super.createTable(session, columns, columnTypes,
                    partitionKeys, clusterKeys);
            super.createIndex(session, "vertices_label_index", HugeKeys.LABEL);
        }

        @Override
        protected List<String> idColumnName() {
            return ImmutableList.of(
                    formatKey(HugeKeys.LABEL),
                    formatKey( HugeKeys.PRIMARY_VALUES));
        }

        @Override
        protected List<String> idColumnValue(Id id) {
            // TODO: improve Id parse()
            return ImmutableList.copyOf(SplicingIdGenerator.parse(id));
        }

        @Override
        protected List<BackendEntry> mergeEntries(List<BackendEntry> entries) {
            // set id for entries
            for (BackendEntry i : entries) {
                CassandraBackendEntry entry = (CassandraBackendEntry) i;
                Id id = SplicingIdGenerator.splicing(
                        entry.column(HugeKeys.LABEL),
                        entry.column(HugeKeys.PRIMARY_VALUES));
                entry.id(id);
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
                HugeKeys.TARGET_VERTEX};

        private static List<String> KEYS_STRING = null;

        public Edge() {
            super(TABLE);
        }

        @Override
        public void init(Session session) {
            HugeKeys[] columns = new HugeKeys[] {
                    HugeKeys.SOURCE_VERTEX,
                    HugeKeys.DIRECTION,
                    HugeKeys.LABEL,
                    HugeKeys.SORT_VALUES,
                    HugeKeys.TARGET_VERTEX,
                    HugeKeys.PROPERTIES};

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
                    HugeKeys.TARGET_VERTEX};

            super.createTable(session, columns, columnTypes, primaryKeys);
            super.createIndex(session, "edges_label_index", HugeKeys.LABEL);
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

            // ensure edge id with Direction
            // NOTE: we assume the id without Direction if it contains 4 parts
            // TODO: should move to Serializer
            if (idParts.size() == 4) {
                idParts = new LinkedList<>(idParts);
                idParts.add(1, Direction.OUT.name());
            }

            return idParts;
        }

        @Override
        protected List<BackendEntry> mergeEntries(List<BackendEntry> entries) {
            // TODO: merge rows before calling result2Entry()

            // merge edges into vertex
            Map<Id, CassandraBackendEntry> vertices = new HashMap<>();

            for (BackendEntry i : entries) {
                CassandraBackendEntry entry = (CassandraBackendEntry) i;
                Id srcVertexId = IdGeneratorFactory.generator().generate(
                        entry.<String>column(HugeKeys.SOURCE_VERTEX));
                if (!vertices.containsKey(srcVertexId)) {
                    CassandraBackendEntry vertex = new CassandraBackendEntry(
                            HugeType.VERTEX, srcVertexId);
                    // set vertex label and pv(assume vertex id with a label)
                    // TODO: improve Id parse()
                    String[] idParts = SplicingIdGenerator.parse(srcVertexId);
                    vertex.column(HugeKeys.LABEL, idParts[0]);
                    vertex.column(HugeKeys.PRIMARY_VALUES, idParts[1]);
                    vertex.column(HugeKeys.PROPERTIES, ImmutableMap.of());

                    vertices.put(srcVertexId, vertex);
                }
                // add edge into vertex as a sub row
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
        public void init(Session session) {
            HugeKeys[] columns = new HugeKeys[] {
                    HugeKeys.PROPERTY_VALUES,
                    HugeKeys.INDEX_LABEL_NAME,
                    HugeKeys.ELEMENT_IDS
            };

            HugeKeys[] primaryKeys = new HugeKeys[] {
                    HugeKeys.PROPERTY_VALUES,
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
            return ImmutableList.of(formatKey(HugeKeys.PROPERTY_VALUES));
        }

        @Override
        public void insert(CassandraBackendEntry.Row entry) {
            Update update = QueryBuilder.update(super.table);

            update.with(QueryBuilder.append(formatKey(HugeKeys.ELEMENT_IDS),
                    entry.column(HugeKeys.ELEMENT_IDS)));
            update.where(QueryBuilder.eq(formatKey(HugeKeys.INDEX_LABEL_NAME),
                    entry.column(HugeKeys.INDEX_LABEL_NAME)));
            update.where(QueryBuilder.eq(formatKey(HugeKeys.PROPERTY_VALUES),
                    entry.column(HugeKeys.PROPERTY_VALUES)));

            super.batch.add(update);
        }

        @Override
        public void delete(CassandraBackendEntry.Row entry) {
            Update update = QueryBuilder.update(super.table);

            update.with(QueryBuilder.remove(formatKey(HugeKeys.ELEMENT_IDS),
                    entry.column(HugeKeys.ELEMENT_IDS)));
            update.where(QueryBuilder.eq(formatKey(HugeKeys.INDEX_LABEL_NAME),
                    entry.column(HugeKeys.INDEX_LABEL_NAME)));
            update.where(QueryBuilder.eq(formatKey(HugeKeys.PROPERTY_VALUES),
                    entry.column(HugeKeys.PROPERTY_VALUES)));

            super.batch.add(update);
        }

        @Override
        protected CassandraBackendEntry result2Entry(HugeType type, Row row) {
            CassandraBackendEntry entry = new CassandraBackendEntry(type);

            entry.column(HugeKeys.PROPERTY_VALUES,
                    row.getString(formatKey(HugeKeys.PROPERTY_VALUES)));
            entry.column(HugeKeys.INDEX_LABEL_NAME,
                    row.getString(formatKey(HugeKeys.INDEX_LABEL_NAME)));
            entry.column(HugeKeys.ELEMENT_IDS, JsonUtil.toJson(
                    row.getSet(formatKey(HugeKeys.ELEMENT_IDS), String.class)));

            return entry;
        }
    }

    public static class SearchIndex extends CassandraTable {

        public static final String TABLE = "search_indexes";

        public SearchIndex() {
            super(TABLE);
        }

        @Override
        public void init(Session session) {
            HugeKeys[] columns = new HugeKeys[] {
                    HugeKeys.INDEX_LABEL_NAME,
                    HugeKeys.PROPERTY_VALUES,
                    HugeKeys.ELEMENT_IDS};

            HugeKeys[] primaryKeys = new HugeKeys[] {
                    HugeKeys.INDEX_LABEL_NAME,
                    HugeKeys.PROPERTY_VALUES
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
            return ImmutableList.of(formatKey(HugeKeys.INDEX_LABEL_NAME));
        }

        @Override
        public void insert(CassandraBackendEntry.Row entry) {
            Update update = QueryBuilder.update(super.table);

            update.where(QueryBuilder.eq(formatKey(HugeKeys.INDEX_LABEL_NAME),
                    entry.column(HugeKeys.INDEX_LABEL_NAME)));
            update.with(QueryBuilder.append(formatKey(HugeKeys.ELEMENT_IDS),
                    entry.column(HugeKeys.ELEMENT_IDS)));
            update.where(QueryBuilder.eq(formatKey(HugeKeys.PROPERTY_VALUES),
                    entry.column(HugeKeys.PROPERTY_VALUES)));

            super.batch.add(update);
        }

        @Override
        public void delete(CassandraBackendEntry.Row entry) {
            Update update = QueryBuilder.update(super.table);

            update.where(QueryBuilder.eq(formatKey(HugeKeys.INDEX_LABEL_NAME),
                    entry.column(HugeKeys.INDEX_LABEL_NAME)));
            update.with(QueryBuilder.remove(formatKey(HugeKeys.ELEMENT_IDS),
                    entry.column(HugeKeys.ELEMENT_IDS)));
            update.where(QueryBuilder.eq(formatKey(HugeKeys.PROPERTY_VALUES),
                    entry.column(HugeKeys.PROPERTY_VALUES)));

            super.batch.add(update);
        }

        @Override
        protected CassandraBackendEntry result2Entry(HugeType type, Row row) {
            CassandraBackendEntry entry = new CassandraBackendEntry(type);

            entry.column(HugeKeys.INDEX_LABEL_NAME,
                    row.getString(formatKey(HugeKeys.INDEX_LABEL_NAME)));
            entry.column(HugeKeys.PROPERTY_VALUES,
                    row.getString(formatKey(HugeKeys.PROPERTY_VALUES)));
            entry.column(HugeKeys.ELEMENT_IDS, JsonUtil.toJson(
                    row.getSet(formatKey(HugeKeys.ELEMENT_IDS), String.class)));

            return entry;
        }
    }
}
