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
                    HugeKeys.MULTIPLICITY,
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
                    HugeKeys.PROPERTY_KEY,
                    HugeKeys.PROPERTY_VALUE
            };

            HugeKeys[] partitionKeys = new HugeKeys[] {
                    HugeKeys.LABEL,
                    HugeKeys.PRIMARY_VALUES
            };

            HugeKeys[] clusterKeys = new HugeKeys[] {
                    HugeKeys.PROPERTY_KEY
            };

            super.createTable(session, columns, partitionKeys, clusterKeys);
            super.createIndex(session, "vertices_label_index", HugeKeys.LABEL);
        }

        @Override
        protected List<String> idColumnName() {
            return ImmutableList.of(
                    HugeKeys.LABEL.name(),
                    HugeKeys.PRIMARY_VALUES.name());
        }

        @Override
        protected List<String> idColumnValue(Id id) {
            // TODO: improve Id parse()
            return ImmutableList.copyOf(SplicingIdGenerator.parse(id));
        }

        @Override
        protected boolean isColumnKey(HugeKeys key) {
            return !isCellKey(key) && !isCellValue(key);
        }

        @Override
        protected boolean isCellKey(HugeKeys key) {
            return key == HugeKeys.PROPERTY_KEY;
        }

        @Override
        protected boolean isCellValue(HugeKeys key) {
            return key == HugeKeys.PROPERTY_VALUE;
        }

        @Override
        protected HugeKeys cellValueType(HugeKeys key) {
            assert key == HugeKeys.PROPERTY_KEY;
            return HugeKeys.PROPERTY_VALUE;
        }

        @Override
        protected List<BackendEntry> mergeEntries(List<BackendEntry> entries) {
            // merge properties with same id into a vertex
            Map<Id, CassandraBackendEntry> vertices = new HashMap<>();

            for (BackendEntry i : entries) {
                CassandraBackendEntry entry = (CassandraBackendEntry) i;
                Id id = SplicingIdGenerator.splicing(
                        entry.column(HugeKeys.LABEL),
                        entry.column(HugeKeys.PRIMARY_VALUES));
                if (!vertices.containsKey(id)) {
                    entry.id(id);
                    vertices.put(id, entry);
                } else {
                    assert entry.cells().size() == 1;
                    vertices.get(id).column(entry.cells().get(0));
                }
            }
            return ImmutableList.copyOf(vertices.values());
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
                    HugeKeys.PROPERTY_KEY,
                    HugeKeys.PROPERTY_VALUE};

            HugeKeys[] primaryKeys = new HugeKeys[] {
                    HugeKeys.SOURCE_VERTEX,
                    HugeKeys.DIRECTION,
                    HugeKeys.LABEL,
                    HugeKeys.SORT_VALUES,
                    HugeKeys.TARGET_VERTEX,
                    HugeKeys.PROPERTY_KEY};

            super.createTable(session, columns, primaryKeys);
            super.createIndex(session, "edges_label_index", HugeKeys.LABEL);
        }

        @Override
        protected List<String> idColumnName() {
            if (KEYS_STRING == null) {
                KEYS_STRING = new ArrayList<>(KEYS.length);
                for (HugeKeys k : KEYS) {
                    KEYS_STRING.add(k.name());
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
        protected boolean isColumnKey(HugeKeys key) {
            return !isCellKey(key) && !isCellValue(key);
        }

        @Override
        protected boolean isCellKey(HugeKeys key) {
            return key == HugeKeys.PROPERTY_KEY;
        }

        @Override
        protected boolean isCellValue(HugeKeys key) {
            return key == HugeKeys.PROPERTY_VALUE;
        }

        @Override
        protected HugeKeys cellValueType(HugeKeys key) {
            assert key == HugeKeys.PROPERTY_KEY;
            return HugeKeys.PROPERTY_VALUE;
        }

        @Override
        protected List<BackendEntry> mergeEntries(List<BackendEntry> entries) {
            // TODO: merge rows before calling result2Entry()

            // merge edges into vertex
            Map<Id, CassandraBackendEntry> vertices = new HashMap<>();

            for (BackendEntry i : entries) {
                CassandraBackendEntry entry = (CassandraBackendEntry) i;
                Id srcVertexId = IdGeneratorFactory.generator().generate(
                        entry.column(HugeKeys.SOURCE_VERTEX));
                if (!vertices.containsKey(srcVertexId)) {
                    CassandraBackendEntry vertex = new CassandraBackendEntry(
                            HugeType.VERTEX, srcVertexId);
                    // set vertex label and pv(assume vertex id with a label)
                    // TODO: improve Id parse()
                    String[] idParts = SplicingIdGenerator.parse(srcVertexId);
                    vertex.column(HugeKeys.LABEL, idParts[0]);
                    vertex.column(HugeKeys.PRIMARY_VALUES, idParts[1]);

                    vertices.put(srcVertexId, vertex);
                }
                // add edge into vertex as a sub row
                vertices.get(srcVertexId).subRow(entry.row());
            }

            // merge edge properties into edge
            for (CassandraBackendEntry vertex : vertices.values()) {
                Map<String, CassandraBackendEntry.Row> egdes = new HashMap<>();
                for (CassandraBackendEntry.Row row : vertex.subRows()) {
                    String edgeId = formatEdgeId(row);
                    if (!egdes.containsKey(edgeId)) {
                        egdes.put(edgeId, row);
                    } else {
                        assert row.cells().size() == 1;
                        egdes.get(edgeId).cell(row.cells().get(0));
                    }
                }
                vertex.subRows(ImmutableList.copyOf(egdes.values()));
            }

            return ImmutableList.copyOf(vertices.values());
        }

        protected static String formatEdgeId(CassandraBackendEntry.Row row) {
            String[] values = new String[KEYS.length];
            for (int i = 0; i < KEYS.length; i++) {
                values[i] = row.key(KEYS[i]);
            }
            // TODO: improve Id concat()
            return SplicingIdGenerator.concat(values).asString();
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
            return ImmutableList.of(HugeKeys.PROPERTY_VALUES.name());
        }

        @Override
        public void insert(CassandraBackendEntry.Row entry) {
            Update update = QueryBuilder.update(super.table);

            update.with(QueryBuilder.append(HugeKeys.ELEMENT_IDS.name(),
                    entry.key(HugeKeys.ELEMENT_IDS)));
            update.where(QueryBuilder.eq(HugeKeys.INDEX_LABEL_NAME.name(),
                    entry.key(HugeKeys.INDEX_LABEL_NAME)));
            update.where(QueryBuilder.eq(HugeKeys.PROPERTY_VALUES.name(),
                    entry.key(HugeKeys.PROPERTY_VALUES)));

            super.batch.add(update);
        }

        @Override
        public void delete(CassandraBackendEntry.Row entry) {
            Update update = QueryBuilder.update(super.table);

            update.with(QueryBuilder.remove(HugeKeys.ELEMENT_IDS.name(),
                    entry.key(HugeKeys.ELEMENT_IDS)));
            update.where(QueryBuilder.eq(HugeKeys.INDEX_LABEL_NAME.name(),
                    entry.key(HugeKeys.INDEX_LABEL_NAME)));
            update.where(QueryBuilder.eq(HugeKeys.PROPERTY_VALUES.name(),
                    entry.key(HugeKeys.PROPERTY_VALUES)));

            super.batch.add(update);
        }

        @Override
        protected CassandraBackendEntry result2Entry(HugeType type, Row row) {
            CassandraBackendEntry entry = new CassandraBackendEntry(type);

            entry.column(HugeKeys.PROPERTY_VALUES,
                    row.getString(HugeKeys.PROPERTY_VALUES.name()));
            entry.column(HugeKeys.INDEX_LABEL_NAME,
                    row.getString(HugeKeys.INDEX_LABEL_NAME.name()));
            entry.column(HugeKeys.ELEMENT_IDS,
                    JsonUtil.toJson(row.getSet(HugeKeys.ELEMENT_IDS.name(), String.class)));

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
            return ImmutableList.of(HugeKeys.INDEX_LABEL_NAME.name());
        }

        @Override
        public void insert(CassandraBackendEntry.Row entry) {
            Update update = QueryBuilder.update(super.table);

            update.where(QueryBuilder.eq(HugeKeys.INDEX_LABEL_NAME.name(),
                    entry.key(HugeKeys.INDEX_LABEL_NAME)));
            update.with(QueryBuilder.append(HugeKeys.ELEMENT_IDS.name(),
                    entry.key(HugeKeys.ELEMENT_IDS)));
            update.where(QueryBuilder.eq(HugeKeys.PROPERTY_VALUES.name(),
                    entry.key(HugeKeys.PROPERTY_VALUES)));

            super.batch.add(update);
        }

        @Override
        public void delete(CassandraBackendEntry.Row entry) {
            Update update = QueryBuilder.update(super.table);

            update.where(QueryBuilder.eq(HugeKeys.INDEX_LABEL_NAME.name(),
                    entry.key(HugeKeys.INDEX_LABEL_NAME)));
            update.with(QueryBuilder.remove(HugeKeys.ELEMENT_IDS.name(),
                    entry.key(HugeKeys.ELEMENT_IDS)));
            update.where(QueryBuilder.eq(HugeKeys.PROPERTY_VALUES.name(),
                    entry.key(HugeKeys.PROPERTY_VALUES)));

            super.batch.add(update);
        }

        @Override
        protected CassandraBackendEntry result2Entry(HugeType type, Row row) {
            CassandraBackendEntry entry = new CassandraBackendEntry(type);

            entry.column(HugeKeys.INDEX_LABEL_NAME,
                    row.getString(HugeKeys.INDEX_LABEL_NAME.name()));
            entry.column(HugeKeys.PROPERTY_VALUES,
                    row.getString(HugeKeys.PROPERTY_VALUES.name()));
            entry.column(HugeKeys.ELEMENT_IDS,
                    JsonUtil.toJson(row.getSet(HugeKeys.ELEMENT_IDS.name(), String.class)));

            return entry;
        }
    }
}
