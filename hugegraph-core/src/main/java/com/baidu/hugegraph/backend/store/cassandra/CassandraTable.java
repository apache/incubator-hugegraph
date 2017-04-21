package com.baidu.hugegraph.backend.store.cassandra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGeneratorFactory;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.type.HugeTypes;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.google.common.collect.ImmutableList;

public abstract class CassandraTable {

    private static final Logger logger = LoggerFactory.getLogger(CassandraTable.class);

    private String table;
    private BatchStatement batch;

    public CassandraTable(String table) {
        this.table = table;
        this.batch = new BatchStatement();
    }

    public Iterable<BackendEntry> query(Session session, Query query) {
        List<BackendEntry> entries = new LinkedList<>();

        Select select = query2Select(query);
        ResultSet results = session.execute(select);

        Iterator<Row> iterator = results.iterator();
        while (iterator.hasNext()) {
            Row row = iterator.next();
            entries.add(result2Entry(query.resultType(), row));
        }

        Iterable<BackendEntry> rs = this.mergeEntries(entries);
        logger.debug("Cassandra return {} for query {}",
                rs, select);
        return rs;
    }

    protected Select query2Select(Query query) {
        // table
        Select select = QueryBuilder.select().from(this.table);
        // limit
        select.limit(query.limit());

        // NOTE: Cassandra does not support query.offset()
        // TODO: deal with order-by: QueryBuilder.asc(columnName)

        // query by id(s)
        List<String> idList = new ArrayList<>(query.ids().size());
        for (Id id : query.ids()) {
            idList.add(id.asString());
        }
        select.where(QueryBuilder.in(this.idColumn().name(), idList));

        // TODO: query by conditions

        return select;
    }

    protected CassandraBackendEntry result2Entry(HugeTypes type, Row row) {
        CassandraBackendEntry entry = new CassandraBackendEntry(type);

        List<Definition> cols = row.getColumnDefinitions().asList();
        for (Definition col : cols) {
            String name = col.getName();
            String value = row.getString(name);
            HugeKeys key = HugeKeys.valueOf(name.toUpperCase());

            if (this.isColumnKey(key)) {
                entry.column(key, value);
            } else if (this.isCellKey(key)) {
                // about key: such as prop-key, now let's get prop-value by it
                // TODO: we should improve this code, let Vertex and Edge implement result2Entry()
                HugeKeys cellKeyType = key;
                String cellKeyValue = value;
                HugeKeys cellValueType = this.cellValueType(cellKeyType);
                String cellValue = row.getString(cellValueType.name());

                entry.column(new CassandraBackendEntry.Cell(
                        cellKeyType, cellKeyValue,
                        cellValueType, cellValue));
            } else {
                assert isCellValue(key);
            }
        }

        return entry;
    }

    protected HugeKeys idColumn() {
        return HugeKeys.NAME;
    }

    protected Iterable<BackendEntry> mergeEntries(List<BackendEntry> entries) {
        return entries;
    }

    protected boolean isColumnKey(HugeKeys key) {
        return true;
    }

    protected boolean isCellKey(HugeKeys key) {
        return false;
    }

    protected boolean isCellValue(HugeKeys key) {
        return false;
    }

    protected HugeKeys cellValueType(HugeKeys key) {
        return null;
    }

    public void insert(CassandraBackendEntry.Row entry) {
        assert entry.keys().size() + entry.cells().size() > 0;

        // insert keys
        if (entry.cells().isEmpty()) {
            Insert insert = QueryBuilder.insertInto(this.table);

            for (Entry<HugeKeys, String> k : entry.keys().entrySet()) {
                insert.value(k.getKey().name(), k.getValue());
            }

            this.batch.add(insert);
        }
        // insert keys + values
        else {
            for (CassandraBackendEntry.Cell i : entry.cells()) {
                Insert insert = QueryBuilder.insertInto(this.table);

                for (Entry<HugeKeys, String> k : entry.keys().entrySet()) {
                    insert.value(k.getKey().name(), k.getValue());
                }

                insert.value(i.nameType().name(), i.name());
                insert.value(i.valueType().name(), i.value());
                this.batch.add(insert);
            }
        }
    }

    public void delete(CassandraBackendEntry.Row entry) {
        Delete delete = QueryBuilder.delete().from(this.table);
        Delete.Where where = delete.where();

        // delete just by keys
        if (entry.cells().isEmpty()) {
            for (Entry<HugeKeys, String> k : entry.keys().entrySet()) {
                where.and(QueryBuilder.eq(k.getKey().name(), k.getValue()));
            }

            this.batch.add(delete);
        }
        // delete by key + value-key (such as vertex property)
        else {
            for (CassandraBackendEntry.Cell i : entry.cells()) {

                for (Entry<HugeKeys, String> k : entry.keys().entrySet()) {
                    where.and(QueryBuilder.eq(k.getKey().name(), k.getValue()));
                }

                where.and(QueryBuilder.eq(i.nameType().name(), i.name()));
                this.batch.add(delete);
            }
        }
    }

    public void commit(Session session) {
        if (session.isClosed()) {
            throw new BackendException("Session has been closed");
        }

        try {
            logger.debug("commit statements: {}", this.batch.getStatements());
            session.execute(this.batch);
            this.batch.clear();
        } catch (InvalidQueryException e) {
            logger.error("Failed to commit statements due to:", e);
            throw new BackendException("Failed to commit statements: "
                    + this.batch.getStatements());
        }
    }

    public boolean hasChanged() {
        return this.batch.size() > 0;
    }

    protected void createTable(Session session,
            HugeKeys[] columns,
            HugeKeys[] primaryKeys) {

        StringBuilder sb = new StringBuilder(128 + columns.length * 64);

        // table
        sb.append("CREATE TABLE IF NOT EXISTS ");
        sb.append(this.table);
        sb.append("(");

        // columns
        for (HugeKeys i : columns) {
            sb.append(i.name()); // column name
            sb.append(" text, "); // column type
        }

        // primary keys
        sb.append("PRIMARY KEY (");
        for (HugeKeys i : primaryKeys) {
            if (i != primaryKeys[0]) {
                sb.append(", ");
            }
            sb.append(i.name()); // primary key name
        }
        sb.append(")");

        // end of table declare
        sb.append(");");

        logger.info("Create table: {}", sb);
        session.execute(sb.toString());
    }

    protected void dropTable(Session session) {
        logger.info("Drop table: {}", this.table);
        session.execute(SchemaBuilder.dropTable(this.table).ifExists());
    }

    /*************************** abstract methods ***************************/

    public abstract void init(Session session);

    public void clear(Session session) {
        this.dropTable(session);
    }


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
                    HugeKeys.PRIMARY_KEYS };

            HugeKeys[] primaryKeys = new HugeKeys[] { HugeKeys.NAME };

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
                    HugeKeys.FREQUENCY };

            HugeKeys[] primaryKeys = new HugeKeys[] { HugeKeys.NAME };

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
                    HugeKeys.PROPERTIES };

            HugeKeys[] primaryKeys = new HugeKeys[] { HugeKeys.NAME };

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
                     HugeKeys.ID,
                     HugeKeys.PROPERTY_KEY,
                     HugeKeys.PROPERTY_VALUE };

             HugeKeys[] primaryKeys = new HugeKeys[] {
                     HugeKeys.ID,
                     HugeKeys.PROPERTY_KEY };

            super.createTable(session, columns, primaryKeys);
        }

        @Override
        protected HugeKeys idColumn() {
            return HugeKeys.ID;
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
        protected Iterable<BackendEntry> mergeEntries(List<BackendEntry> entries) {
            // merge properties with same id into a vertex
            Map<String, CassandraBackendEntry> vertices = new HashMap<>();

            for (BackendEntry i : entries) {
                CassandraBackendEntry entry = (CassandraBackendEntry) i;
                String id = entry.column(HugeKeys.ID);
                if (!vertices.containsKey(id)) {
                    entry.id(IdGeneratorFactory.generator().generate(id));
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
                    HugeKeys.PROPERTY_VALUE };

            HugeKeys[] primaryKeys = new HugeKeys[] {
                    HugeKeys.SOURCE_VERTEX,
                    HugeKeys.DIRECTION,
                    HugeKeys.LABEL,
                    HugeKeys.SORT_VALUES,
                    HugeKeys.TARGET_VERTEX,
                    HugeKeys.PROPERTY_KEY };

           super.createTable(session, columns, primaryKeys);
        }

        @Override
        protected HugeKeys idColumn() {
            return HugeKeys.SOURCE_VERTEX;
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
        protected Iterable<BackendEntry> mergeEntries(List<BackendEntry> entries) {
            // merge edges into vertex
            Map<String, CassandraBackendEntry> vertices = new HashMap<>();

            for (BackendEntry i : entries) {
                CassandraBackendEntry entry = (CassandraBackendEntry) i;
                String srcVertex = entry.column(HugeKeys.SOURCE_VERTEX);
                if (!vertices.containsKey(srcVertex)) {
                    vertices.put(srcVertex, new CassandraBackendEntry(HugeTypes.VERTEX));
                }
                // add edge into vertex as a sub row
                vertices.get(srcVertex).subRow(entry.row());
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

        private String formatEdgeId(CassandraBackendEntry.Row row) {
            final HugeKeys[] keys = new HugeKeys[] {
                    HugeKeys.SOURCE_VERTEX,
                    HugeKeys.DIRECTION,
                    HugeKeys.LABEL,
                    HugeKeys.SORT_VALUES,
                    HugeKeys.TARGET_VERTEX };
            List<String> values = new ArrayList<>(keys.length);
            for (HugeKeys key : keys) {
                values.add(row.key(key));
            }
            return String.join("\u0001", values);
        }
    }

}
