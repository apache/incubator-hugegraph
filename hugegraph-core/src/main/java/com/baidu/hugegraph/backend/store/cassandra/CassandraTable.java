package com.baidu.hugegraph.backend.store.cassandra;

import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;

public abstract class CassandraTable {

    private static final Logger logger = LoggerFactory.getLogger(CassandraTable.class);

    private String table;
    private BatchStatement batch;

    public CassandraTable(String table) {
        this.table = table;
        this.batch = new BatchStatement();
    }

    public Iterable<BackendEntry> query(Query query) {
        // TODO Auto-generated method stub
        return null;
    }

    public void insert(CassandraBackendEntry.Row entry) {
        assert entry.keys().size() + entry.cells().size() > 0;

        // insert keys
        if (entry.cells().isEmpty()) {
            Insert insert = QueryBuilder.insertInto(this.table);

            for (Entry<HugeKeys, String> k : entry.keys().entrySet()) {
                insert.value(k.getKey().string(), k.getValue());
            }

            this.batch.add(insert);
        }
        // insert keys + values
        else {
            for (CassandraBackendEntry.Cell i : entry.cells()) {
                Insert insert = QueryBuilder.insertInto(this.table);

                for (Entry<HugeKeys, String> k : entry.keys().entrySet()) {
                    insert.value(k.getKey().string(), k.getValue());
                }

                insert.value(i.nameType().string(), i.name());
                insert.value(i.valueType().string(), i.value());
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
                where.and(QueryBuilder.eq(k.getKey().string(), k.getValue()));
            }

            this.batch.add(delete);
        }
        // delete by key + value-key (such as vertex property)
        else {
            for (CassandraBackendEntry.Cell i : entry.cells()) {

                for (Entry<HugeKeys, String> k : entry.keys().entrySet()) {
                    where.and(QueryBuilder.eq(k.getKey().string(), k.getValue()));
                }

                where.and(QueryBuilder.eq(i.nameType().string(), i.name()));
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
            sb.append(i.string()); // column name
            sb.append(" text, "); // column type
        }

        // primary keys
        sb.append("PRIMARY KEY (");
        for (HugeKeys i : primaryKeys) {
            if (i != primaryKeys[0]) {
                sb.append(", ");
            }
            sb.append(i.string()); // primary key name
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
                    HugeKeys.INDEX_TYPE,
                    HugeKeys.FIELDS
            };

            // base-type as clustering key
            HugeKeys[] primaryKeys = new HugeKeys[] {
                    HugeKeys.NAME,
                    HugeKeys.BASE_TYPE
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
                     HugeKeys.ID,
                     HugeKeys.PROPERTY_KEY,
                     HugeKeys.PROPERTY_VALUE };

             HugeKeys[] primaryKeys = new HugeKeys[] {
                     HugeKeys.ID,
                     HugeKeys.PROPERTY_KEY };

            super.createTable(session, columns, primaryKeys);
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

    }
}
