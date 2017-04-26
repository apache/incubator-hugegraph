package com.baidu.hugegraph.backend.store.cassandra;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.Condition.Relation;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.query.Query.Order;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.type.HugeTypes;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.querybuilder.Clause;
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
        List<BackendEntry> rs = new LinkedList<>();

        List<Select> selections = query2Select(query);

        for (Select selection : selections) {
            ResultSet results = session.execute(selection);
            rs.addAll(this.results2Entries(query.resultType(), results));
        }

        logger.debug("return {} for query {}", rs, query);
        return rs;
    }

    protected List<Select> query2Select(Query query) {
        // table
        Select select = QueryBuilder.select().from(this.table);

        // limit
        if (query.limit() != Query.NO_LIMIT) {
            select.limit(query.limit());
        }

        // NOTE: Cassandra does not support query.offset()
        if (query.offset() != 0) {
            logger.warn("Query offset are not currently supported"
                    + " on Cassandra strore, it will be ignored");
        }

        // order-by
        for (Entry<HugeKeys, Order> order : query.orders().entrySet()) {
            if (order.getValue() == Order.ASC) {
                select.orderBy(QueryBuilder.asc(order.getKey().name()));
            } else {
                assert order.getValue() == Order.DESC;
                select.orderBy(QueryBuilder.desc(order.getKey().name()));
            }
        }

        // by id
        List<Select> ids = this.queryId2Select(query, select);

        if (query.conditions().isEmpty()) {
            logger.debug("query only by id(s): {}", ids);
            return ids;
        } else {
            List<Select> conds = new ArrayList<Select>(ids.size());
            for (Select selection : ids) {
                // by condition
                conds.addAll(this.queryCondition2Select(query, selection));
            }
            logger.debug("query by conditions: {}", conds);
            return conds;
        }
    }

    protected List<Select> queryId2Select(Query query, Select select) {
        // query by id(s)
        if (query.ids().isEmpty()) {
            return ImmutableList.of(select);
        }

        List<List<String>> ids = new ArrayList<>(query.ids().size());
        for (Id id : query.ids()) {
            ids.add(this.idColumnValue(id));
        }

        List<String> names = this.idColumnName();
        // query only by partition-key
        if (names.size() == 1) {
            List<String> idList = new ArrayList<>(ids.size());
            for (List<String> id : ids) {
                assert id.size() == 1;
                idList.add(id.get(0));
            }
            select.where(QueryBuilder.in(names.get(0), idList));
            return ImmutableList.of(select);
        }
        // query by partition-key + cluster-key
        else {
            // NOTE: Error of multi-column IN when including partition key:
            // error: multi-column relations can only be applied to cluster columns
            // when using: select.where(QueryBuilder.in(names, idList));
            // so we use multi-query instead
            List<Select> selections = new ArrayList<Select>(ids.size());
            for (List<String> id : ids) {
                assert names.size() == id.size();
                // TODO: implement select.clone() to support query by multi-id
                // Select idSelection = select.clone();
                if (ids.size() > 1) {
                    throw new BackendException(
                            "Currently not support query by multi ids with ck");
                }
                // Currently just assign selection instead of clone it
                Select idSelection = select;
                // NOTE: concat with AND relation
                // like: pk = id and ck1 = v1 and ck2 = v2
                for (int i = 0; i < names.size(); i++) {
                    idSelection.where(QueryBuilder.eq(names.get(i), id.get(i)));
                }
                selections.add(idSelection);
            }
            return selections;
        }
    }

    protected Collection<Select> queryCondition2Select(
            Query query, Select select) {
        // query by conditions
        List<Condition> conditions = query.conditions();
        for (Condition condition : conditions) {
            select.where(condition2Cql(condition));
        }
        return ImmutableList.of(select);
    }

    protected static Clause condition2Cql(Condition condition) {
        switch (condition.type()) {
            case AND:
                Condition.And and = (Condition.And) condition;
                // TODO: return QueryBuilder.and(and.left(), and.right());
                Clause left = condition2Cql(and.left());
                Clause right = condition2Cql(and.right());
                return (Clause) QueryBuilder.raw(String.format("%s AND %s",
                        left, right));
            case OR:
                throw new BackendException("Not support OR currently");
            case RELATION:
                Condition.Relation r = (Condition.Relation) condition;
                return relation2Cql(r);
            default:
                String msg = "Not supported condition: " + condition;
                throw new AssertionError(msg);
        }
    }

    protected static Clause relation2Cql(Relation relation) {
        Relation r = relation;
        switch (relation.relation()) {
            case EQ:
                return QueryBuilder.eq(r.key().name(), r.value());
            case GT:
                return QueryBuilder.gt(r.key().name(), r.value());
            case GTE:
                return QueryBuilder.gte(r.key().name(), r.value());
            case LT:
                return QueryBuilder.lt(r.key().name(), r.value());
            case LTE:
                return QueryBuilder.lte(r.key().name(), r.value());
            case NEQ:
            default:
                throw new AssertionError("Not supported relation: " + r);
        }
    }

    protected List<BackendEntry> results2Entries(HugeTypes resultType,
                                                 ResultSet results) {
        List<BackendEntry> entries = new LinkedList<>();

        Iterator<Row> iterator = results.iterator();
        while (iterator.hasNext()) {
            Row row = iterator.next();
            entries.add(result2Entry(resultType, row));
        }

        return this.mergeEntries(entries);
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
                // TODO: we should improve this code,
                // let Vertex and Edge implement results2Entries()
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

    protected List<String> idColumnName() {
        return ImmutableList.of(HugeKeys.NAME.name());
    }

    protected List<String> idColumnValue(Id id) {
        return ImmutableList.of(id.asString());
    }

    protected List<BackendEntry> mergeEntries(List<BackendEntry> entries) {
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
        // TODO: to make it more clear.
        assert (primaryKeys.length > 0);
        HugeKeys[] partitionKeys = new HugeKeys[] {primaryKeys[0]};
        HugeKeys[] clusterKeys = null;
        if (primaryKeys.length > 1) {
            clusterKeys = Arrays.copyOfRange(
                    primaryKeys, 1, primaryKeys.length);
        } else {
            clusterKeys = new HugeKeys[] {};
        }
        this.createTable(session, columns, partitionKeys, clusterKeys);
    }

    protected void createTable(Session session,
                               HugeKeys[] columns,
                               HugeKeys[] pKeys,
                               HugeKeys[] cKeys) {

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

        // partition keys
        sb.append("(");
        for (HugeKeys i : pKeys) {
            if (i != pKeys[0]) {
                sb.append(", ");
            }
            sb.append(i.name());
        }
        sb.append(")");

        // clustering keys
        for (HugeKeys i : cKeys) {
            sb.append(", ");
            sb.append(i.name());
        }

        // end of primary keys
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

    protected void createIndex(Session session,
                               Map<String, HugeKeys> indexColumns) {

        StringBuilder sb = new StringBuilder();
        indexColumns.forEach((indexName, column) -> {
            sb.append("CREATE INDEX ");
            sb.append(indexName);
            sb.append(" ON ");
            sb.append(this.table);
            sb.append("(");
            sb.append(column.name());
            sb.append(");");
        });

        logger.info("create index: {}", sb);
        session.execute(sb.toString());
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
//                    HugeKeys.INDEX_NAMES
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

            Map<String, HugeKeys> indexColumns = new HashMap<>();
            indexColumns.put("vertices_label_index", HugeKeys.LABEL);

            super.createTable(session, columns, partitionKeys, clusterKeys);
            super.createIndex(session, indexColumns);
        }

        @Override
        protected List<String> idColumnName() {
            return ImmutableList.of(
                    HugeKeys.LABEL.name(),
                    HugeKeys.PRIMARY_VALUES.name());
        }

        @Override
        protected List<String> idColumnValue(Id id) {
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
                String idStr = SplicingIdGenerator.splicing(
                        entry.column(HugeKeys.LABEL),
                        entry.column(HugeKeys.PRIMARY_VALUES));
                Id id = IdGeneratorFactory.generator().generate(idStr);
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

            Map<String, HugeKeys> indexColumns = new HashMap<>();
            indexColumns.put("edges_label_index", HugeKeys.LABEL);

            super.createTable(session, columns, primaryKeys);
            super.createIndex(session, indexColumns);
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
            return ImmutableList.copyOf(SplicingIdGenerator.split(id));
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
                            HugeTypes.VERTEX, srcVertexId);
                    // set vertex label and pv(assume vertex id with a label)
                    // TODO: improve Id split()
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
            return SplicingIdGenerator.concat(values);
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

            super.createTable(session, columns, primaryKeys);
        }

        @Override
        protected List<String> idColumnName() {
            return ImmutableList.of(HugeKeys.PROPERTY_VALUES.name());
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

            super.createTable(session, columns, primaryKeys);
        }

        @Override
        protected List<String> idColumnName() {
            return ImmutableList.of(HugeKeys.INDEX_LABEL_NAME.name());
        }
    }
}
