package com.baidu.hugegraph.backend.store.cassandra;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.Condition.Relation;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.query.Query.Order;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.CopyUtil;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
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

    protected String table;
    protected BatchStatement batch;

    public CassandraTable(String table) {
        this.table = table;
        this.batch = new BatchStatement();
    }

    public Iterable<BackendEntry> query(Session session, Query query) {
        List<BackendEntry> rs = new LinkedList<>();

        if (query.limit() == 0 && query.limit() != Query.NO_LIMIT) {
            logger.debug("return empty result(limit=0) for query {}", query);
            return rs;
        }

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
            select.limit((int) query.limit());
        }

        // NOTE: Cassandra does not support query.offset()
        if (query.offset() != 0) {
            logger.warn("Query offset is not supported currently"
                    + " on Cassandra strore, it will be ignored");
        }

        // order-by
        for (Map.Entry<HugeKeys, Order> order : query.orders().entrySet()) {
            String name = formatKey(order.getKey());
            if (order.getValue() == Order.ASC) {
                select.orderBy(QueryBuilder.asc(name));
            } else {
                assert order.getValue() == Order.DESC;
                select.orderBy(QueryBuilder.desc(name));
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

        List<String> nameParts = this.idColumnName();

        List<List<String>> ids = new ArrayList<>(query.ids().size());
        for (Id id : query.ids()) {
            List<String> idParts = this.idColumnValue(id);
            if (nameParts.size() != idParts.size()) {
                throw new BackendException(String.format(
                        "Unsupported ID format: '%s' (should contain %s)",
                        id, nameParts));
            }
            ids.add(idParts);
        }

        // query only by partition-key
        if (nameParts.size() == 1) {
            List<String> idList = new ArrayList<>(ids.size());
            for (List<String> id : ids) {
                assert id.size() == 1;
                idList.add(id.get(0));
            }
            select.where(QueryBuilder.in(nameParts.get(0), idList));
            return ImmutableList.of(select);
        }

        // query by partition-key + clustering-key
        // NOTE: Error if multi-column IN clause include partition key:
        // error: multi-column relations can only be applied to clustering
        // columns when using: select.where(QueryBuilder.in(names, idList));
        // so we use multi-query instead of IN
        List<Select> selections = new ArrayList<Select>(ids.size());
        for (List<String> id : ids) {
            assert nameParts.size() == id.size();
            // NOTE: there is no Select.clone(), just use copy instead
            Select idSelection = CopyUtil.copy(select,
                    QueryBuilder.select().from(this.table));
            // NOTE: concat with AND relation
            // like: pk = id and ck1 = v1 and ck2 = v2
            for (int i = 0; i < nameParts.size(); i++) {
                idSelection.where(QueryBuilder.eq(nameParts.get(i), id.get(i)));
            }
            selections.add(idSelection);
        }
        return selections;
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
                String msg = "Unsupported condition: " + condition;
                throw new AssertionError(msg);
        }
    }

    protected static Clause relation2Cql(Relation relation) {
        String key = relation.key().toString();
        Object value = relation.value();

        // serialize value (TODO: should move to Serializer)
        if (value instanceof Id) {
            value = ((Id) value).asString();
        } else if (value instanceof Direction) {
            value = ((Direction) value).name();
        }

        switch (relation.relation()) {
            case EQ:
                return QueryBuilder.eq(key, value);
            case GT:
                return QueryBuilder.gt(key, value);
            case GTE:
                return QueryBuilder.gte(key, value);
            case LT:
                return QueryBuilder.lt(key, value);
            case LTE:
                return QueryBuilder.lte(key, value);
            case HAS_KEY:
                return QueryBuilder.containsKey(key, value);
            case NEQ:
            default:
                throw new AssertionError("Unsupported relation: " + relation);
        }
    }

    protected List<BackendEntry> results2Entries(HugeType resultType,
                                                 ResultSet results) {
        List<BackendEntry> entries = new LinkedList<>();

        Iterator<Row> iterator = results.iterator();
        while (iterator.hasNext()) {
            Row row = iterator.next();
            entries.add(result2Entry(resultType, row));
        }

        return this.mergeEntries(entries);
    }

    protected CassandraBackendEntry result2Entry(HugeType type, Row row) {
        CassandraBackendEntry entry = new CassandraBackendEntry(type);

        List<Definition> cols = row.getColumnDefinitions().asList();
        for (Definition col : cols) {
            String name = col.getName();
            Object value = row.getObject(name);

            entry.column(parseKey(name), value);
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

    protected String formatKey(HugeKeys key) {
        return key.name();
    }

    protected HugeKeys parseKey(String name) {
        return HugeKeys.valueOf(name.toUpperCase());
    }

    public void insert(CassandraBackendEntry.Row entry) {
        assert entry.columns().size() > 0;
        Insert insert = QueryBuilder.insertInto(this.table);

        for (Map.Entry<HugeKeys, Object> c : entry.columns().entrySet()) {
            insert.value(this.formatKey(c.getKey()), c.getValue());
        }

        this.batch.add(insert);
    }

    public void delete(CassandraBackendEntry.Row entry) {
        // delete just by id
        if (entry.columns().isEmpty()) {
            List<String> idNames = this.idColumnName();
            List<String> idValues = this.idColumnValue(entry.id());
            assert idNames.size() == idValues.size();

            Delete delete = QueryBuilder.delete().from(this.table);
            for (int i = 0; i < idNames.size(); i++) {
                delete.where(QueryBuilder.eq(idNames.get(i), idValues.get(i)));
            }

            this.batch.add(delete);
        }
        // delete just by column keys
        // TODO: delete by id + keys(like index element-ids))
        else {
            // NOTE: there are multi deletions if delete by id + keys
            Delete delete = QueryBuilder.delete().from(this.table);
            for (Map.Entry<HugeKeys, Object> c : entry.columns().entrySet()) {
                // TODO: should support other filters (like containsKey)
                delete.where(QueryBuilder.eq(
                        formatKey(c.getKey()),
                        c.getValue()));
            }

            this.batch.add(delete);
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
        DataType[] columnTypes = new DataType[columns.length];
        for (int i = 0; i < columns.length; i++) {
            columnTypes[i] = DataType.text();
        }
        this.createTable(session, columns, columnTypes, primaryKeys);
    }

    protected void createTable(Session session,
                               HugeKeys[] columns,
                               DataType[] columnTypes,
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
        this.createTable(session, columns, columnTypes, partitionKeys, clusterKeys);
    }

    protected void createTable(Session session,
                               HugeKeys[] columns,
                               HugeKeys[] pKeys,
                               HugeKeys[] cKeys) {
        DataType[] columnTypes = new DataType[columns.length];
        for (int i = 0; i < columns.length; i++) {
            columnTypes[i] = DataType.text();
        }
        this.createTable(session, columns, columnTypes, pKeys, cKeys);
    }

    protected void createTable(Session session,
                               HugeKeys[] columns,
                               DataType[] columnTypes,
                               HugeKeys[] pKeys,
                               HugeKeys[] cKeys) {

        assert (columns.length == columnTypes.length);

        StringBuilder sb = new StringBuilder(128 + columns.length * 64);

        // table
        sb.append("CREATE TABLE IF NOT EXISTS ");
        sb.append(this.table);
        sb.append("(");

        // columns
        for (int i = 0; i < columns.length; i++) {
            // column name
            sb.append(formatKey(columns[i]));
            sb.append(" ");
            // column type
            sb.append(columnTypes[i].asFunctionParameterString());
            sb.append(", ");
        }

        // primary keys
        sb.append("PRIMARY KEY (");

        // partition keys
        sb.append("(");
        for (HugeKeys i : pKeys) {
            if (i != pKeys[0]) {
                sb.append(", ");
            }
            sb.append(formatKey(i));
        }
        sb.append(")");

        // clustering keys
        for (HugeKeys i : cKeys) {
            sb.append(", ");
            sb.append(formatKey(i));
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

    protected void createIndex(Session session, String indexName, HugeKeys column) {

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE INDEX IF NOT EXISTS ");
        sb.append(indexName);
        sb.append(" ON ");
        sb.append(this.table);
        sb.append("(");
        sb.append(formatKey(column));
        sb.append(");");

        logger.info("create index: {}", sb);
        session.execute(sb.toString());
    }

    /*************************** abstract methods ***************************/

    public abstract void init(Session session);

    public void clear(Session session) {
        this.dropTable(session);
    }
}
