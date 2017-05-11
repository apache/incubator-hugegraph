package com.baidu.hugegraph.backend.store.memory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGeneratorFactory;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.serializer.TextBackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.configuration.HugeConfiguration;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.type.HugeType;

/**
 * Created by jishilei on 17/3/19.
 */
public class InMemoryDBStore implements BackendStore {

    private static final Logger logger = LoggerFactory.getLogger(InMemoryDBStore.class);

    private final String name;
    private final ConcurrentNavigableMap<Id, BackendEntry> store;

    public InMemoryDBStore(final String name) {
        this.name = name;
        this.store = new ConcurrentSkipListMap<Id, BackendEntry>();
    }

    @Override
    public Iterable<BackendEntry> query(Query query) {
        Map<Id, BackendEntry> rs = null;

        // filter by type (TODO: maybe we should let all id prefix with type)
        if (SchemaElement.isSchema(query.resultType())) {
            rs = queryPrefixWith(query.resultType().code());
        } else {
            rs = queryAll();
        }

        // query by id(s)
        if (!query.ids().isEmpty()) {
            if (query.resultType() == HugeType.EDGE) {
                // query edge(in a vertex) by id
                // TODO: should covert to query by id + column when serialize
                rs = queryEdgeById(query.ids(), rs);
            } else {
                rs = queryById(query.ids(), rs);
            }
        }

        // query by condition(s)
        if (!query.conditions().isEmpty()) {
            rs = queryFilterBy(query.conditions(), rs);
        }

        logger.info("[store {}] return {} for query: {}",
                this.name, rs.values(), query);
        return rs.values();
    }

    protected Map<Id, BackendEntry> queryAll() {
        return this.store;
    }

    protected Map<Id, BackendEntry> queryPrefixWith(byte prefix) {
        String prefixString = String.format("%x", prefix);
        Map<Id, BackendEntry> entries = new HashMap<>();
        for (BackendEntry item : this.store.values()) {
            if (item.id().asString().startsWith(prefixString)) {
                entries.put(item.id(), item);
            }
        }
        return entries;
    }

    protected Map<Id, BackendEntry> queryById(
            Set<Id> ids,
            Map<Id, BackendEntry> entries) {
        assert ids.size() > 0;
        Map<Id, BackendEntry> rs = new HashMap<>();

        for (Id id : ids) {
            if (entries.containsKey(id)) {
                rs.put(id, entries.get(id));
            }
        }
        return rs;
    }

    private Map<Id, BackendEntry> queryEdgeById(
            Set<Id> ids,
            Map<Id, BackendEntry> entries) {
        assert ids.size() > 0;
        Map<Id, BackendEntry> rs = new HashMap<>();

        for (Id id : ids) {
            String[] parts = SplicingIdGenerator.split(id);
            Id entryId = IdGeneratorFactory.generator().generate(parts[0]);

            // TODO: don't assume this is edge
            parts[0] = HugeType.EDGE_OUT.name();
            String column = SplicingIdGenerator.concat(parts);

            if (entries.containsKey(entryId)) {
                BackendEntry entry = entries.get(entryId);
                // TODO: Compatible with BackendEntry
                TextBackendEntry textEntry = (TextBackendEntry) entry;
                if (textEntry.contains(column)) {
                    TextBackendEntry result = new TextBackendEntry(entryId);
                    result.column(column, textEntry.column(column));
                    rs.put(entryId, result);
                }
            }
        }

        return rs;
    }


    protected Map<Id, BackendEntry> queryFilterBy(
            List<Condition> conditions,
            Map<Id, BackendEntry> entries) {
        assert conditions.size() > 0;

        Map<Id, BackendEntry> rs = new HashMap<>();

        for (BackendEntry entry : entries.values()) {
            // query by conditions
            boolean matched = true;
            for (Condition c : conditions) {
                if (!matchCondition(entry, c)) {
                    // TODO: deal with others Condition like: and, or...
                    matched = false;
                    break;
                }
            }
            if (matched) {
                rs.put(entry.id(), entry);
            }
        }
        return rs;
    }

    private static boolean matchCondition(BackendEntry item, Condition c) {
        // TODO: Compatible with BackendEntry
        TextBackendEntry entry = (TextBackendEntry) item;

        // not supported by memory
        if (!(c instanceof Condition.Relation)) {
            throw new BackendException("Unsupported condition: " + c);
        }

        Condition.Relation r = (Condition.Relation) c;
        String key = r.key().toString();

        // TODO: deal with others Relation like: <, >=, ...
        if (r.relation() == Condition.RelationType.HAS_KEY) {
            return entry.contains(key);
        } else if (r.relation() == Condition.RelationType.EQ) {
            return entry.contains(key, r.value().toString());
        } else if (entry.contains(key)) {
            return r.test(entry.column(key));
        }
        return false;
    }

    @Override
    public void mutate(BackendMutation mutation) {
        mutation.additions().forEach((entry) -> {
            logger.info("[store {}] add entry: {}", this.name, entry);
            this.store.put(entry.id(), entry);
        });

        mutation.deletions().forEach((k) -> {
            logger.info("[store {}] remove id: {}", this.name, k.toString());
            // remove by id (TODO: support remove by id + condition)
            this.store.remove(k.id());
        });
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public void open(HugeConfiguration config) {
        logger.info("open()");
    }

    @Override
    public void close() throws BackendException {
        logger.info("close()");
    }

    @Override
    public void init() {
        // TODO Auto-generated method stub

    }

    @Override
    public void clear() {
        logger.info("clear()");
        this.store.clear();
    }

    @Override
    public void beginTx() {
        // TODO Auto-generated method stub

    }

    @Override
    public void commitTx() {
        // TODO Auto-generated method stub

    }

    @Override
    public void rollbackTx() {
        // TODO Auto-generated method stub

    }

    @Override
    public String toString() {
        return this.name;
    }
}
