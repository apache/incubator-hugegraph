package com.baidu.hugegraph.backend.store.memory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.serializer.TextBackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendMutation;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.configuration.HugeConfiguration;
import com.google.common.collect.ImmutableList;

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
        List<BackendEntry> entries = new ArrayList<BackendEntry>();

        this.store.forEach((Object key, BackendEntry item) -> {
            // TODO: Compatible with BackendEntry
            TextBackendEntry entry = (TextBackendEntry) item;
            query.conditions().forEach((k, v) -> {
                if (entry.contains(k.toString(), v.toString())) {
                    entries.add(entry);
                }

            });

        });
        return ImmutableList.copyOf(entries);
    }

    @Override
    public BackendEntry get(Id id) {
        return this.store.get(id);
    }

    @Override
    public void delete(Id id) {
        this.store.remove(id);
    }

    @Override
    public void mutate(BackendMutation mutation) {
        mutation.additions().forEach((entry) -> {
            logger.info("[store {}] add entry: {}", this.name, entry);
            this.store.put(entry.id(), entry);
        });

        mutation.deletions().forEach((k) -> {
            logger.info("[store {}] remove id: {}", this.name, k.toString());
            this.store.remove(k);
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
}
