package com.baidu.hugegraph2.backend.store.memory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph2.backend.BackendException;
import com.baidu.hugegraph2.backend.id.Id;
import com.baidu.hugegraph2.backend.query.Query;
import com.baidu.hugegraph2.backend.serializer.TextBackendEntry;
import com.baidu.hugegraph2.backend.store.BackendEntry;
import com.baidu.hugegraph2.backend.store.BackendStore;
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
        return store.get(id);
    }

    @Override
    public void delete(Id id) {
        store.remove(id);
    }

    @Override
    public void mutate(Collection<BackendEntry> additions, Set<Id> deletions) {
        additions.forEach((entry) -> {
            logger.info("[store {}] add entry: {}", this.name, entry);
            this.store.put(entry.id(), entry);
        });

        deletions.forEach((k) -> {
            logger.info("[store {}] remove id: {}", this.name, k.asString());
            this.store.remove(k);
        });
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public void clear() {
        logger.info("clear()");
        this.store.clear();
    }

    @Override
    public void close() throws BackendException {
        logger.info("close()");
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
