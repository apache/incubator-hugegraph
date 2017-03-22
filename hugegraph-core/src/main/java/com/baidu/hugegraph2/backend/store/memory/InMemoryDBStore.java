package com.baidu.hugegraph2.backend.store.memory;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph2.backend.BackendException;
import com.baidu.hugegraph2.backend.id.Id;
import com.baidu.hugegraph2.backend.store.BackendEntry;
import com.baidu.hugegraph2.backend.store.BackendStore;

/**
 * Created by jishilei on 17/3/19.
 */
public class InMemoryDBStore implements BackendStore {

    private static final Logger logger = LoggerFactory.getLogger(InMemoryDBStore.class);

    private final String name;
    private final ConcurrentNavigableMap<Object, BackendEntry> store;

    public InMemoryDBStore(final String name){
        logger.info("init: " + name);
        this.name = name;
        this.store = new ConcurrentSkipListMap<Object, BackendEntry>();
    }

    @Override
    public void mutate(Collection<BackendEntry> additions, Set<Id> deletions) {
        logger.info("mutate(): additions {}, deletions {}",
                additions.size(), deletions.size());

        additions.forEach((entry)->{
            logger.info("[store {}] add entry: {}", this.name, entry);
            store.put(entry.id(),entry);
        });

        deletions.forEach((k)->{
            logger.info("[store {}] remove id: {}", this.name, k.asString());
            store.remove(k);
        });
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void clear() {
        logger.info("clear()");
        store.clear();
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
