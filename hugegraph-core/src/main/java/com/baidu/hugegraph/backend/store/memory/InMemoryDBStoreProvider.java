package com.baidu.hugegraph.backend.store.memory;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.BackendStoreProvider;
import com.google.common.base.Preconditions;

/**
 * Created by jishilei on 17/3/19.
 */
public class InMemoryDBStoreProvider implements BackendStoreProvider {
    private static final Logger logger = LoggerFactory.getLogger(InMemoryDBStoreProvider.class);

    private final ConcurrentHashMap<String, BackendStore> stores;

    public InMemoryDBStoreProvider() {
        this.stores = new ConcurrentHashMap<String, BackendStore>();
    }

    @Override
    public BackendStore loadSchemaStore(final String name) {
        logger.info("BackendStore load [ " + name + " ] ");

        if (!this.stores.containsKey(name)) {
            this.stores.putIfAbsent(name, new InMemoryDBStore(name));
        }
        BackendStore store = this.stores.get(name);
        Preconditions.checkNotNull(store);
        return store;
    }

    @Override
    public BackendStore loadGraphStore(String name) {
        logger.info("BackendStore load [ " + name + " ] ");

        if (!this.stores.containsKey(name)) {
            this.stores.putIfAbsent(name, new InMemoryDBStore(name));
        }
        BackendStore store = this.stores.get(name);
        Preconditions.checkNotNull(store);
        return store;
    }

    @Override
    public void close() throws BackendException {
    }

    @Override
    public void init() {
    }

    @Override
    public void clear() throws BackendException {
        this.stores.forEach((String k, BackendStore store) -> {
            store.clear();
        });
    }

    @Override
    public String name() {
        return "memory";
    }
}
