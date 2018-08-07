package com.baidu.hugegraph.backend.store.cassandra;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.BackendStoreProvider;
import com.google.common.base.Preconditions;

public class CassandraStoreProvider implements BackendStoreProvider {
    private static final Logger logger = LoggerFactory.getLogger(CassandraStoreProvider.class);

    private final ConcurrentHashMap<String, BackendStore> stores;

    public CassandraStoreProvider() {
        this.stores = new ConcurrentHashMap<String, BackendStore>();
    }

    @Override
    public BackendStore open(final String name) throws BackendException {
        logger.info("BackendStore open [ " + name + " ] ");

        if (!this.stores.containsKey(name)) {
            this.stores.putIfAbsent(name, new CassandraStore(name));
        }
        BackendStore store = this.stores.get(name);
        Preconditions.checkNotNull(store);
        return store;
    }

    @Override
    public void close() throws BackendException {
        for (BackendStore store : this.stores.values()) {
            store.close();
        }
    }

    @Override
    public void init() {
        for (BackendStore store : this.stores.values()) {
            store.init();
        }
    }

    @Override
    public void clear() throws BackendException {
        for (BackendStore store : this.stores.values()) {
            store.clear();
        }
    }

    @Override
    public String name() {
        return "memory";
    }
}
