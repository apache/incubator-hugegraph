package com.baidu.hugegraph.backend.store;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.util.E;

public abstract class AbstractBackendStoreProvider
        implements BackendStoreProvider {

    protected String name;
    protected Map<String, BackendStore> stores;

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public void open(String name) {
        E.checkNotNull(name, "store name");

        this.name = name;
        this.stores = new ConcurrentHashMap<>();
    }

    @Override
    public void close() throws BackendException {
        for (BackendStore store : this.stores.values()) {
            // TODO: catch exceptions here
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
}
