package com.baidu.hugegraph.backend.store.memory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.backend.store.AbstractBackendStoreProvider;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.google.common.base.Preconditions;

public class InMemoryDBStoreProvider extends AbstractBackendStoreProvider {

    private static final Logger logger = LoggerFactory.getLogger(
            InMemoryDBStoreProvider.class);

    public InMemoryDBStoreProvider(String name) {
        this.open(name);
    }

    private BackendStore load(String name) {
        logger.info("InMemoryDBStoreProvider load '{}'", name);

        if (!this.stores.containsKey(name)) {
            this.stores.putIfAbsent(name, new InMemoryDBStore(name));
        }
        BackendStore store = this.stores.get(name);
        Preconditions.checkNotNull(store);
        return store;
    }

    @Override
    public BackendStore loadSchemaStore(String name) {
        return this.load(name);
    }

    @Override
    public BackendStore loadGraphStore(String name) {
        return this.load(name);
    }

    @Override
    public BackendStore loadIndexStore(String name) {
        return this.load(name);
    }

    @Override
    public String type() {
        return "memory";
    }
}
