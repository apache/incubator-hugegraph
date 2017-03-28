package com.baidu.hugegraph2.backend.store.memory;

import com.baidu.hugegraph2.backend.BackendException;
import com.baidu.hugegraph2.backend.store.*;
import com.google.common.base.Preconditions;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jishilei on 17/3/19.
 */
public class InMemoryDBStoreProvider implements BackendStoreProvider {
    private static final Logger logger = LoggerFactory.getLogger(InMemoryDBStoreProvider.class);

    private final ConcurrentHashMap<String, BackendStore> stores;

    public InMemoryDBStoreProvider() {
        stores = new ConcurrentHashMap<String, BackendStore>();
    }

    @Override
    public BackendStore open(final String name) throws BackendException {

        logger.info("BackendStore open [ " + name + " ] ");

        if (!stores.containsKey(name)) {
            stores.putIfAbsent(name, new InMemoryDBStore(name));
        }
        BackendStore store = stores.get(name);
        Preconditions.checkNotNull(store);
        return store;
    }

    @Override
    public void close() throws BackendException {

    }

    @Override
    public void clear() throws BackendException {

        stores.forEach((String k, BackendStore store) -> {
            store.clear();
        });
    }

    @Override
    public String name() {
        return "memory";
    }
}
