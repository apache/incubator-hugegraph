package com.baidu.hugegraph2.backend.store.memory;

import com.baidu.hugegraph2.backend.BackendException;
import com.baidu.hugegraph2.backend.store.*;
import com.google.common.base.Preconditions;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jishilei on 17/3/19.
 */
public class InMemoryDBStoreProvider implements StoreProvider {

    private final ConcurrentHashMap<String, DBStore> stores;

    public InMemoryDBStoreProvider() {
        stores = new ConcurrentHashMap<String, DBStore>();
    }

    @Override
    public DBStore open(final String name) throws BackendException {
        if (!stores.containsKey(name)) {
            stores.putIfAbsent(name, new InMemoryDBStore(name));
        }
        DBStore store = stores.get(name);
        Preconditions.checkNotNull(store);
        return store;
    }

    @Override
    public void mutate(Map<String, StoreMutation> mutations, StoreTransaction tx) {

        mutations.forEach((k, mutation) -> {
            DBStore store = stores.get(k);
            Preconditions.checkNotNull(store);
            store.mutate(mutation.getAdditions(), mutation.getDeletions(), tx);
        });
    }

    @Override
    public void close() throws BackendException {

    }

    @Override
    public void clear() throws BackendException {

        stores.forEach((String k, DBStore store) -> {
            store.clear();
        });
    }

    @Override
    public String getName() {
        return toString();
    }
}
