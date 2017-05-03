package com.baidu.hugegraph.backend.store;

import com.baidu.hugegraph.backend.store.cassandra.CassandraStoreProvider;
import com.baidu.hugegraph.backend.store.memory.InMemoryDBStoreProvider;

public class BackendProviderFactory {

    public static BackendStoreProvider open(String backend, String name) {
        if (backend.equalsIgnoreCase("memory")) {
            return new InMemoryDBStoreProvider(name);
        }
        else if (backend.equalsIgnoreCase("cassandra")) {
            return new CassandraStoreProvider(name);
        }

        return null;
    }
}
