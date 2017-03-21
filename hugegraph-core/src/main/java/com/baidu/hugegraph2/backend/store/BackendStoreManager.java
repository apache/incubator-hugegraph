package com.baidu.hugegraph2.backend.store;

import com.baidu.hugegraph2.backend.store.memory.InMemoryDBStoreProvider;

public class BackendStoreManager {

    public static BackendStoreProvider provider(String backend) {
        if (backend.equalsIgnoreCase("memory")) {
            return new InMemoryDBStoreProvider();
        }
        else if (backend.equalsIgnoreCase("cassandra")) {
            // TODO: return new CassandraStoreProvider();
            return null;
        }

        return null;
    }
}
