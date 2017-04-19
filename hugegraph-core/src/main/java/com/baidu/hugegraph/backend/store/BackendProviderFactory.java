package com.baidu.hugegraph.backend.store;

import com.baidu.hugegraph.backend.store.memory.InMemoryDBStoreProvider;

public class BackendProviderFactory {

    public static BackendStoreProvider open(String backend) {
        if (backend.equalsIgnoreCase("memory")) {
            return new InMemoryDBStoreProvider();
        }

        return null;
    }
}
