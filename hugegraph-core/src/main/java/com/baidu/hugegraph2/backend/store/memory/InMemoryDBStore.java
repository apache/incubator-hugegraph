package com.baidu.hugegraph2.backend.store.memory;

import com.baidu.hugegraph2.backend.BackendException;
import com.baidu.hugegraph2.backend.store.DBEntry;
import com.baidu.hugegraph2.backend.store.DBStore;
import com.baidu.hugegraph2.backend.store.StoreTransaction;

import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by jishilei on 17/3/19.
 */
public class InMemoryDBStore implements DBStore {

    private final String name;
    private final ConcurrentNavigableMap<Object, DBEntry> store;

    public InMemoryDBStore(final String name){
        this.name = name;
        this.store = new ConcurrentSkipListMap<Object, DBEntry>();
    }

    @Override
    public void mutate(List<DBEntry> additions, List<Object> deletions, StoreTransaction tx) {

        additions.forEach((entry)->{
            store.put(entry.getId(),entry);
        });

        deletions.forEach((k)->{
            store.remove(k);
        });
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void clear() {
        store.clear();
    }

    @Override
    public void close() throws BackendException {

    }
}
