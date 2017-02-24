package com.baidu.hugegraph2.backend.store;

import java.util.List;

import com.baidu.hugegraph2.backend.BackendException;

/**
 * Created by jishilei on 17/3/20.
 */
public class SchemaStore implements DBStore {

    @Override
    public void mutate(List<DBEntry> additions, List<Object> deletions, StoreTransaction tx) {

    }

    @Override
    public String getName() {
        return "schema";
    }

    @Override
    public void close() throws BackendException {

    }

    @Override
    public void clear() {

    }
}
