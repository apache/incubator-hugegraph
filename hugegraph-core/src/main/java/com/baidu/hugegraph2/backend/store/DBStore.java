package com.baidu.hugegraph2.backend.store;

import com.baidu.hugegraph2.backend.BackendException;

import java.util.List;

/**
 * Created by jishilei on 17/3/19.
 */
public interface DBStore {

    public void mutate(List<DBEntry> additions, List<Object> deletions, StoreTransaction tx);

    public String getName();

    /**
     * Closes this store
     *
     * @throws BackendException
     */
    public void close() throws BackendException;

    public void clear();

}
