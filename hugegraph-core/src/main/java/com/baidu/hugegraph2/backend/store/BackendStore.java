package com.baidu.hugegraph2.backend.store;

import com.baidu.hugegraph2.backend.BackendException;
import com.baidu.hugegraph2.backend.id.Id;
import com.baidu.hugegraph2.backend.query.SliceQuery;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Created by jishilei on 17/3/19.
 */
public interface BackendStore {

    public List<BackendEntry> getSlice(SliceQuery query);
    public void mutate(Collection<BackendEntry> additions, Set<Id> deletions);

    public void beginTx();
    public void commitTx();
    public void rollbackTx();

    public String name();

    /**
     * Closes this store
     *
     * @throws BackendException
     */
    public void close() throws BackendException;

    public void clear();

}
