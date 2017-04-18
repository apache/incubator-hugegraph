package com.baidu.hugegraph.backend.store;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;

import java.util.Collection;
import java.util.Set;

/**
 * Created by jishilei on 17/3/19.
 */
public interface BackendStore {

    public Iterable<BackendEntry> query(Query query);

    public BackendEntry get(Id id);

    public void delete(Id id);

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
