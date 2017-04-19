package com.baidu.hugegraph.backend.store;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.configuration.HugeConfiguration;

public interface BackendStore {

    // database name
    public String name();

    // open/close database
    public void open(HugeConfiguration config);
    public void close();

    // initialize/clear database
    public void init();
    public void clear();

    // add/delete data
    public void mutate(BackendMutation mutation);

    // query data
    public Iterable<BackendEntry> query(Query query);

    // TODO: remove the method, use query instead
    public BackendEntry get(Id id);

    // TODO: remove the method, use mutate instead
    public void delete(Id id);

    // transaction
    public void beginTx();
    public void commitTx();
    public void rollbackTx();

}
