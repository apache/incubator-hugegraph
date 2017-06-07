package com.baidu.hugegraph.backend.store;

import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.type.HugeType;

public interface BackendStore {

    // database name
    public String name();

    // open/close database
    public void open(HugeConfig config);
    public void close();

    // initialize/clear database
    public void init();
    public void clear();

    // add/delete data
    public void mutate(BackendMutation mutation);

    // query data
    public Iterable<BackendEntry> query(Query query);

    // transaction
    public void beginTx();
    public void commitTx();
    public void rollbackTx();

    // get metadata by key
    public Object metadata(HugeType type, String meta, Object[] args);
}
