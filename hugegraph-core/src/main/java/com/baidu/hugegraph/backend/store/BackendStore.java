package com.baidu.hugegraph.backend.store;

import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.type.HugeType;

public interface BackendStore {

    // Database name
    public String name();

    // Open/close database
    public void open(HugeConfig config);
    public void close();

    // Initialize/clear database
    public void init();
    public void clear();

    // Add/delete data
    public void mutate(BackendMutation mutation);

    // Query data
    public Iterable<BackendEntry> query(Query query);

    // Transaction
    public void beginTx();
    public void commitTx();
    public void rollbackTx();

    // Get metadata by key
    public Object metadata(HugeType type, String meta, Object[] args);
}
