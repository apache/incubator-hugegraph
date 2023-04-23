package com.baidu.hugegraph.store.meta.base;

import com.baidu.hugegraph.rocksdb.access.RocksDBSession;
import com.baidu.hugegraph.store.util.HgStoreException;

public interface DBSessionBuilder {
    RocksDBSession getSession(int partId) throws HgStoreException;
}
