package com.baidu.hugegraph.store.meta.base;

import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import com.baidu.hugegraph.store.util.HgStoreException;

public interface DBSessionBuilder {
    RocksDBSession getSession(int partId) throws HgStoreException;
}
