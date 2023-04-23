package com.baidu.hugegraph.store.meta.base;

import com.baidu.hugegraph.rocksdb.access.RocksDBSession;

/**
 * 元数据存储在分区的default cf中
 */
public class PartitionMetaStore extends MetaStoreBase {
    public static final String DEFAULT_CF_NAME = "default";

    private final DBSessionBuilder sessionBuilder;
    private final Integer partitionId;

    public PartitionMetaStore(DBSessionBuilder sessionBuilder, int partId) {
        this.sessionBuilder = sessionBuilder;
        this.partitionId = partId;
    }

    @Override
    protected RocksDBSession getRocksDBSession() {
        return sessionBuilder.getSession(this.partitionId);
    }

    @Override
    protected String getCFName() {
        return DEFAULT_CF_NAME;
    }

    protected void flush() {
        try (RocksDBSession dbSession = getRocksDBSession()) {
            dbSession.flush(true);
        }
    }
}
