package com.baidu.hugegraph.store.meta.base;

import com.baidu.hugegraph.rocksdb.access.RocksDBFactory;
import com.baidu.hugegraph.rocksdb.access.RocksDBSession;
import com.baidu.hugegraph.store.options.MetadataOptions;

import java.util.Arrays;

public class GlobalMetaStore extends MetaStoreBase {
    public static final String HSTORE_METADATA_GRAPH_NAME = "hgstore-metadata";
    public static final String HSTORE_CF_NAME = "default";

    private final MetadataOptions options;

    private String dataPath;

    public GlobalMetaStore(MetadataOptions options) {
        this.options = options;
        dataPath = Arrays.asList(options.getDataPath().split(",")).get(0);
    }

    public MetadataOptions getOptions(){
        return options;
    }

    protected RocksDBSession getRocksDBSession() {
        RocksDBFactory rocksDBFactory = RocksDBFactory.getInstance();
        RocksDBSession dbSession = rocksDBFactory.queryGraphDB(HSTORE_METADATA_GRAPH_NAME);
        if (dbSession == null) {
            dbSession = rocksDBFactory.createGraphDB(dataPath, HSTORE_METADATA_GRAPH_NAME);
        }
        return dbSession;
    }

    @Override
    protected String getCFName() {
        return HSTORE_CF_NAME;
    }
}
