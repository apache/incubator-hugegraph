package com.baidu.hugegraph.store;

import com.baidu.hugegraph.rocksdb.access.RocksDBFactory;
import com.baidu.hugegraph.rocksdb.access.RocksDBSession;
import com.baidu.hugegraph.store.business.BusinessHandler;
import com.baidu.hugegraph.store.business.BusinessHandlerImpl;
import com.baidu.hugegraph.store.meta.PartitionManager;
import com.baidu.hugegraph.store.options.HgStoreEngineOptions;
import com.baidu.hugegraph.store.options.RaftRocksdbOptions;
import com.baidu.hugegraph.store.pd.FakePdServiceProvider;
import com.baidu.hugegraph.store.pd.PdProvider;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class UnitTestBase {

    public static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            for (File file : dir.listFiles()) {
                deleteDir(file);
            }
        }
        return dir.delete();
    }

    private String dbPath;

    private BusinessHandler handler;
    public final static RocksDBFactory factory = RocksDBFactory.getInstance();

    public void initDB(String dbPath) {
        this.dbPath = dbPath;
        UnitTestBase.deleteDir(new File(dbPath));
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("rocksdb.write_buffer_size", "1048576");
        configMap.put("rocksdb.bloom_filter_bits_per_key", "10");

        RaftRocksdbOptions.initRocksdbGlobalConfig(configMap);
        BusinessHandlerImpl.initRocksdb(configMap, null);


    }

    protected BusinessHandler getBusinessHandler() {
        if (handler == null) {
            synchronized (this) {
                if (handler == null) {
                    int partitionCount = 2;
                    HgStoreEngineOptions options = new HgStoreEngineOptions() {{
                        setDataPath(dbPath);
                        setFakePdOptions(new HgStoreEngineOptions.FakePdOptions() {{
                            setPartitionCount(partitionCount);
                            setPeersList("127.0.0.1");
                            setStoreList("127.0.0.1");

                        }});
                    }};

                    PdProvider pdProvider = new FakePdServiceProvider(options.getFakePdOptions());
                    PartitionManager partitionManager = new PartitionManager(pdProvider, options);
                    BusinessHandler handler = new BusinessHandlerImpl(partitionManager);
                }
            }
        }

        return handler;
    }

    public RocksDBSession getDBSession(String dbName) {
        RocksDBSession session = factory.queryGraphDB(dbName);
        if (session == null) {
            session = factory.createGraphDB(dbPath, dbName);
        }
        return session;
    }

    public void close() {
        handler.closeAll();
    }
}
