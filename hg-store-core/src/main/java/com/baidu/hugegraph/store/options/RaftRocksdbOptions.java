package com.baidu.hugegraph.store.options;

import com.alipay.sofa.jraft.storage.impl.RocksDBLogStorage;
import com.alipay.sofa.jraft.util.StorageOptionsFactory;
import com.baidu.hugegraph.rocksdb.access.RocksDBOptions;
import com.baidu.hugegraph.store.business.BusinessHandlerImpl;

import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;
import org.apache.hugegraph.config.HugeConfig;

import java.util.Map;

@Slf4j
public class RaftRocksdbOptions {
    static class RocksdbConfig{
        private Env env;
        private LRUCache blockCache;
        private LRUCache writeCache;
        private WriteBufferManager bufferManager;
        private BlockBasedTableConfig tableConfig;
        private long blockCacheCapacity;
        private long writeCacheCapacity;

        public Env getEnv(){ return env; }
        public LRUCache getBlockCache() { return blockCache; }
        public LRUCache getWriteCache() { return writeCache; }
        public WriteBufferManager getBufferManager() { return bufferManager;}
        public BlockBasedTableConfig getTableConfig(){ return tableConfig; }
        public long getBlockCacheCapacity(){ return blockCacheCapacity; }
        public long getWriteCacheCapacity(){ return writeCacheCapacity; }

        public RocksdbConfig(HugeConfig options) {
            RocksDB.loadLibrary();
            this.env = Env.getDefault();
            double writeBufferRatio = options.get(RocksDBOptions.WRITE_BUFFER_RATIO);
            this.writeCacheCapacity = (long) (options.get(RocksDBOptions.TOTAL_MEMORY_SIZE) * writeBufferRatio);
            this.blockCacheCapacity = options.get(RocksDBOptions.TOTAL_MEMORY_SIZE) - writeCacheCapacity;
            this.writeCache = new LRUCache(writeCacheCapacity);
            this.blockCache = new LRUCache(blockCacheCapacity);
            this.bufferManager = new WriteBufferManager(writeCacheCapacity, writeCache,
                    options.get(RocksDBOptions.WRITE_BUFFER_ALLOW_STALL));
            this.tableConfig = new BlockBasedTableConfig() //
                    .setIndexType(IndexType.kTwoLevelIndexSearch) //
                    .setPartitionFilters(true) //
                    .setMetadataBlockSize(8 * SizeUnit.KB) //
                    .setCacheIndexAndFilterBlocks(options.get(RocksDBOptions.PUT_FILTER_AND_INDEX_IN_CACHE)) //
                    .setCacheIndexAndFilterBlocksWithHighPriority(true) //
                    .setPinL0FilterAndIndexBlocksInCache(options.get(RocksDBOptions.PIN_L0_FILTER_AND_INDEX_IN_CACHE)) //
                    .setBlockSize(4 * SizeUnit.KB)//
                    .setBlockCache(blockCache);

            int bitsPerKey = options.get(RocksDBOptions.BLOOM_FILTER_BITS_PER_KEY);
            if (bitsPerKey >= 0) {
                tableConfig.setFilterPolicy(new BloomFilter(bitsPerKey,
                        options.get(RocksDBOptions.BLOOM_FILTER_MODE)));
            }
            tableConfig.setWholeKeyFiltering(
                    options.get(RocksDBOptions.BLOOM_FILTER_WHOLE_KEY));
            log.info("RocksdbConfig {}", options.get(RocksDBOptions.BLOOM_FILTER_BITS_PER_KEY));
        }
    }

    private static RocksdbConfig rocksdbConfig = null;
    private static RocksdbConfig getRocksdbConfig(HugeConfig options){
        if ( rocksdbConfig == null){
            synchronized (RocksdbConfig.class){
                rocksdbConfig = new RocksdbConfig(options);
            }
        }
        return rocksdbConfig;
    }

    private static void registerRaftRocksdbConfig(HugeConfig options) {
        Cache blockCache = new LRUCache(1 * SizeUnit.GB);
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig() //
                .setIndexType(IndexType.kTwoLevelIndexSearch) //
                .setPartitionFilters(true) //
                .setMetadataBlockSize(8 * SizeUnit.KB) //
                .setCacheIndexAndFilterBlocks(options.get(RocksDBOptions.PUT_FILTER_AND_INDEX_IN_CACHE)) //
                .setCacheIndexAndFilterBlocksWithHighPriority(true) //
                .setPinL0FilterAndIndexBlocksInCache(options.get(RocksDBOptions.PIN_L0_FILTER_AND_INDEX_IN_CACHE)) //
                .setBlockSize(4 * SizeUnit.KB)//
                .setBlockCache(blockCache);

        StorageOptionsFactory.registerRocksDBTableFormatConfig(RocksDBLogStorage.class,
                tableConfig);

        DBOptions dbOptions = StorageOptionsFactory.getDefaultRocksDBOptions();
        dbOptions.setEnv(rocksdbConfig.getEnv());

        // raft rocksdb数量固定，通过max_write_buffer_number可以控制
        //dbOptions.setWriteBufferManager(rocksdbConfig.getBufferManager());
        dbOptions.setUnorderedWrite(true);
        StorageOptionsFactory.registerRocksDBOptions(RocksDBLogStorage.class,
                dbOptions);

        ColumnFamilyOptions cfOptions = StorageOptionsFactory.getDefaultRocksDBColumnFamilyOptions();
        cfOptions.setTargetFileSizeBase(256 * SizeUnit.MB);
        cfOptions.setWriteBufferSize(8 * SizeUnit.MB);
        cfOptions.setNumLevels(3);
        cfOptions.setMaxWriteBufferNumber(3);
        cfOptions.setCompressionType(CompressionType.NO_COMPRESSION);
        cfOptions.setMaxBytesForLevelBase(2048 * SizeUnit.GB);

        StorageOptionsFactory.registerRocksDBColumnFamilyOptions(RocksDBLogStorage.class,
                cfOptions);
    }


    public static void initRocksdbGlobalConfig(Map<String, Object> config){
        HugeConfig hugeConfig = BusinessHandlerImpl.initRocksdb(config, null);
        RocksdbConfig rocksdbConfig = getRocksdbConfig(hugeConfig);
        registerRaftRocksdbConfig(hugeConfig);
        config.put(RocksDBOptions.ENV, rocksdbConfig.getEnv());
        config.put(RocksDBOptions.WRITE_BUFFER_MANAGER, rocksdbConfig.getBufferManager());
        config.put(RocksDBOptions.BLOCK_TABLE_CONFIG, rocksdbConfig.getTableConfig());
        config.put(RocksDBOptions.BLOCK_CACHE, rocksdbConfig.getBlockCache());
        config.put(RocksDBOptions.WRITE_CACHE, rocksdbConfig.getWriteCache());
    }

    public static WriteBufferManager getWriteBufferManager(){
        return rocksdbConfig.getBufferManager();
    }

    public static Env getEnv(){
        return rocksdbConfig.getEnv();
    }

    public static Cache getWriteCache() { return rocksdbConfig.getWriteCache();}
    public static Cache getBlockCache() { return rocksdbConfig.getBlockCache();}
    public static long getWriteCacheCapacity() { return rocksdbConfig.getWriteCacheCapacity();}
    public static long getBlockCacheCapacity() { return rocksdbConfig.getBlockCacheCapacity();}
}
