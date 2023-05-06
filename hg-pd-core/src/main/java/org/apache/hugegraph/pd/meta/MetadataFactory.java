package org.apache.hugegraph.pd.meta;

import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.pd.store.HgKVStore;
import org.apache.hugegraph.pd.store.HgKVStoreImpl;
import org.apache.hugegraph.pd.store.RaftKVStore;

/**
 * 存储工厂类，创建相关对象的存储类
 */
public class MetadataFactory {

    private static HgKVStore store = null;

    public static HgKVStore getStore(PDConfig pdConfig){
        if ( store == null ){
            synchronized (MetadataFactory.class){
                if ( store == null ) {
                    HgKVStore proto = new HgKVStoreImpl();
                    //proto.init(pdConfig);
                    store = pdConfig.getRaft().isEnable() ?
                            new RaftKVStore(RaftEngine.getInstance(), proto) :
                            proto;
                    store.init(pdConfig);
                }
            }
        }
        return store;
    }

    public static void closeStore(){
        if ( store != null )
            store.close();
    }

    public static StoreInfoMeta newStoreInfoMeta(PDConfig pdConfig) {
        return new StoreInfoMeta(pdConfig);
    }

    public static PartitionMeta newPartitionMeta(PDConfig pdConfig) {
        return new PartitionMeta(pdConfig);
    }
    public static IdMetaStore newHugeServerMeta(PDConfig pdConfig) {
        return new IdMetaStore(pdConfig);
    }
    public static DiscoveryMetaStore newDiscoveryMeta(PDConfig pdConfig) {
        return new DiscoveryMetaStore(pdConfig);
    }
    public static ConfigMetaStore newConfigMeta(PDConfig pdConfig) {
        return new ConfigMetaStore(pdConfig);
    }
    public static TaskInfoMeta newTaskInfoMeta(PDConfig pdConfig) { return new TaskInfoMeta(pdConfig);}


    public static QueueStore newQueueStore(PDConfig pdConfig) {
        return new QueueStore(pdConfig);
    }

    public static LogMeta newLogMeta(PDConfig pdConfig) {
        return new LogMeta(pdConfig);
    }
}
