package org.apache.hugegraph.pd.meta;

import com.baidu.hugegraph.pd.common.PDException;

import org.apache.hugegraph.pd.config.PDConfig;

import com.baidu.hugegraph.pd.grpc.Metapb;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 * Store信息存储
 */
@Slf4j
public class StoreInfoMeta extends MetadataRocksDBStore {
    private PDConfig pdConfig;

    public StoreInfoMeta(PDConfig pdConfig) {
        super(pdConfig);
        this.pdConfig = pdConfig;
     //   this.timeout = pdConfig.getDiscovery().getHeartbeatOutTimes();
    }

    /**
     * 更新Store信息
     * @param store
     * @throws PDException
     */
    public void updateStore(Metapb.Store store) throws PDException {
        byte[] storeInfoKey = MetadataKeyHelper.getStoreInfoKey(store.getId());
        put(storeInfoKey, store.toByteArray());
    }

    /**
     * 更新Store的存活状态
     *
     * @param store
     */
    public void keepStoreAlive(Metapb.Store store) throws PDException {
        byte[] activeStoreKey = MetadataKeyHelper.getActiveStoreKey(store.getId());
        putWithTTL(activeStoreKey, store.toByteArray(), pdConfig.getStore().getKeepAliveTimeout());
    }


    public void removeActiveStore(Metapb.Store store) throws PDException {
        byte[] activeStoreKey = MetadataKeyHelper.getActiveStoreKey(store.getId());
        removeWithTTL(activeStoreKey);
    }

    public Metapb.Store getStore(Long storeId) throws PDException {
        byte[] storeInfoKey = MetadataKeyHelper.getStoreInfoKey(storeId);
        Metapb.Store store = getOne(Metapb.Store.parser(),storeInfoKey);
        return store;
    }

    /**
     * 获取所有的store
     * @param graphName
     * @return
     * @throws PDException
     */
    public List<Metapb.Store> getStores(String graphName) throws PDException {
        byte[] storePrefix = MetadataKeyHelper.getStorePrefix();
        return scanPrefix(Metapb.Store.parser(),storePrefix);
    }

    /**
     * 获取活跃的Store
     *
     * @param graphName
     * @return
     * @throws PDException
     */
    public List<Metapb.Store> getActiveStores(String graphName) throws PDException {
        byte[] activePrefix = MetadataKeyHelper.getActiveStorePrefix();
        List listWithTTL = getInstanceListWithTTL(Metapb.Store.parser(),
                activePrefix);
        return listWithTTL;
    }
    public List<Metapb.Store> getActiveStores() throws PDException {
        byte[] activePrefix = MetadataKeyHelper.getActiveStorePrefix();
        List listWithTTL = getInstanceListWithTTL(Metapb.Store.parser(),
                                                  activePrefix);
        return listWithTTL;
    }

    /**
     * 检查storeid是否存在
     *
     * @param storeId
     * @return
     */
    public boolean storeExists(Long storeId) throws PDException {
        byte[] storeInfoKey = MetadataKeyHelper.getStoreInfoKey(storeId);
        return containsKey(storeInfoKey);
    }

    /**
     * 更新存储状态信息
     *
     * @param storeStats
     */
    public Metapb.StoreStats updateStoreStats(Metapb.StoreStats storeStats) throws PDException {
        byte[] storeStatusKey = MetadataKeyHelper.getStoreStatusKey(storeStats.getStoreId());

        put(storeStatusKey, storeStats.toByteArray());
        return storeStats;
    }

    public long removeStore(long storeId) throws PDException {
        byte[] storeInfoKey = MetadataKeyHelper.getStoreInfoKey(storeId);
        return remove(storeInfoKey);
    }

    public long removeAll() throws PDException {
        byte[] storePrefix = MetadataKeyHelper.getStorePrefix();
        return this.removeByPrefix(storePrefix);
    }

    public void updateShardGroup(Metapb.ShardGroup group) throws PDException {
        byte[] shardGroupKey = MetadataKeyHelper.getShardGroupKey(group.getId());
        put(shardGroupKey, group.toByteArray());
    }

    public void deleteShardGroup(int groupId) throws PDException {
        byte[] shardGroupKey = MetadataKeyHelper.getShardGroupKey(groupId);
        remove(shardGroupKey);
    }

    public static boolean shardGroupEquals(List<Metapb.Shard> g1, List<Metapb.Shard> g2) {
        ListIterator<Metapb.Shard> e1 = g1.listIterator();
        ListIterator<Metapb.Shard> e2 = g2.listIterator();
        while (e1.hasNext() && e2.hasNext()) {
            Metapb.Shard o1 = e1.next();
            Metapb.Shard o2 = e2.next();
            if (!(o1 == null ? o2 == null : o1.getStoreId() == o2.getStoreId())) {
                return false;
            }
        }
        return !(e1.hasNext() || e2.hasNext());
    }

    public Metapb.ShardGroup getShardGroup(int groupId) throws PDException {
        byte[] shardGroupKey = MetadataKeyHelper.getShardGroupKey(groupId);
        return getOne(Metapb.ShardGroup.parser(), shardGroupKey);
    }

    public int getShardGroupCount() throws PDException {
        byte[] shardGroupPrefix = MetadataKeyHelper.getShardGroupPrefix();
        return scanPrefix(Metapb.ShardGroup.parser(), shardGroupPrefix).size();
    }

    public List<Metapb.ShardGroup> getShardGroups() throws PDException {
        byte[] shardGroupPrefix = MetadataKeyHelper.getShardGroupPrefix();
        return scanPrefix(Metapb.ShardGroup.parser(), shardGroupPrefix);
    }

    public Metapb.StoreStats getStoreStats(long storeId) throws PDException {
        byte[] storeStatusKey = MetadataKeyHelper.getStoreStatusKey(storeId);
        Metapb.StoreStats stats = getOne(Metapb.StoreStats.parser(),
                                       storeStatusKey);
        return stats;
    }
    /**
     * @return store及状态信息
     * @throws PDException
     */
    public List<Metapb.Store> getStoreStatus(boolean isActive) throws PDException {
        byte[] storePrefix = MetadataKeyHelper.getStorePrefix();
        List<Metapb.Store> stores =isActive ? getActiveStores() :
                                   scanPrefix(Metapb.Store.parser(),storePrefix);
        LinkedList<Metapb.Store> list = new LinkedList<>();
        for (int i = 0; i < stores.size(); i++) {
            Metapb.Store store = stores.get(i);
            Metapb.StoreStats stats = getStoreStats(store.getId());
            if (stats != null)
                store = Metapb.Store.newBuilder(store).setStats(getStoreStats(store.getId()))
                        .build();
            list.add(store);
        }
        return list;
    }
}
