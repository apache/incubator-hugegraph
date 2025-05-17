package org.apache.hugegraph.pd;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.meta.ConfigMetaStore;
import org.apache.hugegraph.pd.meta.MetadataFactory;
import org.apache.hugegraph.pd.raft.RaftStateListener;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class ConfigService implements RaftStateListener {

    private PDConfig pdConfig;
    private ConfigMetaStore meta;

    public ConfigService(PDConfig config) {
        this.pdConfig = config;
        config.setConfigService(this);
        meta = MetadataFactory.newConfigMeta(config);
    }

    public Metapb.PDConfig getPDConfig(long version) throws PDException {
        return this.meta.getPdConfig(version);
    }

    public Metapb.PDConfig getPDConfig() throws PDException {
        return this.meta.getPdConfig(0);
    }

    public Metapb.PDConfig setPDConfig(Metapb.PDConfig mConfig) throws PDException {
        Metapb.PDConfig oldCfg = getPDConfig();
        Metapb.PDConfig.Builder builder = oldCfg.toBuilder().mergeFrom(mConfig)
                .setVersion(oldCfg.getVersion() + 1)
                .setTimestamp(System.currentTimeMillis());
        mConfig =  this.meta.setPdConfig(builder.build());
        log.info("PDConfig has been modified, new PDConfig is {}", mConfig);
        updatePDConfig(mConfig);
        return mConfig;
    }

    public List<Metapb.GraphSpace> getGraphSpace(String graphSpaceName) throws PDException {
        return this.meta.getGraphSpace(graphSpaceName);
    }

    public Metapb.GraphSpace setGraphSpace(Metapb.GraphSpace graphSpace) throws PDException {
        return this.meta.setGraphSpace(graphSpace.toBuilder()
                .setTimestamp(System.currentTimeMillis())
                .build());
    }

    /**
     * 从存储中读取配置项，并覆盖全局的PDConfig对象
     * @return
     */
    public PDConfig loadConfig() {
        try {
            Metapb.PDConfig mConfig = this.meta.getPdConfig(0);
            if ( mConfig == null ){
                // todo : 初始化配置, store group
                mConfig = Metapb.PDConfig.newBuilder()
                        .setShardCount(pdConfig.getPartition().getShardCount())
                        .setVersion(1)
                        .setTimestamp(System.currentTimeMillis())
                        .setMaxShardsPerStore(pdConfig.getPartition().getMaxShardsPerStore())
                        .build();
                this.meta.setPdConfig(mConfig);
            }

            pdConfig = updatePDConfig(mConfig);
            // 考虑版本升级
            loadStoreGroup();
        } catch (Exception e) {
            log.error("ConfigService loadConfig exception:", e);
        }
        return pdConfig;
    }

    private void loadStoreGroup() throws PDException {
        var groups = getAllStoreGroup();
        if (groups.isEmpty()) {
            String storeList = pdConfig.getInitialStoreList();
            Map<Integer, Set<String>> groupMap = new HashMap<>();

            // group id -> { store address }
            for (String store : storeList.split(",")) {
                String[] arr = store.split("/");
                int groupId = -1;
                String storeAddress = "";
                if (arr.length == 1) {
                    groupId = 0;
                    storeAddress = arr[0];
                } else if (arr.length == 2){
                    groupId = Integer.parseInt(arr[1]);
                    storeAddress = arr[0];
                } else {
                    throw new PDException(-1, "Invalid store list: " + storeList);
                }

                if (! groupMap.containsKey(groupId)) {
                    groupMap.put(groupId, new HashSet<>());
                }
                groupMap.get(groupId).add(storeAddress);
            }

            var pdConfig = getPDConfig();
            for (var entry : groupMap.entrySet()) {
                int count = entry.getValue().size() * pdConfig.getMaxShardsPerStore() / pdConfig.getShardCount();
                var group = Metapb.StoreGroup.newBuilder()
                        .setGroupId(entry.getKey())
                        .setName("")
                        .setPartitionCount(count)
                        .build();
                meta.saveStoreGroup(group);
            }
        }
    }

    public synchronized PDConfig updatePDConfig(Metapb.PDConfig mConfig){
        log.info("update pd config: mConfig:{}", mConfig);
        pdConfig.getPartition().setShardCount(mConfig.getShardCount());
        pdConfig.getPartition().setMaxShardsPerStore(mConfig.getMaxShardsPerStore());
        return pdConfig;
    }

    public synchronized void setPartitionCount(int storeGroupId, int count) throws PDException {
        var storeGroup = meta.getStoreGroup(storeGroupId);
        if (storeGroup != null) {
            log.info("update the partition count of store group {} to {}", storeGroupId, count);
            meta.saveStoreGroup(storeGroup.toBuilder().setPartitionCount(count).build());
        }
    }

    public synchronized Metapb.StoreGroup createStoreGroup(int storeGroupId, String name, int partitionCount)
            throws PDException {
        return meta.saveStoreGroup(Metapb.StoreGroup.newBuilder()
                    .setGroupId(storeGroupId)
                    .setName(name)
                    .setPartitionCount(partitionCount)
                    .build());
    }

    public synchronized Metapb.StoreGroup updateStoreGroup(int storeGroupId, String name) throws PDException {
        var storeGroup = meta.getStoreGroup(storeGroupId);
        if (storeGroup != null) {
            return meta.saveStoreGroup(storeGroup.toBuilder().setName(name).build());
        }
        return null;
    }

    /**
     * meta store中的数量
     * 由于可能会受分区分裂/合并的影响，原始的partition count不推荐使用
     *
     * @return partition count of cluster
     * @throws PDException when io error
     */
    public int getPartitionCount(int storeGroupId) throws PDException {
        var group = getStoreGroup(storeGroupId);
        return group == null ? 0 : group.getPartitionCount();
    }

    public List<Metapb.StoreGroup> getAllStoreGroup() throws PDException {
        return meta.getStoreGroups();
    }

    public Metapb.StoreGroup getStoreGroup(int groupId) throws PDException {
        return meta.getStoreGroup(groupId);
    }

    @Override
    public void onRaftLeaderChanged() {
        loadConfig();
    }
}
