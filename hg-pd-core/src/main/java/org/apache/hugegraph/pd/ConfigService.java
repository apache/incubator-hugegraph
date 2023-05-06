package org.apache.hugegraph.pd;

import com.baidu.hugegraph.pd.common.PDException;

import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.meta.ConfigMetaStore;
import org.apache.hugegraph.pd.meta.MetadataFactory;
import org.apache.hugegraph.pd.raft.RaftStateListener;

import com.baidu.hugegraph.pd.grpc.Metapb;

import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class ConfigService implements RaftStateListener {

    private PDConfig pdConfig;
    private ConfigMetaStore meta;

    public ConfigService(PDConfig config){
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
                mConfig = Metapb.PDConfig.newBuilder()
                        .setPartitionCount(pdConfig.getInitialPartitionCount())
                        .setShardCount(pdConfig.getPartition().getShardCount())
                        .setVersion(1)
                        .setTimestamp(System.currentTimeMillis())
                        .setMaxShardsPerStore(pdConfig.getPartition().getMaxShardsPerStore())
                        .build();
                this.meta.setPdConfig(mConfig);
            }
            pdConfig = updatePDConfig(mConfig);
        } catch (Exception e) {
            log.error("ConfigService loadConfig exception {}", e);
        }
        return pdConfig;
    }

    public synchronized PDConfig updatePDConfig(Metapb.PDConfig mConfig){
        log.info("update pd config: mConfig:{}", mConfig);
        pdConfig.getPartition().setShardCount(mConfig.getShardCount());
        pdConfig.getPartition().setTotalCount(mConfig.getPartitionCount());
        pdConfig.getPartition().setMaxShardsPerStore(mConfig.getMaxShardsPerStore());
        return pdConfig;
    }

    public synchronized PDConfig setPartitionCount(int count){
        Metapb.PDConfig mConfig = null;
        try {
            mConfig = getPDConfig();
            mConfig = mConfig.toBuilder().setPartitionCount(count).build();
            setPDConfig(mConfig);
        } catch (PDException e) {
            log.error("ConfigService exception {}", e);
            e.printStackTrace();
        }
        return pdConfig;
    }

    /**
     * meta store中的数量
     * 由于可能会受分区分裂/合并的影响，原始的partition count不推荐使用
     *
     * @return partition count of cluster
     * @throws PDException when io error
     */
    public int getPartitionCount() throws PDException {
        return getPDConfig().getPartitionCount();
    }

    @Override
    public void onRaftLeaderChanged() {
        loadConfig();
    }
}
