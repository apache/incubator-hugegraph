package org.apache.hugegraph.pd.meta;

import com.baidu.hugegraph.pd.common.PDException;

import org.apache.hugegraph.pd.config.PDConfig;

import com.baidu.hugegraph.pd.grpc.Metapb;

import java.util.List;
import java.util.Optional;

public class ConfigMetaStore extends MetadataRocksDBStore {


    private final long clusterId;

    public ConfigMetaStore(PDConfig pdConfig) {
        super(pdConfig);
        this.clusterId = pdConfig.getClusterId();
    }
    /**
     * 更新图空间存储状态信息
     *
     * @param
     */
    public Metapb.GraphSpace setGraphSpace(Metapb.GraphSpace graphSpace) throws PDException {
        byte[] graphSpaceKey = MetadataKeyHelper.getGraphSpaceKey(graphSpace.getName());
        graphSpace = graphSpace.toBuilder().setTimestamp(System.currentTimeMillis()).build();
        put(graphSpaceKey, graphSpace.toByteArray());
        return graphSpace;
    }
    public List<Metapb.GraphSpace> getGraphSpace(String graphSpace) throws PDException {
        byte[] graphSpaceKey = MetadataKeyHelper.getGraphSpaceKey(graphSpace);
        return scanPrefix(Metapb.GraphSpace.parser(), graphSpaceKey);
    }

    public Metapb.PDConfig setPdConfig(Metapb.PDConfig pdConfig) throws PDException {
        byte[] graphSpaceKey = MetadataKeyHelper.getPdConfigKey(String.valueOf(pdConfig.getVersion()));
        Metapb.PDConfig config = Metapb.PDConfig.newBuilder(
                pdConfig).setTimestamp(System.currentTimeMillis()).build();
        put(graphSpaceKey, config.toByteArray());
        return config;
    }
    public Metapb.PDConfig getPdConfig(long version) throws PDException {
        byte[] graphSpaceKey = MetadataKeyHelper.getPdConfigKey(version <= 0 ? null :
                                                                  String.valueOf(version));
        Optional<Metapb.PDConfig> max = scanPrefix(
                Metapb.PDConfig.parser(), graphSpaceKey).stream().max(
                (o1, o2) -> (o1.getVersion() > o2.getVersion()) ? 1 : -1);
        return max.isPresent()? max.get() : null;
    }


}
