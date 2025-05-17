package org.apache.hugegraph.pd;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfo;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfos;
import org.apache.hugegraph.pd.grpc.discovery.Query;
import org.apache.hugegraph.pd.meta.DiscoveryMetaStore;
import org.apache.hugegraph.pd.meta.MetadataFactory;

/**
 * @author zhangyingjie
 * @date 2022/1/14
 **/
public class RegistryService {
    private PDConfig pdConfig;
    private DiscoveryMetaStore meta;

    public RegistryService(PDConfig config){
        this.pdConfig = config;
        meta = MetadataFactory.newDiscoveryMeta(config);
    }

    public void register(NodeInfo nodeInfo, int outTimes) throws PDException {
        meta.register(nodeInfo, outTimes);
    }
    public NodeInfos getNodes(Query query) {
       return meta.getNodes(query);
    }
}
