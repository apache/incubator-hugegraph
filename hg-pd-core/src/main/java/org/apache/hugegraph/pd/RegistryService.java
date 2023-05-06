package org.apache.hugegraph.pd;

import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.meta.DiscoveryMetaStore;
import org.apache.hugegraph.pd.meta.MetadataFactory;

import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.pd.grpc.discovery.NodeInfo;
import com.baidu.hugegraph.pd.grpc.discovery.NodeInfos;
import com.baidu.hugegraph.pd.grpc.discovery.Query;

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
