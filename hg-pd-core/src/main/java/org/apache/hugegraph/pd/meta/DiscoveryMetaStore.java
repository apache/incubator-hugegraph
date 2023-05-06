package org.apache.hugegraph.pd.meta;

import com.baidu.hugegraph.pd.common.PDException;

import org.apache.hugegraph.pd.config.PDConfig;

import com.baidu.hugegraph.pd.grpc.discovery.NodeInfo;
import com.baidu.hugegraph.pd.grpc.discovery.NodeInfos;
import com.baidu.hugegraph.pd.grpc.discovery.Query;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author zhangyingjie
 * @date 2022/1/14
 **/
@Slf4j
public class DiscoveryMetaStore extends MetadataRocksDBStore {

    public DiscoveryMetaStore(PDConfig pdConfig) {
        super(pdConfig);
    }

    //appName --> address --> registryInfo
    private static final String PREFIX ="REGIS-";
    private static final String SPLITTER ="-";
    public void register(NodeInfo nodeInfo, int outTimes) throws PDException {
        putWithTTL(toKey(nodeInfo.getAppName(), nodeInfo.getVersion(), nodeInfo.getAddress()),
                   nodeInfo.toByteArray(),(nodeInfo.getInterval() / 1000) * outTimes);
    }

    byte[] toKey(String appName,String version,String address){
        StringBuilder builder = getPrefixBuilder(appName, version);
        builder.append(SPLITTER);
        builder.append(address);
        return builder.toString().getBytes();
    }

    private StringBuilder getPrefixBuilder(String appName, String version) {
        StringBuilder builder = new StringBuilder();
        builder.append(PREFIX);
        if (!StringUtils.isEmpty(appName)){
            builder.append(appName);
            builder.append(SPLITTER);
        }
        if (!StringUtils.isEmpty(version)){
            builder.append(version);
        }
        return builder;
    }

    public NodeInfos getNodes(Query query) {
        List<NodeInfo> nodeInfos = null;
        try {
            StringBuilder builder = getPrefixBuilder(query.getAppName(),
                                                     query.getVersion());
            nodeInfos = getInstanceListWithTTL(
                    NodeInfo.parser(),
                    builder.toString().getBytes());
            builder.setLength(0);
        } catch (PDException e) {
            log.error("An error occurred getting data from the store,{}",e);
        }
        if (query.getLabelsMap() != null && !query.getLabelsMap().isEmpty()) {
            List result =new LinkedList<NodeInfo>();
            for (NodeInfo node:nodeInfos) {
                if (labelMatch(node,query)) result.add(node);
            }
            return NodeInfos.newBuilder().addAllInfo(result).build();
        }
        return NodeInfos.newBuilder().addAllInfo(nodeInfos).build();

    }
    private boolean labelMatch(NodeInfo node,Query query){
        Map<String, String> labelsMap = node.getLabelsMap();
        for (Map.Entry<String,String> entry:query.getLabelsMap().entrySet()) {
            if (!entry.getValue().equals(labelsMap.get(entry.getKey()))){
                return false;
            }
        }
        return true;
    }
}
