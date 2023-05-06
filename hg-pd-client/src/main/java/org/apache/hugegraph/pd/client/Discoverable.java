package org.apache.hugegraph.pd.client;

import com.baidu.hugegraph.pd.grpc.discovery.NodeInfos;
import com.baidu.hugegraph.pd.grpc.discovery.Query;

import java.util.Map;

/**
 * @author zhangyingjie
 * @date 2021/12/20
 **/
public interface Discoverable {

    NodeInfos getNodeInfos(Query query);

    void scheduleTask();
    void cancelTask();
}
