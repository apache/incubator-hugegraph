package org.apache.hugegraph.pd.client.listener;

import org.apache.hugegraph.pd.grpc.watch.WatchResponse;
import org.apache.hugegraph.pd.watch.NodeEvent;
import org.apache.hugegraph.pd.watch.PartitionEvent;

/**
 * @author zhangyingjie
 * @date 2023/9/14
 **/
public interface PDEventListener {
    void onStoreChanged(NodeEvent event);

    void onPartitionChanged(PartitionEvent event);

    void onGraphChanged(WatchResponse event);

    default void onShardGroupChanged(WatchResponse event) {
    }
}
