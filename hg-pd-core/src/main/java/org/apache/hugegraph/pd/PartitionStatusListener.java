package org.apache.hugegraph.pd;

import com.baidu.hugegraph.pd.grpc.Metapb;

/**
 * 分区状态监听
 */
public interface PartitionStatusListener {
    void onPartitionChanged(Metapb.Partition partition, Metapb.Partition newPartition);
    void onPartitionRemoved(Metapb.Partition partition);
}
