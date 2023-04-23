package com.baidu.hugegraph.store;

import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.store.meta.Partition;
import com.baidu.hugegraph.store.meta.PartitionRole;

import java.util.List;

public interface PartitionStateListener {
    // 分区角色发生改变
    void partitionRoleChanged(Partition partition, PartitionRole newRole);
    // 分区发生改变
    void partitionShardChanged(Partition partition, List<Metapb.Shard> oldShards, List<Metapb.Shard> newShards);
}
