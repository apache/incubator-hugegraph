package org.apache.hugegraph.pd.model;

import org.apache.hugegraph.pd.grpc.Metapb;

import lombok.Data;

/**
 * @author zhangyingjie
 * @date 2023/7/25
 **/
@Data
class Shard {
    long partitionId;
    long storeId;
    String state;
    String role;
    int progress;

    public Shard(Metapb.ShardStats shardStats, long partitionId) {
        this.role = String.valueOf(shardStats.getRole());
        this.storeId = shardStats.getStoreId();
        this.state = String.valueOf(shardStats.getState());
        this.partitionId = partitionId;
        this.progress = shardStats.getProgress();
    }

    public Shard(Metapb.Shard shard, long partitionId) {
        this.role = String.valueOf(shard.getRole());
        this.storeId = shard.getStoreId();
        this.state = Metapb.ShardState.SState_Normal.name(); // gshard的状态默认为normal
        this.progress = 0;
        this.partitionId = partitionId;
    }
}
