package com.baidu.hugegraph.pd;

import com.baidu.hugegraph.pd.grpc.Metapb;

public interface ShardGroupStatusListener {
    void onShardListChanged(Metapb.ShardGroup shardGroup, Metapb.ShardGroup newShardGroup);

    void onShardListOp(Metapb.ShardGroup shardGroup);
}
