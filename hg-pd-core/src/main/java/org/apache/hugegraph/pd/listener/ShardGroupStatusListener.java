package org.apache.hugegraph.pd.listener;

import org.apache.hugegraph.pd.grpc.Metapb;

public interface ShardGroupStatusListener {
    void onShardListChanged(Metapb.ShardGroup shardGroup, Metapb.ShardGroup newShardGroup);

    void onShardListOp(Metapb.ShardGroup shardGroup);
}
