package com.baidu.hugegraph.store.meta;

import com.baidu.hugegraph.pd.grpc.Metapb;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class PartitionStats {
    // 分区ID
    private int id;
    private String namespace;
    private String graphName;
    // 分区leader所在shard
    Metapb.Shard leader;
    // 分区离线的shard
    List<Metapb.Shard> offlineShards = new ArrayList<>();
    long committedIndex;
    long leaderTerm;
    long approximateSize;
    long approximateKeys;

    public PartitionStats addOfflineShard(Metapb.Shard shard){
        offlineShards.add(shard);
        return this;
    }

    public Metapb.PartitionStats getProtoObj(){
        return Metapb.PartitionStats.newBuilder()
                .setId(id)
                .addGraphName(graphName)
                .setLeader(leader)
                .addAllShard(offlineShards)
                .setApproximateKeys(approximateKeys)
                .setApproximateSize(approximateSize)
                .setLeaderTerm(leaderTerm)
                .build();
    }
}
