package com.baidu.hugegraph.store.meta;

import com.baidu.hugegraph.pd.grpc.Metapb;

/**
 * @projectName: hugegraph-store
 * @package: com.baidu.hugegraph.store.partitions
 * @className: PartitionRole
 * @author: tyzer
 * @description: TODO
 * @date: 2021/10/19 18:18
 * @version: 1.0
 */
public enum PartitionRole {
    UNKNOWN(0, "unknown"), LEADER(1, "leader"), FOLLOWER(2, "follower"),
    LEARNER(3, "learner"), CANDIDATE(4, "candidate");
    private int role;
    private String name;

    PartitionRole(int role, String name) {
        this.role = role;
        this.name = name;
    }

    @Override
    public String toString() {
        return this.ordinal() + "_" + this.name;
    }

    public String getName(){ return this.name; }

    public Metapb.ShardRole toShardRole(){
        Metapb.ShardRole shardRole = Metapb.ShardRole.None;
        switch (this){
            case LEADER:
                shardRole = Metapb.ShardRole.Leader;
                break;
            case FOLLOWER:
                shardRole = Metapb.ShardRole.Follower;
                break;
            case LEARNER:
                shardRole = Metapb.ShardRole.Learner;
                break;
        }
        return shardRole;
    }

    public static PartitionRole fromShardRole(Metapb.ShardRole shard){
        PartitionRole role = PartitionRole.FOLLOWER;
        switch (shard){
            case Leader:
                role = PartitionRole.LEADER;
                break;
            case Follower:
                role = PartitionRole.FOLLOWER;
                break;
            case Learner:
                role = PartitionRole.LEARNER;
                break;
        }
        return role;

    }
}
