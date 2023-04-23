package com.baidu.hugegraph.store.meta;

import com.baidu.hugegraph.pd.grpc.Metapb;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * 分片副本组
 */
@Data
public class ShardGroup {
    private int id;
    /**
     * Leader任期，leader切换后递增 = raftNode.leader_term
     * 无实际用处
     */
    private long version;
    /**
     * shards版本号，每次改变后递增
     */
    private long confVersion;

    private List<Shard> shards = new CopyOnWriteArrayList<>();

    public ShardGroup addShard(Shard shard){
        this.shards.add(shard);
        return this;
    }

    public static ShardGroup from(Metapb.ShardGroup meta){
        if (meta == null) {
            return null;
        }
        ShardGroup shardGroup = new ShardGroup();
        shardGroup.setId(meta.getId());
        shardGroup.setVersion(meta.getVersion());
        shardGroup.setConfVersion(meta.getConfVer());
        shardGroup.setShards(meta.getShardsList().stream().map(Shard::fromMetaPbShard).collect(Collectors.toList()));
        return shardGroup;
    }

    public synchronized ShardGroup changeLeader(long storeId){
        shards.forEach(shard -> {
            shard.setRole(shard.getStoreId() == storeId ? Metapb.ShardRole.Leader : Metapb.ShardRole.Follower);
        });
        return this;
    }

    public synchronized ShardGroup changeShardList(List<Long> peerIds, List<Long> learners, long leaderId) {
        if (!peerIds.isEmpty()) {
            shards.clear();
            peerIds.forEach(id -> {
                shards.add(new Shard() {{
                    setStoreId(id);
                    setRole(id == leaderId ? Metapb.ShardRole.Leader : Metapb.ShardRole.Follower);
                }});
            });

            learners.forEach(id -> {
                shards.add(new Shard() {{
                    setStoreId(id);
                    setRole(Metapb.ShardRole.Learner);
                }});
            });
            confVersion = confVersion + 1;
        }
        return this;
    }

    public synchronized List<Metapb.Shard> getMetaPbShard(){
        List<Metapb.Shard> shardList = new ArrayList<>();
        shards.forEach(shard -> {
            shardList.add(shard.toMetaPbShard());
        });
        return shardList;
    }

    @Override
    public String toString(){
        StringBuilder builder = new StringBuilder();
        shards.forEach(e->{
            builder.append(String.format("{ id:%s,role:%s },", e.getStoreId(), e.getRole()));
        });
        return builder.length() > 0 ? builder.substring(0, builder.length() - 1) : "";
    }

    public Metapb.ShardGroup getProtoObj(){
        return Metapb.ShardGroup.newBuilder()
                .setId(this.id)
                .setVersion(this.version)
                .setConfVer(this.confVersion)
                .addAllShards(getMetaPbShard())
                .build();
    }
}
