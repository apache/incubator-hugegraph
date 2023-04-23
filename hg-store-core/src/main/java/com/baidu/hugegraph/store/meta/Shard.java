package com.baidu.hugegraph.store.meta;

import com.baidu.hugegraph.pd.grpc.Metapb;
import lombok.Data;

/**
 * 一个分片
 */
@Data
public class Shard {
    private long storeId;
    private Metapb.ShardRole role;

    public Metapb.Shard toMetaPbShard() {
        return Metapb.Shard.newBuilder().setStoreId(storeId).setRole(role).build();
    }

    public static Shard fromMetaPbShard(Metapb.Shard shard){
        Shard s = new Shard();
        s.setRole(shard.getRole());
        s.setStoreId(shard.getStoreId());
        return s;
    }
}
