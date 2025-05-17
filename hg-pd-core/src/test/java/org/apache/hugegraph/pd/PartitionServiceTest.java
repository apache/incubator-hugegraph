package org.apache.hugegraph.pd;

import org.apache.hugegraph.pd.grpc.Metapb;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class PartitionServiceTest {
    @Test
    public void testPartitionHeartbeat() {
        List<Metapb.Shard> shardList = new ArrayList<>();
        shardList.add(Metapb.Shard.newBuilder().setStoreId(1).build());
        shardList.add(Metapb.Shard.newBuilder().setStoreId(2).build());
        shardList.add(Metapb.Shard.newBuilder().setStoreId(3).build());
        shardList = new ArrayList<>(shardList);
        Metapb.PartitionStats stats = Metapb.PartitionStats.newBuilder()
                .addAllShard(shardList).build();
        List<Metapb.Shard> shardList2 = new ArrayList<>(stats.getShardList());
        Collections.shuffle(shardList2);
        shardList2.forEach(shard -> {
            System.out.println(shard.getStoreId());
        });


    }
}
