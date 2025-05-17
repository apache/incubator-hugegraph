package org.apache.hugegraph.pd.core;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hugegraph.pd.PartitionService;
import org.apache.hugegraph.pd.StoreNodeService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;

import lombok.extern.slf4j.Slf4j;

import static org.apache.hugegraph.pd.common.Consts.DEFAULT_STORE_GROUP_ID;

@Slf4j
public class StoreNodeServiceTest extends BaseCoreTest {


    @Test
    public void testStoreNodeService() throws PDException {
        Assert.assertEquals(configService.getPartitionCount(DEFAULT_STORE_GROUP_ID),
                            pdConfig.getInitialStoreMap().size() *
                            pdConfig.getPartition().getMaxShardsPerStore()
                            / pdConfig.getPartition().getShardCount());
        StoreNodeService storeService = new StoreNodeService(pdConfig);
        storeService.init(new PartitionService(pdConfig, storeService, configService));
        int count = 6;
        Metapb.Store[] stores = new Metapb.Store[count];
        for (int i = 0; i < count; i++) {
            Metapb.Store store = Metapb.Store.newBuilder()
                                             .setId(0)
                                             .setAddress("127.0.0.1:850" + i)
                                             .setDeployPath("/data")
                                             .addLabels(Metapb.StoreLabel.newBuilder()
                                                                         .setKey("namespace")
                                                                         .setValue("default").build())
                                             .build();
            stores[i] = storeService.register(store);
            System.out.println("新注册store， id = " + stores[i].getId());
        }
        Assert.assertEquals(count, storeService.getStores("").size());

        for (Metapb.Store store : stores) {
            Metapb.StoreStats stats = Metapb.StoreStats.newBuilder()
                                                       .setStoreId(store.getId())
                                                       .build();
            storeService.heartBeat(stats);
        }

        Assert.assertEquals(6, storeService.getActiveStoresByStoreGroup(DEFAULT_STORE_GROUP_ID).size());

        Metapb.Graph graph = Metapb.Graph.newBuilder()
                                         .setGraphName("defaultGH")
                                         .setPartitionCount(10)
                                         .build();
        // 分配shard
        List<Metapb.Shard> shards = storeService.allocShards(graph, 1);


        Assert.assertEquals(3, shards.size());
        // 设置leader
        Assert.assertEquals(configService.getPartitionCount(DEFAULT_STORE_GROUP_ID),
                storeService.getShardGroups().size());
        Metapb.Shard leader = Metapb.Shard.newBuilder(shards.get(0))
                                          .setRole(Metapb.ShardRole.Leader).build();
        shards = new ArrayList<>(shards);
        shards.set(0, leader);
        // 增加shard
        pdConfig.getPartition().setShardCount(5);

        Metapb.ShardGroup shardGroup = Metapb.ShardGroup.newBuilder()
                                                        .setId(1)
                                                        .addAllShards(shards).build();
        shards = storeService.reallocShards(shardGroup);

        Assert.assertEquals(5, shards.size());
        // 减少shard
        pdConfig.getPartition().setShardCount(3);
        shards = storeService.reallocShards(shardGroup);
        Assert.assertEquals(3, shards.size());
        // 包含leader，leader不能被删除
        Assert.assertTrue(shards.contains(leader));

        // 减少shard
        pdConfig.getPartition().setShardCount(1);
        graph = Metapb.Graph.newBuilder(graph).build();
        shards = storeService.reallocShards(shardGroup);
        Assert.assertEquals(1, shards.size());
        // 包含leader，leader不能被删除
        Assert.assertTrue(shards.contains(leader));

        for (Metapb.Store store : stores) {
            storeService.removeStore(store.getId());
        }
        Assert.assertEquals(0, storeService.getStores("").size());


    }


}