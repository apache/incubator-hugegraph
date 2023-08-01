/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.pd;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.pulse.ChangeShard;
import org.apache.hugegraph.pd.grpc.pulse.CleanPartition;
import org.apache.hugegraph.pd.grpc.pulse.DbCompaction;
import org.apache.hugegraph.pd.grpc.pulse.MovePartition;
import org.apache.hugegraph.pd.grpc.pulse.PartitionKeyRange;
import org.apache.hugegraph.pd.grpc.pulse.SplitPartition;
import org.apache.hugegraph.pd.grpc.pulse.TransferLeader;
import org.junit.Assert;
import org.junit.BeforeClass;

public class StoreNodeServiceTest {
    static PDConfig pdConfig;

    @BeforeClass
    public static void init() throws Exception {
        String path = "tmp/unitTest";
        deleteDirectory(new File(path));
        pdConfig = new PDConfig() {{
            this.setClusterId(100);
            this.setInitialStoreList(
                    "127.0.0.1:8500,127.0.0.1:8501,127.0.0.1:8502,127.0.0.1:8503,127.0.0.1:8504," +
                    "127.0.0.1:8505");
        }};

        pdConfig.setStore(new PDConfig().new Store() {{
            this.setMaxDownTime(3600);
            this.setKeepAliveTimeout(3600);
        }});

        pdConfig.setPartition(new PDConfig().new Partition() {{
            this.setShardCount(3);
            this.setMaxShardsPerStore(3);
        }});
        pdConfig.setRaft(new PDConfig().new Raft() {{
            this.setEnable(false);
        }});
        pdConfig.setDiscovery(new PDConfig().new Discovery());
        pdConfig.setDataPath(path);
        ConfigService configService = new ConfigService(pdConfig);
        pdConfig = configService.loadConfig();
    }

    public static byte[] intToByteArray(int i) {
        byte[] result = new byte[4];
        result[0] = (byte) ((i >> 24) & 0xFF);
        result[1] = (byte) ((i >> 16) & 0xFF);
        result[2] = (byte) ((i >> 8) & 0xFF);
        result[3] = (byte) (i & 0xFF);
        return result;
    }

    public static void deleteDirectory(File dir) {
        try {
            FileUtils.deleteDirectory(dir);
        } catch (IOException e) {
            System.out.printf("Failed to start ....,%s%n", e.getMessage());
        }
    }

    // @Test
    public void testStoreNodeService() throws PDException {
        Assert.assertEquals(pdConfig.getPartition().getTotalCount(),
                            (long) pdConfig.getInitialStoreMap().size() *
                            pdConfig.getPartition().getMaxShardsPerStore()
                            / pdConfig.getPartition().getShardCount());
        StoreNodeService storeService = new StoreNodeService(pdConfig);
        int count = 6;
        Metapb.Store[] stores = new Metapb.Store[count];
        for (int i = 0; i < count; i++) {
            Metapb.Store store = Metapb.Store.newBuilder()
                                             .setId(0)
                                             .setAddress("127.0.0.1:850" + i)
                                             .setDeployPath("/data")
                                             .addLabels(Metapb.StoreLabel.newBuilder()
                                                                         .setKey("namespace")
                                                                         .setValue("default")
                                                                         .build())
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

        Assert.assertEquals(6, storeService.getActiveStores("").size());

        Metapb.Graph graph = Metapb.Graph.newBuilder()
                                         .setGraphName("defaultGH")
                                         .setPartitionCount(10)
                                         .build();
        // 分配shard
        List<Metapb.Shard> shards = storeService.allocShards(graph, 1);


        Assert.assertEquals(3, shards.size());

        Assert.assertEquals(pdConfig.getPartition().getTotalCount(),
                            storeService.getShardGroups().size());        // 设置leader
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

    // @Test
    public void testSplitPartition() throws PDException {
        StoreNodeService storeService = new StoreNodeService(pdConfig);
        PartitionService partitionService = new PartitionService(pdConfig, storeService);

        storeService.init(partitionService);
        partitionService.addInstructionListener(new PartitionInstructionListener() {

            @Override
            public void changeShard(Metapb.Partition partition, ChangeShard changeShard) throws
                                                                                         PDException {

            }

            @Override
            public void transferLeader(Metapb.Partition partition,
                                       TransferLeader transferLeader) throws PDException {

            }

            @Override
            public void splitPartition(Metapb.Partition partition,
                                       SplitPartition splitPartition) throws PDException {
                splitPartition.getNewPartitionList().forEach(p -> {
                    System.out.println("SplitPartition " + p.getId() + " " + p.getStartKey() + "," +
                                       p.getEndKey());
                });
            }

            @Override
            public void dbCompaction(Metapb.Partition partition, DbCompaction dbCompaction) throws
                                                                                            PDException {

            }

            @Override
            public void movePartition(Metapb.Partition partition,
                                      MovePartition movePartition) throws PDException {

            }

            @Override
            public void cleanPartition(Metapb.Partition partition,
                                       CleanPartition cleanPartition) throws PDException {

            }

            @Override
            public void changePartitionKeyRange(Metapb.Partition partition,
                                                PartitionKeyRange partitionKeyRange) throws
                                                                                     PDException {

            }
        });
        int count = 6;
        Metapb.Store[] stores = new Metapb.Store[count];
        for (int i = 0; i < count; i++) {
            Metapb.Store store = Metapb.Store.newBuilder()
                                             .setId(0)
                                             .setAddress("127.0.0.1:850" + i)
                                             .setDeployPath("/data")
                                             .addLabels(Metapb.StoreLabel.newBuilder()
                                                                         .setKey("namespace")
                                                                         .setValue("default")
                                                                         .build())
                                             .build();
            stores[i] = storeService.register(store);
            System.out.println("新注册store， id = " + Long.toHexString(stores[i].getId()));
        }
        Assert.assertEquals(count, storeService.getStores().size());

        Metapb.Graph graph = Metapb.Graph.newBuilder()
                                         .setGraphName("defaultGH")
                                         .build();
        Metapb.PartitionShard ptShard =
                partitionService.getPartitionByCode(graph.getGraphName(), 0);
        System.out.println(ptShard.getPartition().getId());
        {
            Metapb.Partition pt = ptShard.getPartition();
            System.out.println(pt.getId() + " " + pt.getStartKey() + "," + pt.getEndKey());
        }

        Assert.assertEquals(6, storeService.getShardGroups().size());
        // storeService.splitShardGroups(ptShard.getPartition().getId(), 4);
        Assert.assertEquals(9, storeService.getShardGroups().size());
        storeService.getShardGroups().forEach(shardGroup -> {
            System.out.println("shardGroup id = " + shardGroup.getId());
        });
    }

    // @Test
    public void testPartitionService() throws PDException, ExecutionException,
                                              InterruptedException {
        StoreNodeService storeService = new StoreNodeService(pdConfig);
        int count = 6;
        Metapb.Store[] stores = new Metapb.Store[count];
        for (int i = 0; i < count; i++) {
            Metapb.Store store = Metapb.Store.newBuilder()
                                             .setId(0)
                                             .setAddress(String.valueOf(i))
                                             .setDeployPath("/data")
                                             .addLabels(Metapb.StoreLabel.newBuilder()
                                                                         .setKey("namespace")
                                                                         .setValue("default")
                                                                         .build())
                                             .build();
            stores[i] = storeService.register(store);
            System.out.println("新注册store， id = " + Long.toHexString(stores[i].getId()));
        }
        Assert.assertEquals(count, storeService.getStores("").size());


        PartitionService partitionService = new PartitionService(pdConfig, storeService);

        Metapb.Graph graph = Metapb.Graph.newBuilder()
                                         .setGraphName("defaultGH")

                                         .setPartitionCount(10)
                                         .build();
        // 申请分区
        Metapb.PartitionShard[] partitions = new Metapb.PartitionShard[10];
        for (int i = 0; i < partitions.length; i++) {
            partitions[i] =
                    partitionService.getPartitionShard(graph.getGraphName(), intToByteArray(i));
            Assert.assertEquals(3, storeService.getShardGroup(i).getShardsCount());
        }
        System.out.println(
                "分区数量： " + partitionService.getPartitions(graph.getGraphName()).size());

        int[] caseNo = {0}; //1 测试增加shard, 2 //测试store下线

        Metapb.Shard leader = null;
        int[] finalCaseNo = caseNo;

        partitionService.addInstructionListener(new PartitionInstructionListener() {

            @Override
            public void changeShard(Metapb.Partition partition, ChangeShard changeShard) throws
                                                                                         PDException {
                switch (finalCaseNo[0]) {
                    case 2:
                        Assert.assertEquals(5, storeService.getShardGroup(partition.getId())
                                                           .getShardsCount());
                        break;
                    case 3:
                        storeService.getShardGroup(partition.getId()).getShardsList()
                                    .forEach(shard -> {
                                        Assert.assertNotEquals(shard.getStoreId(),
                                                               stores[0].getId());
                                    });
                        break;
                }

            }

            @Override
            public void transferLeader(Metapb.Partition partition, TransferLeader transferLeader) {

            }

            @Override
            public void splitPartition(Metapb.Partition partition, SplitPartition splitPartition) {
            }

            @Override
            public void dbCompaction(Metapb.Partition partition, DbCompaction dbCompaction) throws
                                                                                            PDException {

            }

            @Override
            public void movePartition(Metapb.Partition partition,
                                      MovePartition movePartition) throws PDException {

            }

            @Override
            public void cleanPartition(Metapb.Partition partition,
                                       CleanPartition cleanPartition) throws PDException {

            }

            @Override
            public void changePartitionKeyRange(Metapb.Partition partition,
                                                PartitionKeyRange partitionKeyRange)
                    throws PDException {

            }
        });
        Metapb.Partition partition = partitions[0].getPartition();
        leader = Metapb.Shard.newBuilder(
                storeService.getShardGroup(partition.getId()).getShardsList().get(0)).build();
        Metapb.Shard finalLeader = leader;
        partitionService.addStatusListener(new PartitionStatusListener() {
            @Override
            public void onPartitionChanged(Metapb.Partition partition,
                                           Metapb.Partition newPartition) {

            }

            @Override
            public void onPartitionRemoved(Metapb.Partition partition) {

            }
        });
        // 测试修改图
        caseNo[0] = 1;
        partitionService.updateGraph(graph);
        for (int i = 0; i < partitions.length; i++) {
            partitions[i] =
                    partitionService.getPartitionShard(graph.getGraphName(), intToByteArray(i));
            Assert.assertEquals(3, storeService.getShardGroup(i).getShardsCount());
        }

        graph = Metapb.Graph.newBuilder(graph)
                            .setGraphName("defaultGH")

                            .setPartitionCount(10)
                            .build();
        caseNo[0] = 2;
        partitionService.updateGraph(graph);

        // 测试store离线
        caseNo[0] = 3;
        partitionService.storeOffline(stores[0]);


        Metapb.PartitionStats stats = Metapb.PartitionStats.newBuilder()
                                                           .addGraphName(partition.getGraphName())
                                                           .setId(partition.getId())
                                                           .setLeader(
                                                                   Metapb.Shard.newBuilder(leader)
                                                                               .setRole(
                                                                                       Metapb.ShardRole.Leader))
                                                           .build();
        // 测试leader飘移
        caseNo[0] = 4;
        partitionService.partitionHeartbeat(stats);
        AtomicReference<Metapb.Shard> shard = new AtomicReference<>();
        Metapb.PartitionShard ss =
                partitionService.getPartitionShardById(partition.getGraphName(), partition.getId());
        storeService.getShardList(partition.getId()).forEach(s -> {
            if (s.getRole() == Metapb.ShardRole.Leader) {
                Assert.assertNull(shard.get());
                shard.set(s);
            }
        });

        Assert.assertEquals(leader.getStoreId(), shard.get().getStoreId());

    }

    // @Test
    public void testMergeGraphParams() throws PDException {
        StoreNodeService storeService = new StoreNodeService(pdConfig);
        PartitionService partitionService = new PartitionService(pdConfig, storeService);

        Metapb.Graph dfGraph = Metapb.Graph.newBuilder()

                                           .setPartitionCount(
                                                   pdConfig.getPartition().getTotalCount())

                                           .build();

        Metapb.Graph graph1 = Metapb.Graph.newBuilder()
                                          .setGraphName("test")
                                          .setPartitionCount(20)

                                          .build();

        Metapb.Graph graph2 = Metapb.Graph.newBuilder()
                                          .setGraphName("test")
                                          .setPartitionCount(7).build();
        Metapb.Graph graph3 = Metapb.Graph.newBuilder()
                                          .setGraphName("test")
                                          .build();
        Metapb.Graph graph4 = Metapb.Graph.newBuilder()
                                          .setGraphName("test")
                                          .build();

        Metapb.Graph graph = Metapb.Graph.newBuilder(dfGraph).mergeFrom(graph2).build();
        Assert.assertEquals(graph2.getGraphName(), graph.getGraphName());

        Assert.assertEquals(graph2.getPartitionCount(), graph.getPartitionCount());


        graph = Metapb.Graph.newBuilder(dfGraph).mergeFrom(graph3).build();
        Assert.assertEquals(graph3.getGraphName(), graph.getGraphName());

        Assert.assertEquals(dfGraph.getPartitionCount(), graph.getPartitionCount());


        graph = Metapb.Graph.newBuilder(dfGraph).mergeFrom(graph4).build();
        Assert.assertEquals(graph4.getGraphName(), graph.getGraphName());

        Assert.assertEquals(dfGraph.getPartitionCount(), graph.getPartitionCount());

    }

    // @Test
    public void test() {
        int[] n = new int[3];


        if (++n[2] > 1) {
            System.out.println(n[2]);
        }
        if (++n[2] > 1) {
            System.out.println(n[2]);
        }
        if (++n[2] > 1) {
            System.out.println(n[2]);
        }
    }
}
