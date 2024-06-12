/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.pd.common;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.pd.grpc.Metapb;
import org.junit.Before;
import org.junit.Test;

public class PartitionCacheTest {

    private PartitionCache cache;

    private static Metapb.Partition createPartition(int pid, String graphName, long start,
                                                    long end) {
        return Metapb.Partition.newBuilder()
                               .setId(pid)
                               .setGraphName(graphName)
                               .setStartKey(start)
                               .setEndKey(end)
                               .setState(Metapb.PartitionState.PState_Normal)
                               .setVersion(1)
                               .build();
    }

    private static Metapb.ShardGroup creteShardGroup(int pid) {
        return Metapb.ShardGroup.newBuilder()
                                .addShards(
                                        Metapb.Shard.newBuilder().setStoreId(0)
                                                    .setRole(Metapb.ShardRole.Leader).build()
                                )
                                .setId(pid)
                                .setVersion(0)
                                .setConfVer(0)
                                .setState(Metapb.PartitionState.PState_Normal)
                                .build();
    }

    private static Metapb.Shard createShard() {
        return Metapb.Shard.newBuilder()
                           .setStoreId(0)
                           .setRole(Metapb.ShardRole.Leader)
                           .build();
    }

    private static Metapb.Store createStore(long storeId) {
        return Metapb.Store.newBuilder()
                           .setId(storeId)
                           .setAddress("127.0.0.1")
                           .setCores(4)
                           .setVersion("1")
                           .setDataPath("/tmp/junit")
                           .setDataVersion(1)
                           .setLastHeartbeat(System.currentTimeMillis())
                           .setStartTimestamp(System.currentTimeMillis())
                           .setState(Metapb.StoreState.Up)
                           .setDeployPath("/tmp/junit")
                           .build();
    }

    private static Metapb.Graph createGraph(String graphName, int partitionCount) {
        return Metapb.Graph.newBuilder()
                           .setGraphName(graphName)
                           .setPartitionCount(partitionCount)
                           .setState(Metapb.PartitionState.PState_Normal)
                           .build();
    }

    private static Metapb.ShardGroup createShardGroup() {
        List<Metapb.Shard> shards = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            shards.add(Metapb.Shard.newBuilder()
                                   .setStoreId(i)
                                   .setRole(i == 0 ? Metapb.ShardRole.Leader :
                                            Metapb.ShardRole.Follower)
                                   .build()
            );
        }

        return Metapb.ShardGroup.newBuilder()
                                .setId(1)
                                .setVersion(1)
                                .setConfVer(1)
                                .setState(Metapb.PartitionState.PState_Normal)
                                .addAllShards(shards)
                                .build();
    }

    @Before
    public void setup() {
        this.cache = new PartitionCache();
    }

    @Test
    public void testGetPartitionById() {
        var partition = createPartition(0, "graph0", 0, 65535);
        this.cache.updateShardGroup(creteShardGroup(0));
        this.cache.updatePartition(partition);
        var ret = this.cache.getPartitionById("graph0", 0);
        assertNotNull(ret);
        assertEquals(ret.getKey(), partition);
    }

    @Test
    public void testGetPartitionByKey() throws UnsupportedEncodingException {
        var partition = createPartition(0, "graph0", 0, 65535);
        this.cache.updateShardGroup(creteShardGroup(0));
        this.cache.updatePartition(partition);
        var ret = this.cache.getPartitionByKey("graph0", "0".getBytes(StandardCharsets.UTF_8));
        assertNotNull(ret);
        assertEquals(ret.getKey(), partition);
    }

    @Test
    public void getPartitionByCode() {
        var partition = createPartition(0, "graph0", 0, 1024);
        this.cache.updateShardGroup(creteShardGroup(0));
        this.cache.updatePartition(partition);
        var ret = this.cache.getPartitionByCode("graph0", 10);
        assertNotNull(ret);
        assertEquals(ret.getKey(), partition);
        assertNull(this.cache.getPartitionByCode("graph0", 2000));
    }

    @Test
    public void testGetPartitions() {
        var partition1 = createPartition(0, "graph0", 0, 1024);
        this.cache.updateShardGroup(creteShardGroup(0));
        this.cache.updatePartition(partition1);
        assertEquals(this.cache.getPartitions("graph0").size(), 1);
        var partition2 = createPartition(1, "graph0", 1024, 2048);
        this.cache.updateShardGroup(creteShardGroup(1));
        this.cache.updatePartition(partition2);
        assertEquals(this.cache.getPartitions("graph0").size(), 2);
        System.out.println(this.cache.debugCacheByGraphName("graph0"));
    }

    @Test
    public void testAddPartition() {
        var partition = createPartition(0, "graph0", 0, 65535);
        this.cache.addPartition("graph0", 0, partition);
        var ret = this.cache.getPartitionById("graph0", 0);
        assertNotNull(ret);
        assertEquals(ret.getKey(), partition);
        assertNotNull(this.cache.getPartitionByCode("graph0", 2000));
        System.out.println(this.cache.debugCacheByGraphName("graph0"));
        var partition2 = createPartition(0, "graph0", 0, 1024);
        this.cache.addPartition("graph0", 0, partition2);
        ret = this.cache.getPartitionById("graph0", 0);
        assertNotNull(ret);
        assertEquals(ret.getKey(), partition2);
        assertNull(this.cache.getPartitionByCode("graph0", 2000));
        System.out.println(this.cache.debugCacheByGraphName("graph0"));
    }

    @Test
    public void testUpdatePartition() {
        var partition = createPartition(0, "graph0", 0, 65535);
        this.cache.updateShardGroup(creteShardGroup(0));
        this.cache.addPartition("graph0", 0, partition);
        var partition2 = createPartition(0, "graph0", 0, 1024);
        this.cache.updatePartition("graph0", 0, partition2);
        var ret = this.cache.getPartitionById("graph0", 0);
        assertNotNull(ret);
        assertEquals(ret.getKey(), partition2);
        assertNull(this.cache.getPartitionByCode("graph0", 2000));
    }

    @Test
    public void testUpdatePartition2() {
        var partition = createPartition(0, "graph0", 0, 1024);
        this.cache.updateShardGroup(creteShardGroup(0));
        assertTrue(this.cache.updatePartition(partition));
        assertFalse(this.cache.updatePartition(partition));
        var ret = this.cache.getPartitionById("graph0", 0);
        assertNotNull(ret);
        assertEquals(ret.getKey(), partition);
        assertNull(this.cache.getPartitionByCode("graph0", 2000));
    }

    @Test
    public void testRemovePartition() {
        var partition = createPartition(0, "graph0", 0, 1024);
        this.cache.updateShardGroup(creteShardGroup(0));
        this.cache.updatePartition(partition);
        assertNotNull(this.cache.getPartitionById("graph0", 0));
        this.cache.removePartition("graph0", 0);
        assertNull(this.cache.getPartitionById("graph0", 0));
        System.out.println(this.cache.debugCacheByGraphName("graph0"));
    }

    @Test
    public void testRange() {
        var partition1 = createPartition(1, "graph0", 0, 3);
        var partition2 = createPartition(2, "graph0", 3, 6);
        this.cache.updatePartition(partition1);
        this.cache.updatePartition(partition2);

        var partition3 = createPartition(3, "graph0", 1, 2);
        var partition4 = createPartition(4, "graph0", 2, 3);
        this.cache.updatePartition(partition3);
        this.cache.updatePartition(partition4);
        System.out.println(this.cache.debugCacheByGraphName("graph0"));

        var partition6 = createPartition(1, "graph0", 0, 1);
        this.cache.updatePartition(partition6);
        System.out.println(this.cache.debugCacheByGraphName("graph0"));

        var partition5 = createPartition(1, "graph0", 0, 3);
        this.cache.updatePartition(partition5);
        System.out.println(this.cache.debugCacheByGraphName("graph0"));
    }

    @Test
    public void testRange2() {
        var partition1 = createPartition(1, "graph0", 0, 3);
        var partition2 = createPartition(2, "graph0", 3, 6);
        this.cache.updatePartition(partition1);
        this.cache.updatePartition(partition2);
        System.out.println(this.cache.debugCacheByGraphName("graph0"));

        var partition3 = createPartition(1, "graph0", 2, 3);
        this.cache.updatePartition(partition3);
        System.out.println(this.cache.debugCacheByGraphName("graph0"));

        var partition5 = createPartition(1, "graph0", 0, 3);
        this.cache.updatePartition(partition5);
        System.out.println(this.cache.debugCacheByGraphName("graph0"));
    }

    @Test
    public void testRemovePartitions() {
        var partition1 = createPartition(0, "graph0", 0, 1024);
        var partition2 = createPartition(1, "graph0", 1024, 2048);
        this.cache.updateShardGroup(creteShardGroup(0));
        this.cache.updatePartition(partition1);
        this.cache.updateShardGroup(creteShardGroup(1));
        this.cache.updatePartition(partition2);
        assertEquals(this.cache.getPartitions("graph0").size(), 2);
        this.cache.removePartitions();
        assertEquals(this.cache.getPartitions("graph0").size(), 0);
    }

    @Test
    public void testRemoveAll() {
        var partition1 = createPartition(0, "graph0", 0, 1024);
        var partition2 = createPartition(1, "graph0", 1024, 2048);
        var partition3 = createPartition(0, "graph1", 0, 2048);
        this.cache.updateShardGroup(creteShardGroup(0));
        this.cache.updateShardGroup(creteShardGroup(1));
        this.cache.updatePartition(partition1);
        this.cache.updatePartition(partition2);
        this.cache.updatePartition(partition3);

        assertEquals(this.cache.getPartitions("graph0").size(), 2);
        assertEquals(this.cache.getPartitions("graph1").size(), 1);
        this.cache.removeAll("graph0");
        assertEquals(this.cache.getPartitions("graph0").size(), 0);
        assertEquals(this.cache.getPartitions("graph1").size(), 1);
    }

    @Test
    public void testUpdateShardGroup() {
        var shardGroup = createShardGroup();
        this.cache.updateShardGroup(shardGroup);
        assertNotNull(this.cache.getShardGroup(shardGroup.getId()));
    }

    @Test
    public void testGetShardGroup() {
        var shardGroup = createShardGroup();
        this.cache.updateShardGroup(shardGroup);
        assertEquals(this.cache.getShardGroup(shardGroup.getId()), shardGroup);
    }

    @Test
    public void testAddStore() {
        var store = createStore(1);
        this.cache.addStore(1L, store);
        assertEquals(this.cache.getStoreById(1L), store);
    }

    @Test
    public void testGetStoreById() {
        var store = createStore(1);
        this.cache.addStore(1L, store);
        assertEquals(this.cache.getStoreById(1L), store);
    }

    @Test
    public void testRemoveStore() {
        var store = createStore(1);
        this.cache.addStore(1L, store);
        assertEquals(this.cache.getStoreById(1L), store);

        this.cache.removeStore(1L);
        assertNull(this.cache.getStoreById(1L));
    }

    @Test
    public void testHasGraph() {
        var partition = createPartition(0, "graph0", 0, 65535);
        this.cache.updateShardGroup(creteShardGroup(0));
        this.cache.updatePartition(partition);
        assertTrue(this.cache.hasGraph("graph0"));
        assertFalse(this.cache.hasGraph("graph1"));
    }

    @Test
    public void testUpdateGraph() {
        var graph = createGraph("graph0", 10);
        this.cache.updateGraph(graph);
        assertEquals(this.cache.getGraph("graph0"), graph);
        graph = createGraph("graph0", 12);
        this.cache.updateGraph(graph);
        assertEquals(this.cache.getGraph("graph0"), graph);
    }

    @Test
    public void testGetGraph() {
        var graph = createGraph("graph0", 12);
        this.cache.updateGraph(graph);
        assertEquals(this.cache.getGraph("graph0"), graph);
    }

    @Test
    public void testGetGraphs() {
        var graph1 = createGraph("graph0", 12);
        var graph2 = createGraph("graph1", 12);
        var graph3 = createGraph("graph2", 12);
        this.cache.updateGraph(graph1);
        this.cache.updateGraph(graph2);
        this.cache.updateGraph(graph3);
        assertEquals(this.cache.getGraphs().size(), 3);
    }

    @Test
    public void testReset() {
        var graph1 = createGraph("graph0", 12);
        var graph2 = createGraph("graph1", 12);
        var graph3 = createGraph("graph2", 12);
        this.cache.updateGraph(graph1);
        this.cache.updateGraph(graph2);
        this.cache.updateGraph(graph3);
        assertEquals(this.cache.getGraphs().size(), 3);
        this.cache.reset();
        assertEquals(this.cache.getGraphs().size(), 0);
    }

    @Test
    public void testUpdateShardGroupLeader() {
        var shardGroup = createShardGroup();
        this.cache.updateShardGroup(shardGroup);

        var leader =
                Metapb.Shard.newBuilder().setStoreId(2).setRole(Metapb.ShardRole.Leader).build();
        this.cache.updateShardGroupLeader(shardGroup.getId(), leader);

        assertEquals(this.cache.getLeaderShard(shardGroup.getId()), leader);
    }
}
