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

package org.apache.hugegraph.pd.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.pd.TaskScheduleService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.junit.Before;
import org.junit.Test;

public class TaskScheduleServiceTest extends PDCoreTestBase {

    TaskScheduleService service;

    @Before
    public void init() {
        this.service = getTaskService();
    }

    // TODO
    public void testStoreOffline() {

    }

    // TODO
    public void testPatrolStores() {

    }

    // TODO
    public void testPatrolPartitions() {

    }

    // TODO
    public void testBalancePartitionShard() {

    }

    @Test
    public void testBalancePartitionLeader() throws PDException {
        var list = new ArrayList<Metapb.Partition>();
        for (int i = 0; i < 6; i++) {
            getStoreNodeService().getStoreInfoMeta().updateShardGroup(genShardGroup(i));
            list.add(genPartition(i));
        }

        getPdConfig().getPartition().setShardCount(3);

        getPartitionService().updatePartition(list);
        var rst = this.service.balancePartitionLeader(true);
        assertFalse(rst.isEmpty());
        // recover
        getPdConfig().getPartition().setShardCount(1);
        getStoreNodeService().getStoreInfoMeta().removeAll();
    }

    @Test
    public void testAutoBalancePartitionLeaderWithinSkew() throws PDException {
        // leaders spread 2/2/2 over the three stores
        var rst = autoBalance(new long[]{1, 2, 3, 1, 2, 3}, 1, 2, 3);
        assertTrue(rst.isEmpty());
    }

    @Test
    public void testAutoBalancePartitionLeaderOverSkew() throws PDException {
        // every leader on store 1, as after stores 2 and 3 restarted
        var rst = autoBalance(new long[]{1, 1, 1, 1, 1, 1}, 1, 2, 3);
        assertEquals(4, rst.size());
        assertFalse(rst.containsValue(1L));
    }

    @Test
    public void testAutoBalancePartitionLeaderWithInactiveStore() throws PDException {
        // store 3 holds shards but sends no heartbeat, so it cannot take a leader
        var rst = autoBalance(new long[]{1, 1, 1, 1, 1, 1}, 1, 2);
        assertTrue(rst.isEmpty());
    }

    /**
     * Stores shard groups 0..n-1 on stores 1, 2 and 3 with the leader of group i on
     * leaderStores[i], marks activeStores alive and runs one periodic balance.
     */
    private Map<Integer, Long> autoBalance(long[] leaderStores, long... activeStores) throws
                                                                                 PDException {
        var storeMeta = getStoreNodeService().getStoreInfoMeta();
        var partitions = new ArrayList<Metapb.Partition>();
        for (int i = 0; i < leaderStores.length; i++) {
            storeMeta.updateShardGroup(genShardGroup(i, leaderStores[i]));
            partitions.add(genPartition(i));
        }
        for (long storeId = 1; storeId <= 3; storeId++) {
            var store = Metapb.Store.newBuilder().setId(storeId).build();
            if (Arrays.stream(activeStores).anyMatch(id -> id == store.getId())) {
                storeMeta.keepStoreAlive(store);
            } else {
                storeMeta.removeActiveStore(store);
            }
        }
        getPdConfig().getPartition().setShardCount(3);
        getPartitionService().updatePartition(partitions);
        try {
            // A fresh service, so a balance run by another test does not hit the rate limit
            var service = new TaskScheduleService(getPdConfig(), getStoreNodeService(),
                                                  getPartitionService());
            return service.autoBalancePartitionLeader();
        } finally {
            getPdConfig().getPartition().setShardCount(1);
            for (long storeId : activeStores) {
                storeMeta.removeActiveStore(Metapb.Store.newBuilder().setId(storeId).build());
            }
            for (int i = 0; i < leaderStores.length; i++) {
                storeMeta.deleteShardGroup(i);
            }
        }
    }

    // TODO
    public void testSplitPartition() {

    }

    // TODO
    public void testSplitPartition2() {

    }

    // TODO
    public void testCanAllPartitionsMovedOut() {

    }

    private Metapb.ShardGroup genShardGroup(int groupId) {
        return Metapb.ShardGroup.newBuilder()
                                .setId(groupId)
                                .addAllShards(genShards())
                                .build();
    }

    private Metapb.Partition genPartition(int groupId) {
        return Metapb.Partition.newBuilder()
                               .setId(groupId)
                               .setState(Metapb.PartitionState.PState_Normal)
                               .setGraphName("graph1")
                               .setStartKey(groupId * 10L)
                               .setEndKey(groupId * 10L + 10)
                               .build();
    }

    private Metapb.ShardGroup genShardGroup(int groupId, long leaderStore) {
        var shards = new ArrayList<Metapb.Shard>();
        for (long storeId = 1; storeId <= 3; storeId++) {
            var role = storeId == leaderStore ? Metapb.ShardRole.Leader :
                       Metapb.ShardRole.Follower;
            shards.add(Metapb.Shard.newBuilder().setStoreId(storeId).setRole(role).build());
        }
        return Metapb.ShardGroup.newBuilder()
                                .setId(groupId)
                                .addAllShards(shards)
                                .build();
    }

    private List<Metapb.Shard> genShards() {
        return List.of(
                Metapb.Shard.newBuilder().setStoreId(1).setRole(Metapb.ShardRole.Leader).build(),
                Metapb.Shard.newBuilder().setStoreId(2).setRole(Metapb.ShardRole.Follower).build(),
                Metapb.Shard.newBuilder().setStoreId(3).setRole(Metapb.ShardRole.Follower).build());
    }

}
