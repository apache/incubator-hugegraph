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
import java.util.function.ToLongBiFunction;
import java.util.function.UnaryOperator;

import org.apache.hugegraph.pd.StoreNodeService;
import org.apache.hugegraph.pd.TaskScheduleService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.meta.MetadataFactory;
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

    @Test
    public void testAutoBalancePartitionLeaderWithWarnGroup() throws PDException {
        // store 3 just stopped: still in the active set, but its groups are already Warn
        var rst = autoBalance(new long[]{1, 1, 1, 1, 1, 1},
                              g -> g.toBuilder().setState(Metapb.PartitionState.PState_Warn)
                                    .build(), 0, 1, 2, 3);
        assertTrue(rst.isEmpty());

        // once the group is Normal again, the same imbalance is balanced
        rst = autoBalance(new long[]{1, 1, 1, 1, 1, 1}, g -> g, 0, 1, 2, 3);
        assertEquals(4, rst.size());
    }

    @Test
    public void testAutoBalancePartitionLeaderWithoutQuorum() throws PDException {
        // group 0 lost its quorum: no partition heartbeat for a minute, while its last state
        // is still Normal and every store is still in the active set
        var rst = autoBalance(new long[]{1, 1, 1, 1, 1, 1}, g -> g, 60 * 1000, 1, 2, 3);
        assertTrue(rst.isEmpty());

        // group 0 never reported stats at all
        rst = autoBalance(new long[]{1, 1, 1, 1, 1, 1}, g -> g, -1, 1, 2, 3);
        assertTrue(rst.isEmpty());

        // a heartbeat within the window: the same imbalance is balanced
        rst = autoBalance(new long[]{1, 1, 1, 1, 1, 1}, g -> g, 10 * 1000, 1, 2, 3);
        assertEquals(4, rst.size());
    }

    @Test
    public void testAutoBalancePartitionLeaderWithPendingMember() throws PDException {
        // reallocShards added the shard on store 3 and left its role None
        for (var role : List.of(Metapb.ShardRole.None, Metapb.ShardRole.Learner)) {
            var rst = autoBalance(new long[]{1, 1, 1, 1, 1, 1},
                                  g -> g.toBuilder().setShards(2, g.getShards(2).toBuilder()
                                                                   .setRole(role))
                                        .build(), 0, 1, 2, 3);
            assertTrue(role.name(), rst.isEmpty());
        }
    }

    @Test
    public void testAutoBalancePartitionLeaderWithChangeAfterCheck() throws PDException {
        // reallocShards turns store 3's shards into new None members right after the check
        // read the groups: the plan is made from the checked groups and no transfer is sent
        var rst = autoBalance(new long[]{1, 1, 1, 1, 1, 1}, g -> g, 0,
                              (storeId, groupId) -> 10_000,
                              g -> g.toBuilder().setShards(2, g.getShards(2).toBuilder()
                                                               .setRole(Metapb.ShardRole.None))
                                    .build(), 1, 2, 3);
        assertTrue(rst.isEmpty());
    }

    @Test
    public void testAutoBalancePartitionLeaderWithLaggingFollower() throws PDException {
        // store 3's replicas trail the leaders by 5000 entries or report no index: its share
        // of the leaders stays on store 1 and only store 2 takes leaders
        for (long store3Index : new long[]{5_000, -1}) {
            var rst = autoBalance(new long[]{1, 1, 1, 1, 1, 1}, g -> g, 0,
                                  (storeId, groupId) -> storeId == 3 ? store3Index : 10_000,
                                  null, 1, 2, 3);
            assertEquals(2, rst.size());
            assertTrue(rst.values().stream().allMatch(storeId -> storeId == 2L));
        }

        // 500 entries behind is close enough to take a leader
        var rst = autoBalance(new long[]{1, 1, 1, 1, 1, 1}, g -> g, 0,
                              (storeId, groupId) -> storeId == 3 ? 9_500 : 10_000,
                              null, 1, 2, 3);
        assertEquals(4, rst.size());
        assertTrue(rst.containsValue(3L));
    }

    @Test
    public void testAutoBalancePartitionLeaderWithNegativeSkew() throws PDException {
        // with the default skew this layout moves four leaders; -1 must not run a plan
        getPdConfig().setBalanceLeaderMaxSkew(-1);
        try {
            var rst = autoBalance(new long[]{1, 1, 1, 1, 1, 1}, 1, 2, 3);
            assertTrue(rst.isEmpty());
        } finally {
            getPdConfig().setBalanceLeaderMaxSkew(2);
        }
    }

    private Map<Integer, Long> autoBalance(long[] leaderStores, long... activeStores) throws
                                                                                 PDException {
        return autoBalance(leaderStores, g -> g, 0, activeStores);
    }

    private Map<Integer, Long> autoBalance(long[] leaderStores,
                                           UnaryOperator<Metapb.ShardGroup> group0,
                                           long group0StatsAge,
                                           long... activeStores) throws PDException {
        return autoBalance(leaderStores, group0, group0StatsAge, (storeId, groupId) -> 10_000,
                           null, activeStores);
    }

    /**
     * Stores shard groups 0..n-1 on stores 1, 2 and 3 with the leader of group i on
     * leaderStores[i], all Normal with fresh partition stats, then applies group0 to group 0
     * and ages its stats by group0StatsAge ms (-1: no stats). Marks activeStores alive, each
     * reporting appliedIndex(store, group) for every group (negative: not reported), and runs
     * one periodic balance. A non-null afterRead is applied to every stored group right after
     * the balance reads the group list, like a change landing between check and transfer.
     */
    private Map<Integer, Long> autoBalance(long[] leaderStores,
                                           UnaryOperator<Metapb.ShardGroup> group0,
                                           long group0StatsAge,
                                           ToLongBiFunction<Long, Integer> appliedIndex,
                                           UnaryOperator<Metapb.ShardGroup> afterRead,
                                           long... activeStores) throws PDException {
        var storeMeta = getStoreNodeService().getStoreInfoMeta();
        var partitionMeta = MetadataFactory.newPartitionMeta(getPdConfig());
        // Set aside groups other tests in the suite left behind, so only ours are planned
        var otherGroups = storeMeta.getShardGroups();
        for (var group : otherGroups) {
            storeMeta.deleteShardGroup(group.getId());
        }
        var partitions = new ArrayList<Metapb.Partition>();
        long now = System.currentTimeMillis();
        for (int i = 0; i < leaderStores.length; i++) {
            var group = genShardGroup(i, leaderStores[i], Metapb.PartitionState.PState_Normal);
            long statsAge = 0;
            if (i == 0) {
                group = group0.apply(group);
                statsAge = group0StatsAge;
            }
            storeMeta.updateShardGroup(group);
            if (statsAge >= 0) {
                partitionMeta.updatePartitionStats(Metapb.PartitionStats.newBuilder()
                                                                        .setId(i)
                                                                        .setTimestamp(
                                                                                now - statsAge)
                                                                        .build());
            }
            partitions.add(genPartition(i));
        }
        for (long storeId = 1; storeId <= 3; storeId++) {
            var stats = Metapb.StoreStats.newBuilder();
            for (int i = 0; i < leaderStores.length; i++) {
                long index = appliedIndex.applyAsLong(storeId, i);
                if (index >= 0) {
                    stats.addRaftStats(Metapb.RaftStats.newBuilder().setPartitionId(i)
                                                       .setCommittedIndex(index));
                }
            }
            var store = Metapb.Store.newBuilder().setId(storeId).setStats(stats).build();
            if (Arrays.stream(activeStores).anyMatch(id -> id == store.getId())) {
                storeMeta.keepStoreAlive(store);
            } else {
                storeMeta.removeActiveStore(store);
            }
        }
        getPdConfig().getPartition().setShardCount(3);
        getPartitionService().updatePartition(partitions);
        try {
            StoreNodeService storeService = getStoreNodeService();
            if (afterRead != null) {
                storeService = new StoreNodeService(getPdConfig()) {
                    @Override
                    public List<Metapb.ShardGroup> getShardGroups() throws PDException {
                        var groups = super.getShardGroups();
                        for (var group : groups) {
                            storeMeta.updateShardGroup(afterRead.apply(group));
                        }
                        return groups;
                    }
                };
            }
            // A fresh service, so a balance run by another test does not hit the rate limit
            var service = new TaskScheduleService(getPdConfig(), storeService,
                                                  getPartitionService());
            return service.autoBalancePartitionLeader();
        } finally {
            getPdConfig().getPartition().setShardCount(1);
            for (long storeId : activeStores) {
                storeMeta.removeActiveStore(Metapb.Store.newBuilder().setId(storeId).build());
            }
            partitionMeta.removePartitionStats("");
            for (int i = 0; i < leaderStores.length; i++) {
                storeMeta.deleteShardGroup(i);
            }
            for (var group : otherGroups) {
                storeMeta.updateShardGroup(group);
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

    private Metapb.ShardGroup genShardGroup(int groupId, long leaderStore,
                                            Metapb.PartitionState state) {
        var shards = new ArrayList<Metapb.Shard>();
        for (long storeId = 1; storeId <= 3; storeId++) {
            var role = storeId == leaderStore ? Metapb.ShardRole.Leader :
                       Metapb.ShardRole.Follower;
            shards.add(Metapb.Shard.newBuilder().setStoreId(storeId).setRole(role).build());
        }
        return Metapb.ShardGroup.newBuilder()
                                .setId(groupId)
                                .setState(state)
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
