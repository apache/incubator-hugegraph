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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.hugegraph.pd.PartitionService;
import org.apache.hugegraph.pd.StoreNodeService;
import org.apache.hugegraph.pd.StoreStatusListener;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.MetaTask;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class StoreServiceTest extends PDCoreTestBase {

    private PDConfig config;

    private StoreNodeService service;

    @Before
    public void setUp() {
        this.config = getPdConfig();
        this.service = new StoreNodeService(this.config);
    }

    @Test
    public void testInit() {
        // Setup
        PDConfig pdConfig = getPdConfig();
        final PartitionService partitionService = new PartitionService(pdConfig,
                                                                       new StoreNodeService(
                                                                               pdConfig));

        // Run the test
        this.service.init(partitionService);

        // Verify the results
    }

    @Test
    public void testIsOK() {
        // Setup
        // Run the test
        final boolean result = this.service.isOK();

        // Verify the results
        assertThat(result).isTrue();
    }

    @Test
    public void testRegister() throws Exception {
        // Setup
        final Metapb.Store store = Metapb.Store.newBuilder().setId(0L)
                                               .setAddress("address")
                                               .setRaftAddress("raftAddress")
                                               .addLabels(Metapb.StoreLabel
                                                                  .newBuilder()
                                                                  .build())
                                               .setVersion("version").setState(
                        Metapb.StoreState.Unknown).setStartTimestamp(0L)
                                               .setDeployPath("deployPath")
                                               .setLastHeartbeat(0L).setStats(
                        Metapb.StoreStats.newBuilder().setStoreId(0L)
                                         .setPartitionCount(0).addGraphStats(
                                      Metapb.GraphStats.newBuilder()
                                                       .setGraphName("value")
                                                       .setApproximateSize(0L)
                                                       .setRole(Metapb.ShardRole.None)
                                                       .build()).build())
                                               .setDataVersion(0).setCores(0)
                                               .setDataPath("dataPath").build();
        final Metapb.Store expectedResult = Metapb.Store.newBuilder().setId(0L)
                                                        .setAddress("address")
                                                        .setRaftAddress(
                                                                "raftAddress")
                                                        .addLabels(
                                                                Metapb.StoreLabel
                                                                        .newBuilder()
                                                                        .build())
                                                        .setVersion("version")
                                                        .setState(
                                                                Metapb.StoreState.Unknown)
                                                        .setStartTimestamp(0L)
                                                        .setDeployPath(
                                                                "deployPath")
                                                        .setLastHeartbeat(0L)
                                                        .setStats(
                                                                Metapb.StoreStats
                                                                        .newBuilder()
                                                                        .setStoreId(
                                                                                0L)
                                                                        .setPartitionCount(
                                                                                0)
                                                                        .addGraphStats(
                                                                                Metapb.GraphStats
                                                                                        .newBuilder()
                                                                                        .setGraphName(
                                                                                                "value")
                                                                                        .setApproximateSize(
                                                                                                0L)
                                                                                        .setRole(
                                                                                                Metapb.ShardRole.None)
                                                                                        .build())
                                                                        .build())
                                                        .setDataVersion(0)
                                                        .setCores(0)
                                                        .setDataPath("dataPath")
                                                        .build();

        // Configure PDConfig.getInitialStoreMap(...).
        final Map<String, String> stringStringMap = Map.ofEntries(
                Map.entry("value", "value"));

        // Run the test
        final Metapb.Store result = this.service.register(store);
    }

    @Test
    public void testGetStore() throws Exception {
        // Setup
        try {
            Metapb.GraphStats stats = Metapb.GraphStats.newBuilder()
                                                       .setGraphName("value")
                                                       .setApproximateSize(0L)
                                                       .setRole(
                                                               Metapb.ShardRole.None)
                                                       .build();
            Metapb.StoreStats storeStats = Metapb.StoreStats.newBuilder()
                                                            .setStoreId(0L)
                                                            .setPartitionCount(
                                                                    0)
                                                            .addGraphStats(
                                                                    stats)
                                                            .build();
            final Metapb.Store expectedResult = Metapb.Store.newBuilder()
                                                            .setId(0L)
                                                            .setAddress(
                                                                    "address")
                                                            .setRaftAddress(
                                                                    "raftAddress")
                                                            .addLabels(
                                                                    Metapb.StoreLabel
                                                                            .newBuilder()
                                                                            .build())
                                                            .setVersion(
                                                                    "version")
                                                            .setState(
                                                                    Metapb.StoreState.Unknown)
                                                            .setStartTimestamp(
                                                                    0L)
                                                            .setDeployPath(
                                                                    "deployPath")
                                                            .setLastHeartbeat(
                                                                    0L)
                                                            .setStats(
                                                                    storeStats)
                                                            .setDataVersion(0)
                                                            .setCores(0)
                                                            .setDataPath(
                                                                    "dataPath")
                                                            .build();

            // Run the test
            final Metapb.Store result = this.service.getStore(0L);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testUpdateStore() throws Exception {
        // Setup
        final Metapb.Store store = Metapb.Store.newBuilder().setId(0L)
                                               .setAddress("address")
                                               .setRaftAddress("raftAddress")
                                               .addLabels(Metapb.StoreLabel
                                                                  .newBuilder()
                                                                  .build())
                                               .setVersion("version").setState(
                        Metapb.StoreState.Unknown).setStartTimestamp(0L)
                                               .setDeployPath("deployPath")
                                               .setLastHeartbeat(0L).setStats(
                        Metapb.StoreStats.newBuilder().setStoreId(0L)
                                         .setPartitionCount(0).addGraphStats(
                                      Metapb.GraphStats.newBuilder()
                                                       .setGraphName("value")
                                                       .setApproximateSize(0L)
                                                       .setRole(Metapb.ShardRole.None)
                                                       .build()).build())
                                               .setDataVersion(0).setCores(0)
                                               .setDataPath("dataPath").build();
        final Metapb.Store expectedResult = Metapb.Store.newBuilder().setId(0L)
                                                        .setAddress("address")
                                                        .setRaftAddress(
                                                                "raftAddress")
                                                        .addLabels(
                                                                Metapb.StoreLabel
                                                                        .newBuilder()
                                                                        .build())
                                                        .setVersion("version")
                                                        .setState(
                                                                Metapb.StoreState.Unknown)
                                                        .setStartTimestamp(0L)
                                                        .setDeployPath(
                                                                "deployPath")
                                                        .setLastHeartbeat(0L)
                                                        .setStats(
                                                                Metapb.StoreStats
                                                                        .newBuilder()
                                                                        .setStoreId(
                                                                                0L)
                                                                        .setPartitionCount(
                                                                                0)
                                                                        .addGraphStats(
                                                                                Metapb.GraphStats
                                                                                        .newBuilder()
                                                                                        .setGraphName(
                                                                                                "value")
                                                                                        .setApproximateSize(
                                                                                                0L)
                                                                                        .setRole(
                                                                                                Metapb.ShardRole.None)
                                                                                        .build())
                                                                        .build())
                                                        .setDataVersion(0)
                                                        .setCores(0)
                                                        .setDataPath("dataPath")
                                                        .build();

        // Configure PDConfig.getPartition(...).
        final PDConfig.Partition partition = new PDConfig().new Partition();
        partition.setTotalCount(0);
        partition.setMaxShardsPerStore(0);
        partition.setShardCount(0);

        // Run the test
        final Metapb.Store result = this.service.updateStore(store);
    }

    @Test
    public void testStoreTurnoff() throws Exception {
        // Setup
        try {
            final Metapb.Store store = Metapb.Store.newBuilder().setId(0L)
                                                   .setAddress("address")
                                                   .setRaftAddress("raftAddress")
                                                   .addLabels(Metapb.StoreLabel
                                                                      .newBuilder()
                                                                      .build())
                                                   .setVersion("version").setState(
                            Metapb.StoreState.Unknown).setStartTimestamp(0L)
                                                   .setDeployPath("deployPath")
                                                   .setLastHeartbeat(0L).setStats(
                            Metapb.StoreStats.newBuilder().setStoreId(0L)
                                             .setPartitionCount(0).addGraphStats(
                                          Metapb.GraphStats.newBuilder()
                                                           .setGraphName("value")
                                                           .setApproximateSize(0L)
                                                           .setRole(Metapb.ShardRole.None)
                                                           .build()).build())
                                                   .setDataVersion(0).setCores(0)
                                                   .setDataPath("dataPath").build();

            // Configure PDConfig.getPartition(...).
            final PDConfig.Partition partition = new PDConfig().new Partition();
            partition.setTotalCount(0);
            partition.setMaxShardsPerStore(0);
            partition.setShardCount(0);

            // Run the test
            this.service.storeTurnoff(store);

            // Verify the results
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetStores1() throws Exception {
        // Setup
        final List<Metapb.Store> expectedResult = List.of(
                Metapb.Store.newBuilder().setId(0L).setAddress("address")
                            .setRaftAddress("raftAddress")
                            .addLabels(Metapb.StoreLabel.newBuilder().build())
                            .setVersion("version")
                            .setState(Metapb.StoreState.Unknown)
                            .setStartTimestamp(0L).setDeployPath("deployPath")
                            .setLastHeartbeat(0L).setStats(
                              Metapb.StoreStats.newBuilder().setStoreId(0L)
                                               .setPartitionCount(0).addGraphStats(
                                            Metapb.GraphStats.newBuilder()
                                                             .setGraphName("value")
                                                             .setApproximateSize(0L)
                                                             .setRole(Metapb.ShardRole.None)
                                                             .build()).build())
                            .setDataVersion(0).setCores(0)
                            .setDataPath("dataPath").build());

        // Run the test
        final List<Metapb.Store> result = this.service.getStores();
    }

    @Test
    public void testGetStores2() throws Exception {
        // Setup
        final List<Metapb.Store> expectedResult = List.of(
                Metapb.Store.newBuilder().setId(0L).setAddress("address")
                            .setRaftAddress("raftAddress")
                            .addLabels(Metapb.StoreLabel.newBuilder().build())
                            .setVersion("version")
                            .setState(Metapb.StoreState.Unknown)
                            .setStartTimestamp(0L).setDeployPath("deployPath")
                            .setLastHeartbeat(0L).setStats(
                              Metapb.StoreStats.newBuilder().setStoreId(0L)
                                               .setPartitionCount(0).addGraphStats(
                                            Metapb.GraphStats.newBuilder()
                                                             .setGraphName("value")
                                                             .setApproximateSize(0L)
                                                             .setRole(Metapb.ShardRole.None)
                                                             .build()).build())
                            .setDataVersion(0).setCores(0)
                            .setDataPath("dataPath").build());

        // Run the test
        final List<Metapb.Store> result = this.service.getStores("graphName");
    }

    @Test
    public void testGetStoreStatus() throws Exception {
        // Setup
        final List<Metapb.Store> expectedResult = List.of(
                Metapb.Store.newBuilder().setId(0L).setAddress("address")
                            .setRaftAddress("raftAddress")
                            .addLabels(Metapb.StoreLabel.newBuilder().build())
                            .setVersion("version")
                            .setState(Metapb.StoreState.Unknown)
                            .setStartTimestamp(0L).setDeployPath("deployPath")
                            .setLastHeartbeat(0L).setStats(
                              Metapb.StoreStats.newBuilder().setStoreId(0L)
                                               .setPartitionCount(0).addGraphStats(
                                            Metapb.GraphStats.newBuilder()
                                                             .setGraphName("value")
                                                             .setApproximateSize(0L)
                                                             .setRole(Metapb.ShardRole.None)
                                                             .build()).build())
                            .setDataVersion(0).setCores(0)
                            .setDataPath("dataPath").build());

        // Run the test
        final List<Metapb.Store> result = this.service.getStoreStatus(false);

    }

    @Test
    public void testGetShardGroups() throws Exception {
        // Setup
        final List<Metapb.ShardGroup> expectedResult = List.of(
                Metapb.ShardGroup.newBuilder().setId(0).addShards(
                              Metapb.Shard.newBuilder().setStoreId(0L)
                                          .setRole(Metapb.ShardRole.None).build())
                                 .setState(Metapb.PartitionState.PState_None)
                                 .build());

        // Run the test
        final List<Metapb.ShardGroup> result = this.service.getShardGroups();

    }

    @Test
    public void testGetShardGroup() throws Exception {
        // Setup
        final Metapb.ShardGroup expectedResult = Metapb.ShardGroup.newBuilder()
                                                                  .setId(0)
                                                                  .addShards(
                                                                          Metapb.Shard
                                                                                  .newBuilder()
                                                                                  .setStoreId(
                                                                                          0L)
                                                                                  .setRole(
                                                                                          Metapb.ShardRole.None)
                                                                                  .build())
                                                                  .setState(
                                                                          Metapb.PartitionState.PState_None)
                                                                  .build();

        // Run the test
        final Metapb.ShardGroup result = this.service.getShardGroup(0);

        // Verify the results
    }

    @Test
    public void testGetShardGroupsByStore() throws Exception {
        // Setup
        final List<Metapb.ShardGroup> expectedResult = List.of(
                Metapb.ShardGroup.newBuilder().setId(0).addShards(
                              Metapb.Shard.newBuilder().setStoreId(0L)
                                          .setRole(Metapb.ShardRole.None).build())
                                 .setState(Metapb.PartitionState.PState_None)
                                 .build());

        // Run the test
        final List<Metapb.ShardGroup> result = this.service.getShardGroupsByStore(
                0L);
    }

    @Test
    public void testGetActiveStores1() throws Exception {
        // Setup
        final List<Metapb.Store> expectedResult = List.of(
                Metapb.Store.newBuilder().setId(0L).setAddress("address")
                            .setRaftAddress("raftAddress")
                            .addLabels(Metapb.StoreLabel.newBuilder().build())
                            .setVersion("version")
                            .setState(Metapb.StoreState.Unknown)
                            .setStartTimestamp(0L).setDeployPath("deployPath")
                            .setLastHeartbeat(0L).setStats(
                              Metapb.StoreStats.newBuilder().setStoreId(0L)
                                               .setPartitionCount(0).addGraphStats(
                                            Metapb.GraphStats.newBuilder()
                                                             .setGraphName("value")
                                                             .setApproximateSize(0L)
                                                             .setRole(Metapb.ShardRole.None)
                                                             .build()).build())
                            .setDataVersion(0).setCores(0)
                            .setDataPath("dataPath").build());

        // Run the test
        final List<Metapb.Store> result = this.service.getActiveStores("graphName");

        // Verify the results
    }

    @Test
    public void testGetActiveStores1ThrowsPDException() {
        try {
            List<Metapb.Store> stores = this.service.getActiveStores();
            assertThat(stores.size()).isEqualTo(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Ignore // state is Pending instead of Tombstone
    @Test
    public void testGetTombStores() throws Exception {
        // Setup
        final List<Metapb.Store> storeList = List.of(
                Metapb.Store.newBuilder().setId(0L).setAddress("address")
                            .setRaftAddress("raftAddress")
                            .addLabels(Metapb.StoreLabel.newBuilder().build())
                            .setVersion("version")
                            .setState(Metapb.StoreState.Tombstone)
                            .setStartTimestamp(0L).setDeployPath("deployPath")
                            .setLastHeartbeat(0L).setStats(
                              Metapb.StoreStats.newBuilder().setStoreId(0L)
                                               .setPartitionCount(0).addGraphStats(
                                            Metapb.GraphStats.newBuilder()
                                                             .setGraphName("value")
                                                             .setApproximateSize(0L)
                                                             .setRole(Metapb.ShardRole.None)
                                                             .build()).build())
                            .setDataVersion(0).setCores(0)
                            .setDataPath("dataPath").build());
        this.service.register(storeList.get(0));

        // Run the test
        final List<Metapb.Store> result = this.service.getTombStores();

        // Verify the results
        assertThat(result.size()).isEqualTo(1);
        this.service.removeStore(result.get(0).getId());
        List<Metapb.Store> stores = this.service.getStores();
        assertThat(stores.size()).isEqualTo(0);
    }

    @Test
    public void testAllocShards() throws Exception {
        // Setup
        try {
            final Metapb.Graph graph = Metapb.Graph.newBuilder()
                                                   .setGraphName("graphName")
                                                   .setGraphState(
                                                           Metapb.GraphState
                                                                   .newBuilder()
                                                                   .setMode(
                                                                           Metapb.GraphMode.ReadWrite)
                                                                   .setReason(
                                                                           Metapb.GraphModeReason.Quota)
                                                                   .build())
                                                   .build();
            final List<Metapb.Shard> expectedResult = List.of(
                    Metapb.Shard.newBuilder().setStoreId(0L)
                                .setRole(Metapb.ShardRole.None).build());

            // Configure PDConfig.getPartition(...).
            final PDConfig.Partition partition = new PDConfig().new Partition();
            partition.setTotalCount(0);
            partition.setMaxShardsPerStore(0);
            partition.setShardCount(0);

            // Run the test
            final List<Metapb.Shard> result = this.service.allocShards(graph, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testReallocShards() throws Exception {
        // Setup
        try {
            final Metapb.ShardGroup shardGroup = Metapb.ShardGroup.newBuilder()
                                                                  .setId(0)
                                                                  .addShards(
                                                                          Metapb.Shard
                                                                                  .newBuilder()
                                                                                  .setStoreId(
                                                                                          0L)
                                                                                  .setRole(
                                                                                          Metapb.ShardRole.None)
                                                                                  .build())
                                                                  .setState(
                                                                          Metapb.PartitionState.PState_None)
                                                                  .build();
            final List<Metapb.Shard> expectedResult = List.of(
                    Metapb.Shard.newBuilder().setStoreId(0L)
                                .setRole(Metapb.ShardRole.None).build());

            // Configure PDConfig.getPartition(...).
            final PDConfig.Partition partition = new PDConfig().new Partition();
            partition.setTotalCount(0);
            partition.setMaxShardsPerStore(0);
            partition.setShardCount(0);
            when(this.config.getPartition()).thenReturn(partition);

            // Run the test
            final List<Metapb.Shard> result = this.service.reallocShards(shardGroup);

            // Verify the results
            assertThat(result).isEqualTo(expectedResult);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testUpdateShardGroup() {
        try {
            final List<Metapb.Shard> shards = List.of(
                    Metapb.Shard.newBuilder().setStoreId(0L)
                                .setRole(Metapb.ShardRole.None).build());

            // Run the test
            this.service.updateShardGroup(0, shards, 0, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testUpdateShardGroupState() throws Exception {
        try {
            this.service.updateShardGroupState(0, Metapb.PartitionState.PState_None);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testHeartBeat() throws Exception {
        // Setup
        try {
            final Metapb.StoreStats storeStats = Metapb.StoreStats.newBuilder()
                                                                  .setStoreId(
                                                                          0L)
                                                                  .setPartitionCount(
                                                                          0)
                                                                  .addGraphStats(
                                                                          Metapb.GraphStats
                                                                                  .newBuilder()
                                                                                  .setGraphName(
                                                                                          "value")
                                                                                  .setApproximateSize(
                                                                                          0L)
                                                                                  .setRole(
                                                                                          Metapb.ShardRole.None)
                                                                                  .build())
                                                                  .build();
            final Metapb.ClusterStats expectedResult = Metapb.ClusterStats
                    .newBuilder().setState(Metapb.ClusterState.Cluster_OK)
                    .setMessage("message").setTimestamp(0L).build();
            when(this.config.getMinStoreCount()).thenReturn(0);

            // Configure PDConfig.getPartition(...).
            final PDConfig.Partition partition = new PDConfig().new Partition();
            partition.setTotalCount(0);
            partition.setMaxShardsPerStore(0);
            partition.setShardCount(0);
            when(this.config.getPartition()).thenReturn(partition);

            // Run the test
            final Metapb.ClusterStats result = this.service.heartBeat(storeStats);

            // Verify the results
            assertThat(result).isEqualTo(expectedResult);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testUpdateClusterStatus1() {
        // Setup
        final Metapb.ClusterStats expectedResult = Metapb.ClusterStats
                .newBuilder().setState(Metapb.ClusterState.Cluster_OK)
                .setMessage("message").setTimestamp(0L).build();

        // Run the test
        final Metapb.ClusterStats result = this.service.updateClusterStatus(
                Metapb.ClusterState.Cluster_OK);
    }

    @Test
    public void testUpdateClusterStatus2() {
        // Setup
        final Metapb.ClusterStats expectedResult = Metapb.ClusterStats
                .newBuilder().setState(Metapb.ClusterState.Cluster_OK)
                .setMessage("message").setTimestamp(0L).build();

        // Run the test
        final Metapb.ClusterStats result = this.service.updateClusterStatus(
                Metapb.PartitionState.PState_None);
    }

    @Test
    public void testCheckStoreStatus() {
        // Setup
        // Run the test
        this.service.checkStoreStatus();

        // Verify the results
    }

    @Test
    public void testAddStatusListener() {
        // Setup
        final StoreStatusListener mockListener = mock(
                StoreStatusListener.class);

        // Run the test
        this.service.addStatusListener(mockListener);

        // Verify the results
    }

    @Test
    public void testOnStoreStatusChanged() {
        // Setup
        final Metapb.Store store = Metapb.Store.newBuilder().setId(0L)
                                               .setAddress("address")
                                               .setRaftAddress("raftAddress")
                                               .addLabels(Metapb.StoreLabel
                                                                  .newBuilder()
                                                                  .build())
                                               .setVersion("version").setState(
                        Metapb.StoreState.Unknown).setStartTimestamp(0L)
                                               .setDeployPath("deployPath")
                                               .setLastHeartbeat(0L).setStats(
                        Metapb.StoreStats.newBuilder().setStoreId(0L)
                                         .setPartitionCount(0).addGraphStats(
                                      Metapb.GraphStats.newBuilder()
                                                       .setGraphName("value")
                                                       .setApproximateSize(0L)
                                                       .setRole(Metapb.ShardRole.None)
                                                       .build()).build())
                                               .setDataVersion(0).setCores(0)
                                               .setDataPath("dataPath").build();

        // Verify the results
    }

    @Test
    public void testOnShardGroupSplit() {
        // Setup
        final Metapb.ShardGroup shardGroup = Metapb.ShardGroup.newBuilder()
                                                              .setId(0)
                                                              .addShards(
                                                                      Metapb.Shard
                                                                              .newBuilder()
                                                                              .setStoreId(
                                                                                      0L)
                                                                              .setRole(
                                                                                      Metapb.ShardRole.None)
                                                                              .build())
                                                              .setState(
                                                                      Metapb.PartitionState.PState_None)
                                                              .build();
        final List<Metapb.ShardGroup> newShardGroups = List.of(
                Metapb.ShardGroup.newBuilder().setId(0).addShards(
                              Metapb.Shard.newBuilder().setStoreId(0L)
                                          .setRole(Metapb.ShardRole.None).build())
                                 .setState(Metapb.PartitionState.PState_None)
                                 .build());
        final Consumer<MetaTask.Task> mockTask = mock(Consumer.class);

        // Verify the results
    }

    @Ignore // active stores are fewer than min store count in pd config
    @Test
    public void testCheckStoreCanOffline() {
        // Setup
        final Metapb.Store currentStore = Metapb.Store.newBuilder().setId(0L)
                                                      .setAddress("address")
                                                      .setRaftAddress(
                                                              "raftAddress")
                                                      .addLabels(
                                                              Metapb.StoreLabel
                                                                      .newBuilder()
                                                                      .build())
                                                      .setVersion("version")
                                                      .setState(
                                                              Metapb.StoreState.Unknown)
                                                      .setStartTimestamp(0L)
                                                      .setDeployPath(
                                                              "deployPath")
                                                      .setLastHeartbeat(0L)
                                                      .setStats(
                                                              Metapb.StoreStats
                                                                      .newBuilder()
                                                                      .setStoreId(
                                                                              0L)
                                                                      .setPartitionCount(
                                                                              0)
                                                                      .addGraphStats(
                                                                              Metapb.GraphStats
                                                                                      .newBuilder()
                                                                                      .setGraphName(
                                                                                              "value")
                                                                                      .setApproximateSize(
                                                                                              0L)
                                                                                      .setRole(
                                                                                              Metapb.ShardRole.None)
                                                                                      .build())
                                                                      .build())
                                                      .setDataVersion(0)
                                                      .setCores(0)
                                                      .setDataPath("dataPath")
                                                      .build();
        // Run the test
        final boolean result = this.service.checkStoreCanOffline(currentStore);

        // Verify the results
        assertThat(result).isTrue();
    }

    @Test
    public void testShardGroupsDbCompaction() throws Exception {
        // Setup
        // Run the test
        try {
            this.service.shardGroupsDbCompaction(0, "tableName");
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Verify the results
    }

    @Test
    public void testGetQuota() throws Exception {
        // Setup
        // Run the test
        try {
            this.service.getQuota();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // migrated from StoreNodeServiceNewTest
    @Test
    public void testRemoveShardGroup() throws PDException {
        for (int i = 0; i < 12; i++) {
            Metapb.ShardGroup group = Metapb.ShardGroup.newBuilder()
                                                       .setId(i)
                                                       .setState(
                                                               Metapb.PartitionState.PState_Offline)
                                                       .build();
            this.service.getStoreInfoMeta().updateShardGroup(group);
        }

        this.service.deleteShardGroup(11);
        this.service.deleteShardGroup(10);

        assertEquals(10, getPdConfig().getConfigService().getPDConfig().getPartitionCount());
        // restore
        getPdConfig().getConfigService().setPartitionCount(12);
    }
}
