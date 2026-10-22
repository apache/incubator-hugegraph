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

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;

import org.apache.hugegraph.pd.ConfigService;
import org.apache.hugegraph.pd.KvService;
import org.apache.hugegraph.pd.PartitionService;
import org.apache.hugegraph.pd.StoreNodeService;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.meta.ConfigMetaStore;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class GraphSpaceUsageTest {

    private static void setField(Object target, String name, Object value) throws Exception {
        Class<?> type = target instanceof ConfigService ? ConfigService.class : StoreNodeService.class;
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    @Test
    public void testQuotaUpdatePreservesUsageButStatisticsCanResetToZero() throws Exception {
        ConfigService service = Mockito.mock(ConfigService.class, Mockito.CALLS_REAL_METHODS);
        ConfigMetaStore meta = Mockito.mock(ConfigMetaStore.class);
        setField(service, "meta", meta);
        Metapb.GraphSpace stored = Metapb.GraphSpace.newBuilder().setName("shop")
                                                .setStorageLimit(100).setUsedSize(512).build();
        Mockito.when(meta.getGraphSpace("shop")).thenReturn(List.of(stored));
        Mockito.when(meta.setGraphSpace(Mockito.any())).thenAnswer(call -> call.getArgument(0));
        Metapb.GraphSpace update = stored.toBuilder().setStorageLimit(200).setUsedSize(0).build();
        Assert.assertEquals(512, service.setGraphSpaceLimit(update).getUsedSize());
        Assert.assertEquals(200, service.setGraphSpaceLimit(update).getStorageLimit());
        Assert.assertEquals(0, service.setGraphSpace(update).getUsedSize());
    }

    @Test
    public void testUsageReturnsToZeroAfterLastGraphIsRemoved() throws Exception {
        StoreNodeService service = Mockito.mock(StoreNodeService.class, Mockito.CALLS_REAL_METHODS);
        PartitionService partitions = Mockito.mock(PartitionService.class);
        ConfigService config = Mockito.mock(ConfigService.class);
        setField(service, "partitionService", partitions);
        setField(service, "configService", config);
        setField(service, "kvService", Mockito.mock(KvService.class));
        Metapb.GraphSpace stored = Metapb.GraphSpace.newBuilder().setName("shop")
                                                .setStorageLimit(100).setUsedSize(512).build();
        Mockito.when(config.getGraphSpace("")).thenReturn(List.of(stored));
        Mockito.when(config.getGraphSpace("shop")).thenReturn(List.of(stored));
        Mockito.when(partitions.getGraphs()).thenReturn(Collections.emptyList());
        service.getQuota();
        Mockito.verify(config).setGraphSpace(stored.toBuilder().setUsedSize(0).build());
    }

    @Test
    public void testUsageAdvancesWithoutDefaultClusterConfigAndCountsOnlyLeaders() throws Exception {
        StoreNodeService service = Mockito.mock(StoreNodeService.class, Mockito.CALLS_REAL_METHODS);
        PartitionService partitions = Mockito.mock(PartitionService.class);
        ConfigService config = Mockito.mock(ConfigService.class);
        KvService kv = Mockito.mock(KvService.class);
        setField(service, "partitionService", partitions);
        setField(service, "configService", config);
        setField(service, "kvService", kv);
        Metapb.Graph graph = Metapb.Graph.newBuilder().setGraphName("shop/g/g").build();
        Mockito.when(partitions.getGraphs()).thenReturn(List.of(graph));
        Metapb.GraphStats leader = Metapb.GraphStats.newBuilder().setGraphName("shop/g/g")
                                                  .setRole(Metapb.ShardRole.Leader)
                                                  .setApproximateSize(512).build();
        Metapb.Store store = Metapb.Store.newBuilder().setStats(Metapb.StoreStats.newBuilder()
                .addGraphStats(leader).addGraphStats(leader.toBuilder().setRole(Metapb.ShardRole.Follower)))
                                          .build();
        Mockito.doReturn(List.of(store)).when(service).getStores("shop/g/g");
        Mockito.when(config.getGraphSpace("shop")).thenReturn(Collections.emptyList());
        service.getQuota();
        ArgumentCaptor<Metapb.GraphSpace> written = ArgumentCaptor.forClass(Metapb.GraphSpace.class);
        Mockito.verify(config).setGraphSpace(written.capture());
        Assert.assertEquals("shop", written.getValue().getName());
        Assert.assertEquals(512, written.getValue().getUsedSize());
    }
}
