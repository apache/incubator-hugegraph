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

package org.apache.hugegraph.store;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.pd.client.ClientCache;
import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.meta.Store;
import org.apache.hugegraph.store.meta.StoreMetadata;
import org.apache.hugegraph.store.options.HgStoreEngineOptions;
import org.apache.hugegraph.store.pd.PdProvider;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class StoreRegistrationTest {

    @Test
    public void testRestoreCompletesBeforeUpRegistration() throws Exception {
        List<String> events = new ArrayList<>();
        HgStoreEngine engine = Mockito.mock(HgStoreEngine.class);
        PdProvider provider = Mockito.mock(PdProvider.class);
        Mockito.when(provider.registerStore(Mockito.any())).thenAnswer(call -> {
            Store store = call.getArgument(0);
            events.add(store.getState().name());
            return 7L;
        });
        Mockito.doAnswer(call -> {
            events.add("restore");
            return null;
        }).when(engine).stateChanged(Mockito.any(), Mockito.any(), Mockito.eq(Metapb.StoreState.Up));
        HeartbeatService service = service(engine, provider);
        try {
            service.registerStore();
            Assert.assertEquals(List.of("Offline", "restore", "Up"), events);
        } finally {
            service.shutdown();
        }
    }

    @Test
    public void testFailedRestoreNeverRegistersUp() throws Exception {
        HgStoreEngine engine = Mockito.mock(HgStoreEngine.class);
        PdProvider provider = Mockito.mock(PdProvider.class);
        Mockito.when(provider.registerStore(Mockito.any())).thenReturn(7L);
        Mockito.doThrow(new IllegalStateException("restore failed")).when(engine)
               .stateChanged(Mockito.any(), Mockito.any(), Mockito.eq(Metapb.StoreState.Up));
        HeartbeatService service = service(engine, provider);
        try {
            service.registerStore();
            Mockito.verify(provider, Mockito.times(1)).registerStore(Mockito.any());
            Assert.assertEquals(Metapb.StoreState.Offline, service.getStoreInfo().getState());
        } finally {
            service.shutdown();
        }
    }

    @Test
    public void testShutdownReportsOfflineAndPreservesMetadata() throws Exception {
        PdProvider provider = Mockito.mock(PdProvider.class);
        PDClient client = Mockito.mock(PDClient.class);
        Mockito.when(provider.getPDClient()).thenReturn(client);
        Mockito.when(client.getCache()).thenReturn(Mockito.mock(ClientCache.class));
        Metapb.Store remote = Metapb.Store.newBuilder().setId(7).setState(Metapb.StoreState.Up)
                                       .setRaftAddress("127.0.0.1:8510")
                                       .addLabels(Metapb.StoreLabel.newBuilder()
                                                                 .setKey("zone").setValue("a")).build();
        Mockito.when(client.getStore(7)).thenReturn(remote);
        HeartbeatService service = service(null, provider);
        service.getStoreInfo().setId(7);
        service.shutdown();
        ArgumentCaptor<Metapb.Store> request = ArgumentCaptor.forClass(Metapb.Store.class);
        Mockito.verify(client).updateStore(request.capture());
        Assert.assertEquals(remote.toBuilder().setState(Metapb.StoreState.Offline).build(),
                            request.getValue());
        Assert.assertEquals(Metapb.StoreState.Offline, service.getStoreInfo().getState());
    }

    @Test
    public void testShutdownContinuesWhenPdIsUnavailable() throws Exception {
        PdProvider provider = Mockito.mock(PdProvider.class);
        PDClient client = Mockito.mock(PDClient.class);
        Mockito.when(provider.getPDClient()).thenReturn(client);
        Mockito.when(client.getCache()).thenReturn(Mockito.mock(ClientCache.class));
        Mockito.when(client.getStore(7)).thenThrow(new PDException(1, "unavailable"));
        HeartbeatService service = service(null, provider);
        service.getStoreInfo().setId(7);
        service.shutdown();
        Mockito.verify(client, Mockito.never()).updateStore(Mockito.any());
        Assert.assertEquals(Metapb.StoreState.Offline, service.getStoreInfo().getState());
    }

    private static HeartbeatService service(HgStoreEngine engine, PdProvider provider) throws Exception {
        HeartbeatService service = new HeartbeatService(engine);
        HgStoreEngineOptions options = new HgStoreEngineOptions();
        options.setGrpcAddress("127.0.0.1:8500");
        options.setRaftAddress("127.0.0.1:8510");
        Store store = new Store();
        store.setState(Metapb.StoreState.Unknown);
        set(service, "options", options);
        set(service, "storeInfo", store);
        set(service, "pdProvider", provider);
        service.setStoreMetadata(Mockito.mock(StoreMetadata.class));
        Mockito.when(provider.getClusterStats()).thenReturn(Metapb.ClusterStats.newBuilder()
                .setState(Metapb.ClusterState.Cluster_OK).build());
        return service;
    }

    private static void set(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
