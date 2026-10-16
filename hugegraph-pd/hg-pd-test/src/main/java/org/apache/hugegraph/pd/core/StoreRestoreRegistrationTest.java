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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.pd.StoreNodeService;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.meta.StoreInfoMeta;
import org.apache.hugegraph.testutil.Whitebox;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class StoreRestoreRegistrationTest {

    private StoreNodeService service;
    private StoreInfoMeta metadata;
    private AtomicReference<Metapb.Store> current;

    @Before
    public void setUp() throws Exception {
        service = Mockito.mock(StoreNodeService.class, Mockito.CALLS_REAL_METHODS);
        metadata = Mockito.mock(StoreInfoMeta.class);
        current = new AtomicReference<>(store(Metapb.StoreState.Up));
        PDConfig config = Mockito.mock(PDConfig.class);
        Mockito.when(config.getInitialStoreMap()).thenReturn(Map.of());
        Whitebox.setInternalState(service, "pdConfig", config);
        Whitebox.setInternalState(service, "storeInfoMeta", metadata);
        Whitebox.setInternalState(service, "statusListeners", List.of());
        Mockito.doNothing().when(service).checkStoreStatus();
        Mockito.when(metadata.storeExists(7L)).thenReturn(true);
        Mockito.when(metadata.getStore(7L)).thenAnswer(call -> current.get());
        Mockito.doAnswer(call -> {
            current.set(call.getArgument(0));
            return null;
        }).when(metadata).updateStore(Mockito.any());
    }

    @Test
    public void testRestoringStoreRemainsOfflineUntilSecondRegistration() throws Exception {
        Metapb.Store restoring = service.register(store(Metapb.StoreState.Offline));
        Assert.assertEquals(Metapb.StoreState.Offline, restoring.getState());
        Mockito.verify(metadata).removeActiveStore(restoring);
        Mockito.verify(metadata, Mockito.never()).keepStoreAlive(Mockito.any());

        Metapb.Store restored = service.register(store(Metapb.StoreState.Up));
        Assert.assertEquals(Metapb.StoreState.Up, restored.getState());
        Mockito.verify(metadata).keepStoreAlive(restored);
    }

    @Test
    public void testPendingStoreStillRequiresActivation() throws Exception {
        current.set(store(Metapb.StoreState.Pending));
        Assert.assertEquals(Metapb.StoreState.Pending,
                            service.register(store(Metapb.StoreState.Offline)).getState());
        Assert.assertEquals(Metapb.StoreState.Pending,
                            service.register(store(Metapb.StoreState.Up)).getState());
        Mockito.verify(metadata, Mockito.never()).keepStoreAlive(Mockito.any());
    }

    @Test
    public void testLegacyUpRegistrationStillActivatesOfflineStore() throws Exception {
        current.set(store(Metapb.StoreState.Offline));
        Assert.assertEquals(Metapb.StoreState.Up,
                            service.register(store(Metapb.StoreState.Up)).getState());
    }

    private static Metapb.Store store(Metapb.StoreState state) {
        return Metapb.Store.newBuilder().setId(7).setAddress("127.0.0.1:8500")
                           .setRaftAddress("127.0.0.1:8510").setState(state).build();
    }
}
