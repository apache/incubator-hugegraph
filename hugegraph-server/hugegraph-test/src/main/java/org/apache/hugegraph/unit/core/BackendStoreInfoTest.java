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

package org.apache.hugegraph.unit.core;

import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.store.BackendStoreInfo;
import org.apache.hugegraph.backend.store.BackendStoreProvider;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class BackendStoreInfoTest {

    @Test
    public void testBackendStoreInfo() {
        BackendStoreProvider provider = Mockito.mock(BackendStoreProvider.class);
        BackendStore store = Mockito.mock(BackendStore.class);
        HugeConfig config = Mockito.mock(HugeConfig.class);

        Mockito.when(provider.initialized()).thenReturn(true);
        Mockito.when(provider.loadSystemStore(config)).thenReturn(store);
        Mockito.when(store.storedVersion()).thenReturn("1.11");

        BackendStoreInfo backendStoreInfo = new BackendStoreInfo(config,
                                                                 provider);
        Assert.assertTrue(backendStoreInfo.exists());

        Mockito.when(provider.driverVersion()).thenReturn("1.10");
        Assert.assertFalse(backendStoreInfo.checkVersion());
    }
}
