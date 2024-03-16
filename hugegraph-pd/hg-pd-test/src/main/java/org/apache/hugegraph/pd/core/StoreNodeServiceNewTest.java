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

import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hugegraph.pd.StoreNodeService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.junit.Before;
import org.junit.Test;

public class StoreNodeServiceNewTest extends PDCoreTestBase {
    private StoreNodeService service;

    @Before
    public void init() {
        this.service = getStoreNodeService();
    }

    @Test
    public void testGetTaskInfoMeta() {
        assertNotNull(this.service.getTaskInfoMeta());
    }

    public void testGetStoreInfoMeta() {
        assertNotNull(this.service.getStoreInfoMeta());
    }

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
