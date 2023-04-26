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

package core.store.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.meta.MetadataKeyHelper;
import org.apache.hugegraph.store.util.PartitionMetaStoreWrapper;
import org.junit.Before;
import org.junit.Test;

import core.StoreEngineTestBase;

public class PartitionMetaStoreWrapperTest extends StoreEngineTestBase {

    private PartitionMetaStoreWrapper wrapper;

    private Metapb.Partition partition;

    public static void putToDb(Metapb.Partition partition, PartitionMetaStoreWrapper wrapper) {
        byte[] key = MetadataKeyHelper.getPartitionKey(partition.getGraphName(), partition.getId());
        wrapper.put(partition.getId(), key, partition.toByteArray());
    }

    @Before
    public void setup() {
        wrapper = new PartitionMetaStoreWrapper();
        partition = Metapb.Partition.newBuilder()
                                    .setId(1)
                                    .setGraphName("graph0")
                                    .setStartKey(0L)
                                    .setEndKey(65535L)
                                    .build();
    }

    @Test
    public void testGet() {
        putToDb(partition, wrapper);
        byte[] key = MetadataKeyHelper.getPartitionKey(partition.getGraphName(), partition.getId());
        assertEquals(partition, wrapper.get(1, key, Metapb.Partition.parser()));
        byte[] key2 = MetadataKeyHelper.getPartitionKey("not_exists", partition.getId());
        assertNull(wrapper.get(1, key2, Metapb.Partition.parser()));
    }

    @Test
    public void testPut() {
        putToDb(partition, wrapper);
        byte[] key = MetadataKeyHelper.getPartitionKey(partition.getGraphName(), partition.getId());
        var list = wrapper.scan(partition.getId(), Metapb.Partition.parser(), key);
        assertEquals(list.size(), 1);
        assertEquals(list.get(0).getGraphName(), partition.getGraphName());
    }

    @Test
    public void testDelete() {
        putToDb(partition, wrapper);
        byte[] key = MetadataKeyHelper.getPartitionKey(partition.getGraphName(), partition.getId());
        wrapper.delete(partition.getId(), key);
        var list = wrapper.scan(partition.getId(), Metapb.Partition.parser(), key);
        assertEquals(list.size(), 0);
    }

    @Test
    public void testScan() {
        putToDb(partition, wrapper);
        byte[] key = MetadataKeyHelper.getPartitionKey(partition.getGraphName(), partition.getId());
        var list = wrapper.scan(partition.getId(), Metapb.Partition.parser(), key);
        assertEquals(list.size(), 1);
    }

}
