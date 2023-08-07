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

package core.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.hugegraph.store.PartitionEngine;
import org.junit.Before;
import org.junit.Test;

import core.StoreEngineTestBase;

public class PartitionEngineTest extends StoreEngineTestBase {

    PartitionEngine engine;

    @Before
    public void setup() {
        engine = createPartitionEngine(0);
    }

    @Test
    public void testHasPartition() {
        assertTrue(engine.hasPartition("graph0"));
    }

    @Test
    public void testGetGroupId() {
        assertEquals(engine.getGroupId().intValue(), 0);
    }

    @Test
    public void testGetShardGroup() {
        assertEquals(engine.getShardGroup().getShards().size(), 1);
    }

    @Test
    public void testIsLeader() {
        System.out.println(engine.isLeader());
    }

    @Test
    public void testGetLeader() {
        assertEquals(engine.getLeader().toString(), engine.getOptions().getRaftAddress());
    }

    @Test
    public void testGetAlivePeers() {
        try {
            System.out.println(engine.getAlivePeers().size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetRaftNode() {
        assertNotNull(engine.getRaftNode());
    }

    @Test
    public void testGetPartitions() {
        assertEquals(engine.getPartitions().get("graph0").getId(), 0);
    }

    @Test
    public void testGetPartition() {
        assertEquals(engine.getPartition("graph0").getId(), 0);
    }

    @Test
    public void testGetCommittedIndex() throws InterruptedException {
        Thread.sleep(1000);
        System.out.println(engine.getCommittedIndex());
    }

}
