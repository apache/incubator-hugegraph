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

package client;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.HgStoreSession;
import org.junit.Assert;
import org.junit.Test;

import util.HgStoreTestUtil;

/**
 * 测试修改副本数
 */
public class ChangeShardNumTest extends BaseClientTest {
    @Test
    public void test3To1() throws PDException {
        int number = 10000;
        HgStoreSession session = storeClient.openSession(graphName);
        HgStoreTestUtil.batchPut(session, tableName, "testKey", number);

        try (HgKvIterator<HgKvEntry> iterators = session.scanIterator(tableName)) {
//            Assert.assertEquals(number, HgStoreTestUtil.amountOf(iterators));
        }

        Metapb.PDConfig pdConfig = pdClient.getPDConfig();
        pdConfig = pdConfig.toBuilder().setShardCount(1).build();

        pdClient.setPDConfig(pdConfig);
        pdClient.balancePartition();
    }

    //    @Test
    public void test1To3() throws PDException {
        int number = 10000;
        HgStoreSession session = storeClient.openSession(graphName);
        HgStoreTestUtil.batchPut(session, tableName, "testKey", number);

        try (HgKvIterator<HgKvEntry> iterators = session.scanIterator(tableName)) {
            Assert.assertEquals(number, HgStoreTestUtil.amountOf(iterators));
        }

        Metapb.PDConfig pdConfig = pdClient.getPDConfig();
        pdConfig = pdConfig.toBuilder().setShardCount(3).build();

        pdClient.setPDConfig(pdConfig);
        pdClient.balancePartition();
    }
}
