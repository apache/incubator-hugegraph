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

package org.apache.hugegraph.store.client;

import org.apache.hugegraph.store.HgStoreSession;
import org.apache.hugegraph.store.util.HgStoreTestUtil;
import org.junit.Assert;

/**
 * created on 2021/10/12
 */
public class HgStoreNodeSessionTest {

    private static final HgStoreNodeManager nodeManager = HgStoreNodeManager.getInstance();
    private static HgStoreNode node;

    // @BeforeClass
    public static void init() {
        node = nodeManager.addNode(HgStoreTestUtil.GRAPH_NAME,
                                   nodeManager.getNodeBuilder().setAddress("localhost:9080")
                                              .build());
    }

    private static HgStoreSession getStoreSession() {
        return node.openSession(HgStoreTestUtil.GRAPH_NAME);
    }

    private HgStoreNode getOneNode() {
        return node;
    }

    // @Test
    public void truncate() {
        HgStoreTestUtil.println("--- test truncate ---");
        String tableName = "UNIT_TRUNCATE_1";
        String keyName = "KEY_TRUNCATE";

        HgStoreSession session = getStoreSession();
        HgStoreTestUtil.batchPut(session, tableName, keyName, 100);
        Assert.assertEquals(100, HgStoreTestUtil.amountOf(session.scanIterator(tableName)));

        String tableName2 = "UNIT_TRUNCATE_2";
        HgStoreTestUtil.batchPut(session, tableName2, keyName, 100);
        Assert.assertEquals(100, HgStoreTestUtil.amountOf(session.scanIterator(tableName2)));

        session.truncate();
        Assert.assertEquals(0, HgStoreTestUtil.amountOf(session.scanIterator(tableName)));
        Assert.assertEquals(0, HgStoreTestUtil.amountOf(session.scanIterator(tableName2)));
    }

}
