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

package org.apache.hugegraph.store.meta;

import java.io.File;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.store.UnitTestBase;
import org.apache.hugegraph.store.meta.base.DBSessionBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GraphIDManagerTest extends UnitTestBase {
    @Before
    public void init() {
        String dbPath = "/tmp/junit";
        deleteDir(new File(dbPath));
        super.initDB(dbPath);
    }

    @Test
    public void test() throws PDException {
        GraphIdManager.maxGraphID = 64;
        int max = GraphIdManager.maxGraphID;
        try (RocksDBSession session = getDBSession("test")) {
            GraphIdManager gid = new GraphIdManager(new DBSessionBuilder() {
                @Override
                public RocksDBSession getSession(int partId) {
                    return session.clone();
                }
            }, 0);
            for (int i = 0; i < max; i++) {
                Assert.assertEquals(i, gid.getCId("Test", max));
            }

            Assert.assertEquals(-1, gid.getCId("Test", max));

            gid.delCId("Test", 3);
            Assert.assertEquals(3, gid.getCId("Test", max));
            Assert.assertEquals(-1, gid.getCId("Test", max));

            long start = System.currentTimeMillis();
            for (int i = 0; i < GraphIdManager.maxGraphID; i++) {
                long id = gid.getGraphId("g" + i);
                Assert.assertEquals(i, id);
            }
            System.out.println("time is " + (System.currentTimeMillis() - start));
            {
                gid.releaseGraphId("g" + 10);
                long id = gid.getGraphId("g" + 10);
                Assert.assertEquals(10, id);
            }
            start = System.currentTimeMillis();
            for (int i = 0; i < GraphIdManager.maxGraphID; i++) {
                long id = gid.releaseGraphId("g" + i);
                Assert.assertEquals(i, id);
            }
            System.out.println("time is " + (System.currentTimeMillis() - start));
            start = System.currentTimeMillis();
            for (int i = 0; i < GraphIdManager.maxGraphID; i++) {
                long id = gid.getCId(GraphIdManager.GRAPH_ID_PREFIX, GraphIdManager.maxGraphID);
                //  long id = gid.getGraphId("g" + i);
                Assert.assertTrue(id >= 0);
            }
            System.out.println("time is " + (System.currentTimeMillis() - start));
        }
    }
}
