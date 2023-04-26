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

package core;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hugegraph.rocksdb.access.RocksDBFactory;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.store.business.BusinessHandler;
import org.apache.hugegraph.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class HgBusinessImplTest extends StoreEngineTestBase {

    public BusinessHandler getBusinessHandler() {
        return getStoreEngine().getBusinessHandler();
    }

    @Test
    public void testPut() throws InterruptedException {

        String graph1 = "testPut-graph1";
        String graph2 = "testPut-graph2";

        createPartitionEngine(0, graph1);
        createPartitionEngine(0, graph2);

        String table = "testPut";
        BusinessHandler handler = getBusinessHandler();
        handler.doPut(graph1, 0, table, "key1".getBytes(), "value1".getBytes());
        handler.doPut(graph1, 0xF, table, "key2".getBytes(), "value1".getBytes());
        handler.doPut(graph1, 0xFF, table, "key3".getBytes(), "value1".getBytes());
        handler.doPut(graph1, 0xFFF, table, "key4".getBytes(), "value1".getBytes());

        handler.doPut(graph2, 0, table, "key21".getBytes(), "value1".getBytes());
        handler.doPut(graph2, 0xF, table, "key22".getBytes(), "value1".getBytes());
        handler.doPut(graph2, 0xFF, table, "key23".getBytes(), "value1".getBytes());
        handler.doPut(graph2, 0xFFF, table, "key24".getBytes(), "value1".getBytes());

        System.out.println("--------------------dump all -------");
        dump(handler, graph1, 0);
        System.out.println("--------------------dump scan 0 0xff -------");
        ScanIterator iterator = handler.scan(graph1, table, 0, 0xff);
        int count = 0;
        while (iterator.hasNext()) {
            RocksDBSession.BackendColumn entry = iterator.next();
            System.out.println(new String(entry.name) + " -- " + Bytes.toHex(entry.name));
            count++;
        }

        Assert.assertEquals(2, count);

        System.out.println("--------------------dump scan prefix -------");
        iterator = handler.scanPrefix(graph1, 0, table, "key".getBytes());

        count = 0;
        while (iterator.hasNext()) {
            RocksDBSession.BackendColumn entry = iterator.next();
            System.out.println(new String(entry.name) + " -- " + Bytes.toHex(entry.name));
            count++;
        }

        Assert.assertEquals(4, count);
        System.out.println("--------------------dump scan range key1 key4 -------");
        iterator = handler.scan(graph1, 0, table, "key1".getBytes(), "key4".getBytes(),
                                ScanIterator.Trait.SCAN_LT_END);
        count = 0;
        while (iterator.hasNext()) {
            RocksDBSession.BackendColumn entry = iterator.next();
            System.out.println(new String(entry.name) + " -- " + Bytes.toHex(entry.name));
            count++;
        }

        Assert.assertEquals(3, count);
        boolean cleanResult = handler.cleanPartition(graph1, 0);
        Assert.assertTrue(cleanResult);
        Thread.sleep(2000);

        handler.closeAll();
    }

    @Test
    public void testLoadSnapshot() throws InterruptedException {

        String graph1 = "LoadSnapshot-graph1";
        createPartitionEngine(0, graph1);
        BusinessHandler handler = getBusinessHandler();

        String table = "LoadSnapshot-table";
        for (int i = 0; i < 256; i++) {
            handler.doPut(graph1, i, table, ("key" + i).getBytes(), "value1".getBytes());
        }
        ScanIterator iterator = handler.scanAll(graph1, table);

        System.out.println(iterator.count());

        String snapshotPath;
        try (RocksDBSession session = handler.getSession(0)) {
            snapshotPath = session.getDbPath();
        }

        handler.closeAll();

        System.out.println("start loadSnapshot");
        handler.loadSnapshot(snapshotPath, graph1, 0, 10);
        iterator = handler.scanAll(graph1, table);
        Assert.assertEquals(255, iterator.count());
        try (RocksDBSession session = handler.getSession(0)) {
            System.out.println(session.getDbPath());
        }

        CountDownLatch latch = new CountDownLatch(1);
        RocksDBFactory.getInstance()
                      .addRocksdbChangedListener(new RocksDBFactory.RocksdbChangedListener() {
                          @Override
                          public void onCompacted(String dbName) {
                              RocksDBFactory.RocksdbChangedListener.super.onCompacted(dbName);
                          }

                          @Override
                          public void onDBDeleteBegin(String dbName, String filePath) {
                              RocksDBFactory.RocksdbChangedListener.super.onDBDeleteBegin(dbName,
                                                                                          filePath);
                          }

                          @Override
                          public void onDBDeleted(String dbName, String filePath) {
                              latch.countDown();
                          }

                          @Override
                          public void onDBSessionReleased(RocksDBSession dbSession) {
                              RocksDBFactory.RocksdbChangedListener.super.onDBSessionReleased(
                                      dbSession);
                          }
                      });

        latch.await(1, TimeUnit.SECONDS);

    }

    public void dump(BusinessHandler handler, String graph, int partId) {
        ScanIterator cfIterator = handler.scanRaw(graph, partId, 0);
        while (cfIterator.hasNext()) {
            try (ScanIterator iterator = cfIterator.next()) {
                byte[] cfName = cfIterator.position();
                System.out.println(graph + "-" + partId + "-" + new String(cfName) + "--------");
                while (iterator.hasNext()) {
                    RocksDBSession.BackendColumn col = iterator.next();
                    System.out.println(new String(col.name) + " -- " + Bytes.toHex(col.name));
                }
            }
        }
        cfIterator.close();
    }
}
