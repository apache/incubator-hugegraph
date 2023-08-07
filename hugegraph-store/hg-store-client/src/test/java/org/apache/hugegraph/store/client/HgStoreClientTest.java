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

package org.apache.hugegraph.store.client;

import static org.apache.hugegraph.store.client.util.HgStoreClientConst.ALL_PARTITION_OWNER;

import java.util.List;

import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.HgKvStore;
import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.HgStoreClient;
import org.apache.hugegraph.store.HgStoreSession;
import org.apache.hugegraph.store.util.HgStoreTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HgStoreClientTest {

    public final static byte[] EMPTY_BYTES = new byte[0];
    private static final String Graph_Name = "testGraphName";
    private static String Table_Name = "testTableName";
    private HgStoreClient storeClient;
    private PDClient pdClient;

    @Before
    public void init() {
        storeClient = HgStoreClient.create(PDConfig.of("127.0.0.1:8686")
                                                   .setEnableCache(true));
        pdClient = storeClient.getPdClient();
    }


    @Test
    public void testPutData() {
        HgStoreSession session = storeClient.openSession(Graph_Name);
        long start = System.currentTimeMillis();
        HgStoreTestUtil.batchPut(session, Table_Name, "testKey", 100000);

        System.out.println("Time is " + (System.currentTimeMillis() - start));
        try (HgKvIterator<HgKvEntry> iterator = session.scanIterator(Table_Name)) {
            Assert.assertEquals(100000, HgStoreTestUtil.amountOf(iterator));
        }
    }

    @Test
    public void testScan() throws PDException {

        HgStoreSession session = storeClient.openSession(Graph_Name);
        HgStoreTestUtil.batchPut(session, Table_Name, "testKey", 12);

        int count = 0;
        byte[] position = null;
        try (HgKvIterator<HgKvEntry> iterator = session.scanIterator(Table_Name)) {
            while (iterator.hasNext()) {
                iterator.next();
                position = iterator.position();
                dumpPosition(position);
                if (++count > 5) {
                    break;
                }
            }
        }


        System.out.println("--------------------------------");
        try (HgKvIterator<HgKvEntry> iterator = session.scanIterator(Table_Name)) {
            iterator.seek(position);
            while (iterator.hasNext()) {
                iterator.next();
                dumpPosition(iterator.position());
            }
        }

        System.out.println("--------------------------------");


        byte[] start = new byte[]{0x0};
        byte[] end = new byte[]{-1};
        try (HgKvIterator<HgKvEntry> iterator = session.scanIterator(Table_Name,
                                                                     HgOwnerKey.of(
                                                                             ALL_PARTITION_OWNER,
                                                                             start),
                                                                     HgOwnerKey.of(
                                                                             ALL_PARTITION_OWNER,
                                                                             end))) {
            iterator.seek(position);
            while (iterator.hasNext()) {
                iterator.next();
                dumpPosition(iterator.position());
            }
        }
    }

    public void dumpPosition(byte[] b) {
        byte[] buf = new byte[Long.BYTES];
        System.arraycopy(b, 0, buf, 0, Long.BYTES);
        long storeId = HgStoreTestUtil.toLong(buf);
        buf = new byte[Integer.BYTES];
        System.arraycopy(b, Long.BYTES, buf, 0, Integer.BYTES);
        int partId = HgStoreTestUtil.toInt(buf);
        String key = new String(b);

        System.out.println(" " + storeId + ", " + partId + ", " + key);
    }

    // @Test
    public void testDeleteData() {
        Table_Name = "deleteData5";
        HgStoreSession session = storeClient.openSession(Graph_Name);
        int ownerCode = 1;
        HgStoreTestUtil.batchPut(session, Table_Name, "T", 10, (key) -> {
                                     return HgStoreTestUtil.toOwnerKey(ownerCode, key);
                                 }
        );
        try (HgKvIterator<HgKvEntry> iterators = session.scanIterator(Table_Name)) {
//            while (iterators.hasNext()){
//                System.out.println(new String(iterators.next().key()));
//            }
            Assert.assertEquals(10, HgStoreTestUtil.amountOf(iterators));
        }
        session.beginTx();
        session.deletePrefix(Table_Name, HgStoreTestUtil.toOwnerKey(ownerCode, "T"));
        session.commit();

        System.out.println("=================================");
        try (HgKvIterator<HgKvEntry> iterators = session.scanIterator(Table_Name)) {
            Assert.assertEquals(0, HgStoreTestUtil.amountOf(iterators));
//            while (iterators.hasNext()){
//                System.out.println(new String(iterators.next().key()));
//            }
        }
    }

    // @Test
    public void testDropTable() throws PDException {
        HgStoreSession session = storeClient.openSession(Graph_Name);

        String table1 = "Table1";
        session.createTable(table1);
        HgStoreTestUtil.batchPut(session, table1, "testKey", 1000);

        try (HgKvIterator<HgKvEntry> iterators = session.scanIterator(table1)) {
            Assert.assertEquals(1000, HgStoreTestUtil.amountOf(iterators));
        }

        session.dropTable(table1);
        try (HgKvIterator<HgKvEntry> iterators = session.scanIterator(table1)) {
            Assert.assertEquals(0, HgStoreTestUtil.amountOf(iterators));
        }

        deleteGraph(Graph_Name);
    }


    public void deleteGraph(String graphName) throws PDException {
        HgStoreSession session = storeClient.openSession(graphName);
        session.deleteGraph(graphName);
        pdClient.delGraph(graphName);

        Metapb.Graph graph = null;
        try {
            graph = pdClient.getGraph(graphName);
        } catch (PDException e) {
            Assert.assertEquals(103, e.getErrorCode());
        }
        Assert.assertNull(graph);
    }

    // @Test
    public void testScanPartition() throws PDException {
        // testPutData();
        List<Metapb.Partition> partitions = pdClient.getPartitions(0, "DEFAULT/hugegraph/g");
        HgStoreSession session = storeClient.openSession("DEFAULT/hugegraph/g");
        for (Metapb.Partition partition : partitions) {
            try (HgKvIterator<HgKvEntry> iterators = session.scanIterator("g+v",
                                                                          (int) (partition.getStartKey()),
                                                                          (int) (partition.getEndKey()),
                                                                          HgKvStore.SCAN_HASHCODE,
                                                                          EMPTY_BYTES)) {

                System.out.println(
                        " " + partition.getId() + " " + HgStoreTestUtil.amountOf(iterators));
            }
        }
    }


}
