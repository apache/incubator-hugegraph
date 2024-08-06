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

import static org.apache.hugegraph.store.client.util.HgStoreClientConst.EMPTY_BYTES;
import static org.apache.hugegraph.store.util.HgStoreTestUtil.batchPut;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.HgKvStore;
import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.HgScanQuery;
import org.apache.hugegraph.store.HgStoreSession;
import org.apache.hugegraph.store.client.grpc.KvCloseableIterator;
import org.apache.hugegraph.store.client.util.HgStoreClientConfig;
import org.apache.hugegraph.store.client.util.HgStoreClientConst;
import org.apache.hugegraph.store.client.util.MetricX;
import org.apache.hugegraph.store.grpc.common.ScanOrderType;
import org.apache.hugegraph.store.util.HgStoreTestUtil;
import org.junit.Assert;
import org.junit.Test;

public class HgSessionManagerRaftPDTest extends HgStoreClientBase {

    public static final String GRAPH_NAME_X = "default/hugegraph/x";
    public static final String GRAPH_NAME_Y = "default/hugegraph/y";
    public static final String GRAPH_NAME_Z = "default/hugegraph/z";
    public static final String TABLE_NAME_1 = "table1";
    public static final String TABLE_NAME_2 = "table2";
    public static final String TABLE_NAME_3 = "table3";

    private HgStoreSession getStoreSession() {
        return storeClient.openSession(HgStoreTestUtil.GRAPH_NAME);
    }

    private HgStoreSession getStoreSession(String graph) {
        return storeClient.openSession(graph);
    }

    @Test
    public void putGet() {
        System.out.println("--- test put & get ---");

        HgStoreSession session = getStoreSession(HgStoreTestUtil.GRAPH_NAME);

        HgOwnerKey key = HgStoreTestUtil.toOwnerKey("FOR-PUT-KEY");
        byte[] value = HgStoreTestUtil.toBytes("FOR-PUT-VALUE");

        long stime = System.currentTimeMillis();
        batchPut(session, TABLE_NAME, "BATCH-PUT-TEST", 30000);
        System.out.println("Time is " + (System.currentTimeMillis() - stime));
    }

    public void putGet2() {
        System.out.println("--- test put & get ---");

        HgStoreSession session = getStoreSession("testGraph");
        HgOwnerKey key = HgStoreTestUtil.toOwnerKey("FOR-PUT-KEY");
        byte[] value = HgStoreTestUtil.toBytes("FOR-PUT-VALUE");
        long stime = System.currentTimeMillis();
        HgStoreTestUtil.batchPut(session, "testTable", "BATCH-PUT-TEST", 30000);
        System.out.println("Time is " + (System.currentTimeMillis() - stime));
    }

    @Test
    public void scanPartition() {
        HgStoreTestUtil.println("--- test scanPartition ---");

        HgStoreSession session = getStoreSession();

        Iterator iterator =
                session.scanIterator(TABLE_NAME, 0, 65535, HgKvStore.SCAN_HASHCODE, EMPTY_BYTES);
        System.out.println(HgStoreTestUtil.amountOf(iterator));
    }

    @Test
    public void check() {
        System.out.println("--- test check ---");

        HgStoreSession session = getStoreSession();
        HgKvIterator<HgKvEntry> iterator = session.scanIterator(TABLE_NAME);
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        System.out.println(count);
    }

    @Test
    public void putGetUnique() {
        System.out.println("--- test put & get ---");

        HgStoreSession session = getStoreSession();

        // add timestamp into key to avoid key duplication
        Date date = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("HHmmss");
        String timestamp = formatter.format(date);

        HgOwnerKey key = HgStoreTestUtil.toOwnerKey("FOR-PUT-KEY-" + timestamp);
        byte[] value = HgStoreTestUtil.toBytes("FOR-PUT-VALUE");

        Assert.assertTrue(session.put(TABLE_NAME, key, value));

        Assert.assertEquals(
                HgStoreTestUtil.toStr(value), HgStoreTestUtil.toStr(session.get(TABLE_NAME, key)));
    }

    @Test
    public void testBatchPutExt() throws IOException {
        System.out.println("--- test batchPut ---");
        HgStoreSession session = getStoreSession();
        session.truncate();
        String keyPrefix = "BATCH-GET-UNIT";

        Map<HgOwnerKey, byte[]> map = HgStoreTestUtil.batchPut(session, keyPrefix, 10);

        File outputFile = new File("tmp/batch_put_list");
        if (outputFile.exists()) {
            FileUtils.forceDelete(outputFile);
        }
        FileUtils.forceMkdir(new File("tmp/"));

        ObjectOutputStream oo = new ObjectOutputStream(new FileOutputStream(outputFile));
        oo.writeObject(map);
        oo.close();
        System.out.printf("%d entries have be put into graph %s\n", map.size(),
                          HgStoreTestUtil.GRAPH_NAME);

        int count = 0;
        HgKvIterator<HgKvEntry> iterator = null;
        iterator = session.scanIterator(TABLE_NAME);
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        Assert.assertEquals(10, count);
    }

    //     @Test
    public void testBatchGetExt() throws IOException, ClassNotFoundException {
        File outputFile = new File("tmp/batch_put_list");
        ObjectInputStream ois = new ObjectInputStream(new FileInputStream(outputFile));
        Map<HgOwnerKey, byte[]> map = (Map<HgOwnerKey, byte[]>) ois.readObject();
        ois.close();
        System.out.printf("%d entries get from %s\n", map.size(), outputFile.getPath());

        HgStoreSession session = getStoreSession();
        List<HgOwnerKey> keyList =
                map.entrySet().stream().map(e -> e.getKey()).collect(Collectors.toList());
        List<HgKvEntry> resList = session.batchGetOwner(TABLE_NAME, keyList);

        Assert.assertTrue(
                (resList.stream().map(e -> map.containsKey(HgStoreTestUtil.toOwnerKey(e.key())))
                        .allMatch(Boolean::booleanValue)));
    }

    @Test
    public void testBatchPutUniqueExt() throws IOException {
        System.out.println("--- test batchPut ---");
        HgStoreSession session = getStoreSession();

        // add timestamp into key to avoid key duplication
        Date date = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("HHmmss");
        String timestamp = formatter.format(date);

        String keyPrefix = "BATCH-GET-UNIT-" + timestamp;

        Map<HgOwnerKey, byte[]> map = HgStoreTestUtil.batchPut(session, keyPrefix, 30);

        File outputFile = new File("tmp/batch_put_list");
        if (outputFile.exists()) {
            FileUtils.forceDelete(outputFile);
        }
        FileUtils.forceMkdir(new File("tmp/"));

        ObjectOutputStream oo = new ObjectOutputStream(new FileOutputStream(outputFile));
        oo.writeObject(map);
        oo.close();
        System.out.printf("%d entries have be put into graph %s\n", map.size(),
                          HgStoreTestUtil.GRAPH_NAME);
    }

    @Test
    public void testBatchPutMultiGraph() throws IOException {
        System.out.println("--- test testBatchPutMultiGraph ---");
        Date date = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("HHmmss");
        String timestamp = formatter.format(date);
        HgStoreSession session1 = getStoreSession(GRAPH_NAME_X);
        HgStoreSession session2 = getStoreSession(GRAPH_NAME_Y);
        HgStoreSession session3 = getStoreSession(GRAPH_NAME_Z);
        String keyPrefix1 = "BATCH-PUT-UNIT-X-" + timestamp;
        String keyPrefix2 = "BATCH-PUT-UNIT-Y-" + timestamp;
        String keyPrefix3 = "BATCH-PUT-UNIT-Z-" + timestamp;
        HgStoreTestUtil.batchPut(session1, TABLE_NAME_1, keyPrefix1, 1000);
        HgStoreTestUtil.batchPut(session1, TABLE_NAME_2, keyPrefix1, 1000);
        HgStoreTestUtil.batchPut(session1, TABLE_NAME_3, keyPrefix1, 1000);
        HgStoreTestUtil.batchPut(session2, TABLE_NAME_1, keyPrefix2, 1000);
        HgStoreTestUtil.batchPut(session2, TABLE_NAME_2, keyPrefix2, 1000);
        HgStoreTestUtil.batchPut(session2, TABLE_NAME_3, keyPrefix2, 1000);
        HgStoreTestUtil.batchPut(session3, TABLE_NAME_1, keyPrefix3, 1000);
        HgStoreTestUtil.batchPut(session3, TABLE_NAME_2, keyPrefix3, 1000);
        HgStoreTestUtil.batchPut(session3, TABLE_NAME_3, keyPrefix3, 1000);
    }

    //     @Test
    // CAUTION: ONLY FOR LONG!
    // Note: Currently, only long type values can be merged.
    public void merge() {
        System.out.println("--- test merge (1+1=2)---");
        HgStoreSession session = getStoreSession();
        String mergeKey = "merge-key";
        HgOwnerKey key = HgStoreTestUtil.toOwnerKey(mergeKey);
        byte[] value = HgStoreTestUtil.toBytes(1L);

        System.out.println("- put " + mergeKey + ":1 -");
        session.put(TABLE_NAME, key, value);
        System.out.println("- merge " + mergeKey + ":1 -");
        session.merge(TABLE_NAME, key, value);
        long res = HgStoreTestUtil.toLong(session.get(TABLE_NAME, key));
        System.out.printf("after merge " + mergeKey + "=%s%n", res);
        Assert.assertEquals(2L, res);

        String putStr = "19";
        session.put(TABLE_NAME, key, HgStoreTestUtil.toBytes(putStr));
        byte[] b1 = session.get(TABLE_NAME, key);
        Assert.assertEquals(putStr, HgStoreTestUtil.toStr(b1));
    }

    @Test
    public void delete() {
        System.out.println("--- test delete ---");
        HgStoreSession session = getStoreSession();

        String delKey = "del-key";
        String delValue = "del-value";
        HgOwnerKey key = HgStoreTestUtil.toOwnerKey(delKey);
        byte[] value = HgStoreTestUtil.toBytes(delValue);

        HgStoreTestUtil.println("- put " + delKey + ": " + delValue);
        session.put(TABLE_NAME, key, value);
        HgStoreTestUtil.println("- delete " + delKey);
        session.delete(TABLE_NAME, key);
        value = session.get(TABLE_NAME, key);
        HgStoreTestUtil.println("- get " + delKey + ": " + HgStoreTestUtil.toStr(value));
        Assert.assertEquals(EMPTY_BYTES, value);
    }

    @Test
    public void deleteSingle() {
        System.out.println("--- test deleteSingle ---");
        HgStoreSession session = getStoreSession();

        String delKey = "del-single-key";
        String delValue = "del-single-value";
        HgOwnerKey key = HgStoreTestUtil.toOwnerKey(delKey);
        byte[] value = HgStoreTestUtil.toBytes(delValue);

        session.put(TABLE_NAME, key, value);
        value = session.get(TABLE_NAME, key);
        Assert.assertEquals(delValue, HgStoreTestUtil.toStr(value));

        HgStoreTestUtil.println("- delete-single : [" + delKey + "]");
        session.deleteSingle(TABLE_NAME, key);
        value = session.get(TABLE_NAME, key);
        HgStoreTestUtil.println(
                "- after del, get [" + delKey + "] = " + HgStoreTestUtil.toStr(value));
        Assert.assertEquals("", HgStoreTestUtil.toStr(value));
    }

    @Test
    public void deleteRange() {
        HgStoreTestUtil.println("--- test deleteRange ---");
        HgStoreSession session = getStoreSession();

        String rangePrefix = "DEL-RANGE-KEY";
        String owner = "batch-delete-owner";
        Map<HgOwnerKey, byte[]> map =
                HgStoreTestUtil.batchPut(
                        session,
                        TABLE_NAME,
                        rangePrefix,
                        10,
                        key -> {
                            return HgStoreTestUtil.toOwnerKey(owner, key);
                        });

        HgOwnerKey startKey = HgStoreTestUtil.toOwnerKey(owner, rangePrefix + "-00");
        HgOwnerKey endKey = HgStoreTestUtil.toOwnerKey(owner, rangePrefix + "-05");

        Assert.assertTrue(session.deleteRange(TABLE_NAME, startKey, endKey));

        HgStoreTestUtil.println(
                "- after delete range from [" + HgStoreTestUtil.toStr(startKey.getKey()) +
                "] to [" +
                HgStoreTestUtil.toStr(endKey.getKey()) + "]");

        for (int i = 0; i < 10; i++) {
            HgOwnerKey key =
                    HgStoreTestUtil.toOwnerKey(owner, rangePrefix + "-" +
                                                      HgStoreTestUtil.padLeftZeros(
                                                              String.valueOf(i), 2));
            String value = HgStoreTestUtil.toStr(session.get(TABLE_NAME, key));

            // TODO: [start,end)?
            if (i < 5) {
                Assert.assertEquals("", value);
            } else {
                // println(value);
                Assert.assertNotEquals("", value);
            }
        }
    }

    @Test
    public void deletePrefix() {
        System.out.println("--- test deletePrefix ---");
        HgStoreSession session = getStoreSession();

        String prefixStr = "DEL-PREFIX-KEY";
        String owner = "batch-delete-owner";
        Map<HgOwnerKey, byte[]> map =
                HgStoreTestUtil.batchPut(
                        session,
                        TABLE_NAME,
                        prefixStr,
                        10,
                        key -> {
                            return HgStoreTestUtil.toOwnerKey(owner, key);
                        });

        // printOwner(map.entrySet().stream().map(e -> e.getKey()).collect(Collectors.toList()));

        HgOwnerKey prefix = HgStoreTestUtil.toOwnerKey(owner, prefixStr);

        Assert.assertEquals(10, HgStoreTestUtil.amountOf(session.scanIterator(TABLE_NAME, prefix)));
        session.deletePrefix(TABLE_NAME, prefix);
        Assert.assertEquals(0, HgStoreTestUtil.amountOf(session.scanIterator(TABLE_NAME, prefix)));

        HgStoreTestUtil.println("- after delete by prefix:[" + prefixStr + "]");

        for (int i = 0; i < 10; i++) {
            HgOwnerKey key =
                    HgStoreTestUtil.toOwnerKey(owner, prefixStr + HgStoreTestUtil.toSuffix(i, 2));
            String value = HgStoreTestUtil.toStr(session.get(TABLE_NAME, key));
            Assert.assertEquals("", value);
        }
    }

    // @Test
    // TODO: this test's result is unstable
    public void scanIterator() {
        HgStoreTestUtil.println("--- test scanIterator ---");
        String tableName = TABLE_NAME;
        String keyName = "SCAN-ITER";
        HgStoreSession session = getStoreSession();
        HgStoreTestUtil.batchPut(session, tableName, keyName, 10000);
        int count = 0;
        int limit = 0;
        int max = 99999;
        HgKvIterator<HgKvEntry> iterator = null;

        HgStoreTestUtil.println("-- test 0 element --");
        iterator = session.scanIterator(tableName, HgStoreTestUtil.toAllPartitionKey("__SCAN-001"),
                                        HgStoreTestUtil.toAllPartitionKey("__SCAN-100"), 0);
        Assert.assertFalse(iterator.hasNext());
        try {
            iterator.next();
            Assert.fail();
        } catch (Throwable t) {
            HgStoreTestUtil.println("-- test NoSuchElementException --");
            Assert.assertTrue(t instanceof NoSuchElementException);
        }

        for (int i = 1; i <= 10; i++) {
            limit = i;
            iterator =
                    session.scanIterator(
                            tableName, HgStoreTestUtil.toAllPartitionKey(keyName + "-0"),
                            HgStoreTestUtil.toAllPartitionKey(keyName + "-1"), limit);
            count = 0;
            while (iterator.hasNext()) {
                count++;
                HgKvEntry entry = iterator.next();
            }
            Assert.assertEquals(limit, count);
        }

        for (int i = 1; i <= 10; i++) {
            limit = i;
            iterator =
                    session.scanIterator(
                            tableName, HgStoreTestUtil.toAllPartitionKey(keyName + "-00001"),
                            HgStoreTestUtil.toAllPartitionKey(keyName + "-00005"), limit);
            count = 0;
            while (iterator.hasNext()) {
                count++;
                HgKvEntry entry = iterator.next();
            }
            if (i <= 5) {
                Assert.assertEquals(limit, count);
            } else {
                Assert.assertEquals(5, count);
            }
        }

        limit = 0;
        iterator =
                session.scanIterator(
                        tableName, HgStoreTestUtil.toAllPartitionKey(keyName + "-0"),
                        HgStoreTestUtil.toAllPartitionKey(keyName + "-1"), limit);

        count = 0;
        while (iterator.hasNext()) {
            count++;
            HgKvEntry entry = iterator.next();
            if (count >= max) {
                break;
            }
        }
        Assert.assertEquals(10000, count);

        iterator = session.scanIterator(tableName);
        count = 0;
        while (iterator.hasNext()) {
            count++;
            HgKvEntry ignore = iterator.next();
            if (count >= max) {
                break;
            }
        }
        Assert.assertEquals(10000, count);

        iterator =
                session.scanIterator(tableName, HgStoreTestUtil.toAllPartitionKey(keyName + "-01"));
        count = 0;
        while (iterator.hasNext()) {
            count++;
            HgKvEntry entry = iterator.next();
            if (count >= max) {
                break;
            }
        }
        Assert.assertEquals(1000, count);
    }

    @Test
    public void paging() {
        HgStoreTestUtil.println("--- test scanIterator_range ---");
        String graph = "UNIT/paging";
        String tableName = TABLE_NAME;
        String keyName = "SCAN-PAGING";
        int keyAmt = 100;
        HgStoreSession session = getStoreSession(graph);
        HgStoreTestUtil.batchPut(session, tableName, keyName, keyAmt);
        int count = 0;
        int limit = 0;
        int max = 99999;
        HgKvIterator<HgKvEntry> iterator = null;

        count = 0;

        iterator =
                session.scanIterator(
                        tableName,
                        HgStoreTestUtil.toAllPartitionKey(keyName + "-000"),
                        HgStoreClientConst.EMPTY_OWNER_KEY,
                        0,
                        HgKvStore.SCAN_ANY,
                        EMPTY_BYTES);
        List<byte[]> positionList = new LinkedList<>();
        while (iterator.hasNext()) {
            HgKvEntry entry = iterator.next();
            positionList.add(iterator.position());
            if (count >= max) {
                break;
            }
        }

        iterator = session.scanIterator(tableName, 100);

        byte[] position = positionList.get(50);
        HgStoreTestUtil.println("seek: " + Arrays.toString(position));
        iterator.seek(position);
        HgStoreTestUtil.println(iterator);
        // Assert.assertEquals(50, HgStoreTestUtil.amountOf(iterator));

    }

    @Test
    public void truncate() {
        HgStoreTestUtil.println("--- test truncate ---");
        String graph = "graph_truncate";
        String tableName = TABLE_NAME;
        String keyName = "KEY_TRUNCATE";

        HgStoreSession session = getStoreSession(graph);
        HgStoreTestUtil.batchPut(session, tableName, keyName, 100);
        Assert.assertEquals(100, HgStoreTestUtil.amountOf(session.scanIterator(tableName)));

        session.truncate();
        Assert.assertEquals(0, HgStoreTestUtil.amountOf(session.scanIterator(tableName)));
    }

    // @Test
    public void scanIteratorHuge() {
        /*************** test no limit, with 10 millions **************/
        String tableName = TABLE_NAME;
        String keyName = "SCAN-HUGE";
        int amount = 10_000_000;
        HgStoreSession session = getStoreSession();

        if (HgStoreTestUtil.amountOf(
                session.scanIterator(tableName, HgStoreTestUtil.toAllPartitionKey(keyName), 10)) <
            10) {
            HgStoreTestUtil.batchPut(session, tableName, keyName, amount);
        }

        int count = 0;
        HgKvIterator<HgKvEntry> iterator = session.scanIterator(tableName);

        while (iterator.hasNext()) {
            count++;
            HgKvEntry entry = iterator.next();
        }

        Assert.assertEquals(amount, count);
    }

    @Test
    public void scanTable() {
        HgStoreSession session = getStoreSession("DEFAULT/hg1/g");
        HgStoreTestUtil.println(session.scanIterator("g+v", 10));
    }

    @Test
    public void testDelGraph() {
        HgStoreSession session = getStoreSession();
        session.deleteGraph(HgStoreTestUtil.GRAPH_NAME);
    }

    // TODO: need figure out
    // @Test
    public void benchmarkScanBatch() {
        HgStoreTestUtil.println("--- Benchmark scanBatch ---");
        String tableName = TABLE_NAME;
        String keyName = "SCAN-BATCH";
        int keyAmt = 30001;

        HgStoreSession session = getStoreSession();

        if (HgStoreTestUtil.amountOf(
                session.scanIterator(tableName, HgStoreTestUtil.toAllPartitionKey(keyName), 1)) <
            1) {
            HgStoreTestUtil.batchPut(session, tableName, keyName, keyAmt);
        }
        HgStoreTestUtil.println("-- Starting scan --");
        List<HgKvIterator<HgKvEntry>> iterators = session.scanBatch(HgScanQuery.tableOf(tableName));
        Assert.assertEquals(keyAmt, HgStoreTestUtil.amountIn(iterators));
    }

    @Test
    public void benchmarkScanBatch2() throws IOException {
        HgStoreTestUtil.println("--- Benchmark scanBatch2 ---");
        String tableName = TABLE_NAME;
        HgStoreSession session = getStoreSession();
        String keyName = "SCAN-BATCH";
        int keyAmt = 300;
        Map<HgOwnerKey, byte[]> data =
                HgStoreTestUtil.batchPut(session, tableName, keyName, keyAmt);
        HgStoreTestUtil.println("-- Starting scan --");
        MetricX metrics = MetricX.ofStart();
        // HgStoreTestUtil.println(session.scanIterator(tableName));
        long t = System.currentTimeMillis();
        int count = 0;
        List<HgOwnerKey> keys = new ArrayList<>();
        data.forEach((k, v) -> keys.add(k));
        KvCloseableIterator<HgKvIterator<HgKvEntry>> iterators =
                session.scanBatch2(
                        HgScanQuery.prefixIteratorOf(tableName, keys.iterator(),
                                                     ScanOrderType.ORDER_NONE)
                                   .builder()
                                   .setScanType(0x40)
                                   .build());
        while (iterators.hasNext()) {
            HgKvIterator<HgKvEntry> iterator = iterators.next();
            count += HgStoreTestUtil.amountOf(iterator);
        }
        keys.clear();
        iterators.close();
    }

    @Test
    public void benchmarkScanBatchSkipDegree() throws IOException {
        HgStoreTestUtil.println("--- Benchmark scanBatch2 1Owner---");
        String tableName = TABLE_NAME;
        HgStoreSession session = getStoreSession();
        String keyName = "SCAN-BATCH";
        int keyAmt = 300;
        byte[] owner = "Owner".getBytes();
        Map<HgOwnerKey, byte[]> data =
                HgStoreTestUtil.batchPut(
                        session,
                        tableName,
                        keyName,
                        keyAmt,
                        key -> {
                            return HgStoreTestUtil.toOwnerKey(owner, key);
                        });
        HgStoreTestUtil.println("-- Starting scan --");
        MetricX metrics = MetricX.ofStart();
        long t = System.currentTimeMillis();
        int count = 0;

        List<HgOwnerKey> keys = new ArrayList<>();
        keys.add(HgStoreTestUtil.toOwnerKey(owner, keyName));

        KvCloseableIterator<HgKvIterator<HgKvEntry>> iterators =
                session.scanBatch2(
                        HgScanQuery.prefixIteratorOf(tableName, keys.iterator(),
                                                     ScanOrderType.ORDER_NONE)
                                   .builder()
                                   .setScanType(0x40)
                                   .setSkipDegree(1)
                                   .build());
        while (iterators.hasNext()) {
            HgKvIterator<HgKvEntry> iterator = iterators.next();
            count += HgStoreTestUtil.amountOf(iterator);
        }
        keys.clear();
        iterators.close();
        iterators = null;

        HgStoreTestUtil.println("time is " + (System.currentTimeMillis() - t));
        metrics.end();
        HgStoreTestUtil.println("*************************************************");
        HgStoreTestUtil.println("***********  Batch Scan Completed  **************");
        HgStoreTestUtil.println("Total : " + (metrics.past() / 1000) + " (sec)");
        HgStoreTestUtil.println("  Sum : " + (MetricX.getIteratorWait() / 1000) + " (sec)");
        HgStoreTestUtil.println("  Amt : " + MetricX.getIteratorCount() + " (scans).");
        HgStoreTestUtil.println("  Avg : " + MetricX.getIteratorWaitAvg() + " (ms)");
        HgStoreTestUtil.println("  Max : " + MetricX.getIteratorWaitMax() + " (ms)");
        HgStoreTestUtil.println(" Fail : " + metrics.getFailureCount() + " (times)");
        HgStoreTestUtil.println(
                " Page : " + HgStoreClientConfig.of().getNetKvScannerPageSize() + " (KVs)");
        HgStoreTestUtil.println(" size is " + count);
        HgStoreTestUtil.println("*************************************************");
    }
}
