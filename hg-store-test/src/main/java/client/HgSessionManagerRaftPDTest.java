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

import static client.HgKvStoreTest.TABLE_NAME;
import static org.apache.hugegraph.store.client.util.HgStoreClientConst.EMPTY_BYTES;
import static util.HgStoreTestUtil.GRAPH_NAME;
import static util.HgStoreTestUtil.amountOf;
import static util.HgStoreTestUtil.batchPut;
import static util.HgStoreTestUtil.padLeftZeros;
import static util.HgStoreTestUtil.println;
import static util.HgStoreTestUtil.toAllPartitionKey;
import static util.HgStoreTestUtil.toBytes;
import static util.HgStoreTestUtil.toLong;
import static util.HgStoreTestUtil.toOwnerKey;
import static util.HgStoreTestUtil.toStr;
import static util.HgStoreTestUtil.toSuffix;

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
import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.HgKvStore;
import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.HgScanQuery;
import org.apache.hugegraph.store.HgStoreClient;
import org.apache.hugegraph.store.HgStoreSession;
import org.apache.hugegraph.store.client.grpc.KvCloseableIterator;
import org.apache.hugegraph.store.client.util.HgStoreClientConfig;
import org.apache.hugegraph.store.client.util.HgStoreClientConst;
import org.apache.hugegraph.store.client.util.MetricX;
import org.apache.hugegraph.store.grpc.common.ScanOrderType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import util.HgStoreTestUtil;

public class HgSessionManagerRaftPDTest {
    public static final String GRAPH_NAME_X = "default/hugegraph/x";
    public static final String GRAPH_NAME_Y = "default/hugegraph/y";
    public static final String GRAPH_NAME_Z = "default/hugegraph/z";
    public static final String TABLE_NAME_1 = "table1";
    public static final String TABLE_NAME_2 = "table2";
    public static final String TABLE_NAME_3 = "table3";
    private static final String PD_ADDRESS = "localhost:8686";
    public static HgStoreClient storeClient;
    private static PDClient pdClient;

    @BeforeClass
    public static void init() {
        pdClient = PDClient.create(PDConfig.of(PD_ADDRESS).setEnableCache(true));
        storeClient = HgStoreClient.create(pdClient);
    }

    private static HgStoreSession getStoreSession() {
        return storeClient.openSession(GRAPH_NAME);
    }

    private static HgStoreSession getStoreSession(String graph) {
        return storeClient.openSession(graph);
    }

    @Test
    public void putGet() {
        System.out.println("--- test put & get ---");

        HgStoreSession session = getStoreSession(GRAPH_NAME);

        HgOwnerKey key = toOwnerKey("FOR-PUT-KEY");
        byte[] value = toBytes("FOR-PUT-VALUE");

        long stime = System.currentTimeMillis();
        batchPut(session, TABLE_NAME, "BATCH-PUT-TEST", 30000);
        System.out.println("Time is " + (System.currentTimeMillis() - stime));
    }

    public void putGet2() {
        System.out.println("--- test put & get ---");

        HgStoreSession session = getStoreSession("testGraph");
        HgOwnerKey key = toOwnerKey("FOR-PUT-KEY");
        byte[] value = toBytes("FOR-PUT-VALUE");
        long stime = System.currentTimeMillis();
        batchPut(session, "testTable", "BATCH-PUT-TEST", 30000);
        System.out.println("Time is " + (System.currentTimeMillis() - stime));
    }

    @Test
    public void scanPartition() {
        println("--- test scanPartition ---");

        HgStoreSession session = getStoreSession();

        Iterator iterator =
                session.scanIterator(TABLE_NAME, 0, 65535, HgKvStore.SCAN_HASHCODE, EMPTY_BYTES);
        System.out.println(amountOf(iterator));
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

        HgOwnerKey key = toOwnerKey("FOR-PUT-KEY-" + timestamp);
        byte[] value = toBytes("FOR-PUT-VALUE");

        Assert.assertTrue(session.put(TABLE_NAME, key, value));

        Assert.assertEquals(toStr(value), toStr(session.get(TABLE_NAME, key)));
    }

    @Test
    public void testBatchPutExt() throws IOException {
        System.out.println("--- test batchPut ---");
        HgStoreSession session = getStoreSession();
        session.truncate();
        String keyPrefix = "BATCH-GET-UNIT";

        Map<HgOwnerKey, byte[]> map = batchPut(session, keyPrefix, 10);

        File outputFile = new File("tmp/batch_put_list");
        if (outputFile.exists()) {
            FileUtils.forceDelete(outputFile);
        }
        FileUtils.forceMkdir(new File("tmp/"));

        ObjectOutputStream oo = new ObjectOutputStream(new FileOutputStream(outputFile));
        oo.writeObject(map);
        oo.close();
        System.out.printf("%d entries have be put into graph %s\n", map.size(), GRAPH_NAME);

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
                (resList.stream().map(e -> map.containsKey(toOwnerKey(e.key())))
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

        Map<HgOwnerKey, byte[]> map = batchPut(session, keyPrefix, 30);

        File outputFile = new File("tmp/batch_put_list");
        if (outputFile.exists()) {
            FileUtils.forceDelete(outputFile);
        }
        FileUtils.forceMkdir(new File("tmp/"));

        ObjectOutputStream oo = new ObjectOutputStream(new FileOutputStream(outputFile));
        oo.writeObject(map);
        oo.close();
        System.out.printf("%d entries have be put into graph %s\n", map.size(), GRAPH_NAME);
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
        batchPut(session1, TABLE_NAME_1, keyPrefix1, 1000);
        batchPut(session1, TABLE_NAME_2, keyPrefix1, 1000);
        batchPut(session1, TABLE_NAME_3, keyPrefix1, 1000);
        batchPut(session2, TABLE_NAME_1, keyPrefix2, 1000);
        batchPut(session2, TABLE_NAME_2, keyPrefix2, 1000);
        batchPut(session2, TABLE_NAME_3, keyPrefix2, 1000);
        batchPut(session3, TABLE_NAME_1, keyPrefix3, 1000);
        batchPut(session3, TABLE_NAME_2, keyPrefix3, 1000);
        batchPut(session3, TABLE_NAME_3, keyPrefix3, 1000);
    }

    //     @Test
    // CAUTION: ONLY FOR LONG！
    // 注意：目前只可以对long类型value进行Merge操作。
    public void merge() {
        System.out.println("--- test merge (1+1=2)---");
        HgStoreSession session = getStoreSession();
        String mergeKey = "merge-key";
        HgOwnerKey key = toOwnerKey(mergeKey);
        byte[] value = toBytes(1L);

        System.out.println("- put " + mergeKey + ":1 -");
        session.put(TABLE_NAME, key, value);
        System.out.println("- merge " + mergeKey + ":1 -");
        session.merge(TABLE_NAME, key, value);
        long res = toLong(session.get(TABLE_NAME, key));
        System.out.printf("after merge " + mergeKey + "=%s%n", res);
        Assert.assertEquals(2L, res);

        String putStr = "19";
        session.put(TABLE_NAME, key, toBytes(putStr));
        byte[] b1 = session.get(TABLE_NAME, key);
        Assert.assertEquals(putStr, toStr(b1));
    }

    @Test
    public void delete() {
        System.out.println("--- test delete ---");
        HgStoreSession session = getStoreSession();

        String delKey = "del-key";
        String delValue = "del-value";
        HgOwnerKey key = toOwnerKey(delKey);
        byte[] value = toBytes(delValue);

        println("- put " + delKey + ": " + delValue);
        session.put(TABLE_NAME, key, value);
        println("- delete " + delKey);
        session.delete(TABLE_NAME, key);
        value = session.get(TABLE_NAME, key);
        println("- get " + delKey + ": " + toStr(value));
        Assert.assertEquals(EMPTY_BYTES, value);
    }

    @Test
    public void deleteSingle() {
        System.out.println("--- test deleteSingle ---");
        HgStoreSession session = getStoreSession();

        String delKey = "del-single-key";
        String delValue = "del-single-value";
        HgOwnerKey key = toOwnerKey(delKey);
        byte[] value = toBytes(delValue);

        session.put(TABLE_NAME, key, value);
        value = session.get(TABLE_NAME, key);
        Assert.assertEquals(delValue, toStr(value));

        println("- delete-single : [" + delKey + "]");
        session.deleteSingle(TABLE_NAME, key);
        value = session.get(TABLE_NAME, key);
        println("- after del, get [" + delKey + "] = " + toStr(value));
        Assert.assertEquals("", toStr(value));
    }

    @Test
    public void deleteRange() {
        println("--- test deleteRange ---");
        HgStoreSession session = getStoreSession();

        String rangePrefix = "DEL-RANGE-KEY";
        String owner = "batch-delete-owner";
        Map<HgOwnerKey, byte[]> map =
                batchPut(
                        session,
                        TABLE_NAME,
                        rangePrefix,
                        10,
                        key -> {
                            return toOwnerKey(owner, key);
                        });

        HgOwnerKey startKey = toOwnerKey(owner, rangePrefix + "-00");
        HgOwnerKey endKey = toOwnerKey(owner, rangePrefix + "-05");

        Assert.assertTrue(session.deleteRange(TABLE_NAME, startKey, endKey));

        println("- after delete range from [" + toStr(startKey.getKey()) + "] to [" +
                toStr(endKey.getKey()) + "]");

        for (int i = 0; i < 10; i++) {
            HgOwnerKey key =
                    toOwnerKey(owner, rangePrefix + "-" + padLeftZeros(String.valueOf(i), 2));
            String value = toStr(session.get(TABLE_NAME, key));

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
                batchPut(
                        session,
                        TABLE_NAME,
                        prefixStr,
                        10,
                        key -> {
                            return toOwnerKey(owner, key);
                        });

        // printOwner(map.entrySet().stream().map(e -> e.getKey()).collect(Collectors.toList()));

        HgOwnerKey prefix = toOwnerKey(owner, prefixStr);

        Assert.assertEquals(10, amountOf(session.scanIterator(TABLE_NAME, prefix)));
        session.deletePrefix(TABLE_NAME, prefix);
        Assert.assertEquals(0, amountOf(session.scanIterator(TABLE_NAME, prefix)));

        println("- after delete by prefix:[" + prefixStr + "]");

        for (int i = 0; i < 10; i++) {
            HgOwnerKey key = toOwnerKey(owner, prefixStr + toSuffix(i, 2));
            String value = toStr(session.get(TABLE_NAME, key));
            Assert.assertEquals("", value);
        }
    }

    @Test
    public void scanIterator() {
        println("--- test scanIterator ---");
        String tableName = "UNIT_SCAN";
        String keyName = "SCAN-ITER";
        HgStoreSession session = getStoreSession();
        batchPut(session, tableName, keyName, 10000);
        int count = 0;
        int limit = 0;
        int max = 99999;
        HgKvIterator<HgKvEntry> iterator = null;

        println("-- test 0 element --");
        iterator = session.scanIterator(tableName, toAllPartitionKey("__SCAN-001"),
                                        toAllPartitionKey("__SCAN-100"), 0);
        Assert.assertFalse(iterator.hasNext());
        try {
            iterator.next();
            Assert.fail();
        } catch (Throwable t) {
            println("-- test NoSuchElementException --");
            Assert.assertTrue(t instanceof NoSuchElementException);
        }

        for (int i = 1; i <= 10; i++) {
            limit = i;
            iterator =
                    session.scanIterator(
                            tableName, toAllPartitionKey(keyName + "-0"),
                            toAllPartitionKey(keyName + "-1"), limit);
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
                            tableName, toAllPartitionKey(keyName + "-00001"),
                            toAllPartitionKey(keyName + "-00005"), limit);
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
                        tableName, toAllPartitionKey(keyName + "-0"),
                        toAllPartitionKey(keyName + "-1"), limit);

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
            HgKvEntry entry = iterator.next();
            if (count >= max) {
                break;
            }
        }
        Assert.assertEquals(10000, count);

        iterator = session.scanIterator(tableName, toAllPartitionKey(keyName + "-01"));
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
        println("--- test scanIterator_range ---");
        String graph = "UNIT/paging";
        String tableName = "UNIT_SCAN_PAGING";
        String keyName = "SCAN-PAGING";
        int keyAmt = 100;
        HgStoreSession session = getStoreSession(graph);
        batchPut(session, tableName, keyName, keyAmt);
        int count = 0;
        int limit = 0;
        int max = 99999;
        HgKvIterator<HgKvEntry> iterator = null;

        count = 0;

        iterator =
                session.scanIterator(
                        tableName,
                        toAllPartitionKey(keyName + "-000"),
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
        println("seek: " + Arrays.toString(position));
        iterator.seek(position);
        HgStoreTestUtil.println(iterator);
        // Assert.assertEquals(50, HgStoreTestUtil.amountOf(iterator));

    }

    @Test
    public void truncate() {
        println("--- test truncate ---");
        String graph = "graph_truncate";
        String tableName = "UNIT_TRUNCATE_1";
        String keyName = "KEY_TRUNCATE";

        HgStoreSession session = getStoreSession(graph);
        batchPut(session, tableName, keyName, 100);
        Assert.assertEquals(100, amountOf(session.scanIterator(tableName)));

        String tableName2 = "UNIT_TRUNCATE_2";
        batchPut(session, tableName2, keyName, 100);
        Assert.assertEquals(100, amountOf(session.scanIterator(tableName2)));

        session.truncate();
        Assert.assertEquals(0, amountOf(session.scanIterator(tableName)));
        Assert.assertEquals(0, amountOf(session.scanIterator(tableName2)));
    }

    // // @Test
    public void scanIteratorHuge() {

        /*************** test no limit, with 10 millions **************/
        String tableName = "UNIT_HUGE";
        String keyName = "SCAN-HUGE";
        int amount = 10_000_000;
        HgStoreSession session = getStoreSession();

        if (amountOf(session.scanIterator(tableName, toAllPartitionKey(keyName), 10)) < 10) {
            batchPut(session, tableName, keyName, amount);
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
        session.deleteGraph(GRAPH_NAME);
    }

    @Test
    public void benchmarkScanBatch() {
        println("--- Benchmark scanBatch ---");
        String tableName = "Benchmark_SCAN_BATCH";
        String keyName = "SCAN-BATCH";
        int keyAmt = 30001;

        HgStoreSession session = getStoreSession();

        if (amountOf(session.scanIterator(tableName, toAllPartitionKey(keyName), 1)) < 1) {
            batchPut(session, tableName, keyName, keyAmt);
        }
        println("-- Starting scan --");
        MetricX metrics = MetricX.ofStart();
        // HgStoreTestUtil.println(session.scanIterator(tableName));
        List<HgKvIterator<HgKvEntry>> iterators = session.scanBatch(HgScanQuery.tableOf(tableName));
        Assert.assertEquals(keyAmt, HgStoreTestUtil.amountIn(iterators));
    }

    @Test
    public void benchmarkScanBatch2() throws IOException {
        println("--- Benchmark scanBatch2 ---");
        String tableName = TABLE_NAME;
        HgStoreSession session = getStoreSession();
        String keyName = "SCAN-BATCH";
        int keyAmt = 300;
        Map<HgOwnerKey, byte[]> data = batchPut(session, tableName, keyName, keyAmt);
        println("-- Starting scan --");
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
            count += amountOf(iterator);
        }
        keys.clear();
        iterators.close();
    }

    @Test
    public void benchmarkScanBatchSkipDegree() throws IOException {
        println("--- Benchmark scanBatch2 1Owner---");
        String tableName = TABLE_NAME;
        HgStoreSession session = getStoreSession();
        String keyName = "SCAN-BATCH";
        int keyAmt = 300;
        byte[] owner = "Owner".getBytes();
        Map<HgOwnerKey, byte[]> data =
                batchPut(
                        session,
                        tableName,
                        keyName,
                        keyAmt,
                        key -> {
                            return toOwnerKey(owner, key);
                        });
        println("-- Starting scan --");
        MetricX metrics = MetricX.ofStart();
        long t = System.currentTimeMillis();
        int count = 0;

        List<HgOwnerKey> keys = new ArrayList<>();
        keys.add(toOwnerKey(owner, keyName));

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
            count += amountOf(iterator);
        }
        keys.clear();
        iterators.close();
        iterators = null;

        println("time is " + (System.currentTimeMillis() - t));
        metrics.end();
        println("*************************************************");
        println("***********  Batch Scan Completed  **************");
        println("Total : " + (metrics.past() / 1000) + " (sec)");
        println("  Sum : " + (MetricX.getIteratorWait() / 1000) + " (sec)");
        println("  Amt : " + MetricX.getIteratorCount() + " (scans).");
        println("  Avg : " + MetricX.getIteratorWaitAvg() + " (ms)");
        println("  Max : " + MetricX.getIteratorWaitMax() + " (ms)");
        println(" Fail : " + metrics.getFailureCount() + " (times)");
        println(" Page : " + HgStoreClientConfig.of().getNetKvScannerPageSize() + " (KVs)");
        println(" size is " + count);
        println("*************************************************");
    }
}
