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

package org.apache.hugegraph.store;

import static org.apache.hugegraph.store.client.util.HgStoreClientConst.EMPTY_BYTES;

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
import org.apache.hugegraph.store.client.grpc.KvCloseableIterator;
import org.apache.hugegraph.store.client.util.HgStoreClientConfig;
import org.apache.hugegraph.store.client.util.HgStoreClientConst;
import org.apache.hugegraph.store.client.util.MetricX;
import org.apache.hugegraph.store.grpc.common.ScanOrderType;
import org.apache.hugegraph.store.util.HgStoreTestUtil;
import org.junit.Assert;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;


/**
 * 使用pd，支持raft的单元测试
 */
@Slf4j
public class HgSessionManagerRaftPDTest {
    public final static String GRAPH_NAME_X = "default/hugegraph/x";
    public final static String GRAPH_NAME_Y = "default/hugegraph/y";
    public final static String GRAPH_NAME_Z = "default/hugegraph/z";
    public final static String TABLE_NAME_1 = "table1";
    public final static String TABLE_NAME_2 = "table2";
    public final static String TABLE_NAME_3 = "table3";
    private static final String pdAddress = "localhost:8686";
    public static HgStoreClient storeClient;
    private static PDClient pdClient;

    // @BeforeClass
    public static void init() {
        pdClient = PDClient.create(PDConfig.of(pdAddress).setEnableCache(true));
        storeClient = HgStoreClient.create(pdClient);
    }

    private static HgStoreSession getStoreSession() {
        return storeClient.openSession(HgStoreTestUtil.GRAPH_NAME);
    }

    private static HgStoreSession getStoreSession(String graph) {
        return storeClient.openSession(graph);
    }

    // @Test
    public void put_get() {
        System.out.println("--- test put & get ---");

        HgStoreSession session = getStoreSession(HgStoreTestUtil.GRAPH_NAME);

        HgOwnerKey key = HgStoreTestUtil.toOwnerKey("FOR-PUT-KEY");
        //HgOwnerKey key = toAllNodeKey("FOR-PUT-KEY");
        byte[] value = HgStoreTestUtil.toBytes("FOR-PUT-VALUE");

        //   Assert.assertTrue(session.put(TABLE_NAME, key, value));

        //   Assert.assertEquals(toStr(value), toStr(session.get(TABLE_NAME, key)));

        long stime = System.currentTimeMillis();
        HgStoreTestUtil.batchPut(session, HgStoreTestUtil.TABLE_NAME, "BATCH-PUT-TEST", 30000);
        System.out.println("Time is " + (System.currentTimeMillis() - stime));
    }

    public void put_get2() {
        System.out.println("--- test put & get ---");

        HgStoreSession session = getStoreSession("testGraph");
        HgOwnerKey key = HgStoreTestUtil.toOwnerKey("FOR-PUT-KEY");
        byte[] value = HgStoreTestUtil.toBytes("FOR-PUT-VALUE");
        long stime = System.currentTimeMillis();
        HgStoreTestUtil.batchPut(session, "testTable", "BATCH-PUT-TEST", 30000);
        System.out.println("Time is " + (System.currentTimeMillis() - stime));
    }

    // @Test
    public void scanPartition() {
        HgStoreTestUtil.println("--- test scanPartition ---");

        HgStoreSession session = getStoreSession();
        //  System.out.println(amountOf(session.scanIterator(TABLE_NAME)));


        Iterator iterator = session.scanIterator(HgStoreTestUtil.TABLE_NAME, 0, 65535,
                                                 HgKvStore.SCAN_HASHCODE, EMPTY_BYTES);
        System.out.println(HgStoreTestUtil.amountOf(iterator));
    }

    // @Test
    public void check() {
        System.out.println("--- test check ---");

        HgStoreSession session = getStoreSession();
        HgKvIterator<HgKvEntry> iterator = session.scanIterator(HgStoreTestUtil.TABLE_NAME);
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        System.out.println(count);
    }

    // @Test
    public void put_get_unique() {
        System.out.println("--- test put & get ---");

        HgStoreSession session = getStoreSession();

        // add timestamp into key to avoid key duplication
        Date date = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("HHmmss");
        String timestamp = formatter.format(date);

        HgOwnerKey key = HgStoreTestUtil.toOwnerKey("FOR-PUT-KEY-" + timestamp);
        //HgOwnerKey key = toAllNodeKey("FOR-PUT-KEY");
        byte[] value = HgStoreTestUtil.toBytes("FOR-PUT-VALUE");

        Assert.assertTrue(session.put(HgStoreTestUtil.TABLE_NAME, key, value));

        Assert.assertEquals(HgStoreTestUtil.toStr(value), HgStoreTestUtil.toStr(session.get(
                HgStoreTestUtil.TABLE_NAME, key)));
    }


    @Test
    public void testBatchPutExt() throws IOException {
        System.out.println("--- test batchPut ---");
        HgStoreSession session = getStoreSession();
        String keyPrefix = "BATCH-GET-UNIT";

        Map<HgOwnerKey, byte[]> map = HgStoreTestUtil.batchPut(session, keyPrefix, 1000);

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
        iterator = session.scanIterator(HgStoreTestUtil.TABLE_NAME);
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        Assert.assertEquals(1000, count);

    }


    // @Test
    public void testBatchGetExt() throws IOException, ClassNotFoundException {
        File outputFile = new File("tmp/batch_put_list");
        ObjectInputStream ois = new ObjectInputStream(new FileInputStream(outputFile));
        Map<HgOwnerKey, byte[]> map = (Map<HgOwnerKey, byte[]>) ois.readObject();
        ois.close();
        System.out.printf("%d entries get from %s\n", map.size(), outputFile.getPath());

        HgStoreSession session = getStoreSession();
        List<HgOwnerKey> keyList =
                map.entrySet().stream().map(e -> e.getKey()).collect(Collectors.toList());
        List<HgKvEntry> resList = session.batchGetOwner(HgStoreTestUtil.TABLE_NAME, keyList);

        Assert.assertTrue((resList.stream()
                                  .map(e -> map.containsKey(HgStoreTestUtil.toOwnerKey(e.key())))
                                  .allMatch(Boolean::booleanValue))
        );
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

        Map<HgOwnerKey, byte[]> map = HgStoreTestUtil.batchPut(session, keyPrefix, 300000);

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

    // @Test
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

    // @Test
    //CAUTION: ONLY FOR LONG！
    //注意：目前只可以对long类型value进行Merge操作。
    public void merge() {
        System.out.println("--- test merge (1+1=2)---");
        HgStoreSession session = getStoreSession();
        String mergeKey = "merge-key";
        HgOwnerKey key = HgStoreTestUtil.toOwnerKey(mergeKey);
        byte[] value = HgStoreTestUtil.toBytes(1L);

        System.out.println("- put " + mergeKey + ":1 -");
        session.put(HgStoreTestUtil.TABLE_NAME, key, value);
        System.out.println("- merge " + mergeKey + ":1 -");
        session.merge(HgStoreTestUtil.TABLE_NAME, key, value);
        long res = HgStoreTestUtil.toLong(session.get(HgStoreTestUtil.TABLE_NAME, key));
        System.out.printf("after merge " + mergeKey + "=%s%n", res);
        Assert.assertEquals(2L, res);

        String putStr = "19";
        session.put(HgStoreTestUtil.TABLE_NAME, key, HgStoreTestUtil.toBytes(putStr));
        byte[] b1 = session.get(HgStoreTestUtil.TABLE_NAME, key);
        Assert.assertEquals(putStr, HgStoreTestUtil.toStr(b1));
    }

    // @Test
    public void delete() {
        System.out.println("--- test delete ---");
        HgStoreSession session = getStoreSession();

        String delKey = "del-key";
        String delValue = "del-value";
        HgOwnerKey key = HgStoreTestUtil.toOwnerKey(delKey);
        byte[] value = HgStoreTestUtil.toBytes(delValue);

        HgStoreTestUtil.println("- put " + delKey + ": " + delValue);
        session.put(HgStoreTestUtil.TABLE_NAME, key, value);
        HgStoreTestUtil.println("- delete " + delKey);
        session.delete(HgStoreTestUtil.TABLE_NAME, key);
        value = session.get(HgStoreTestUtil.TABLE_NAME, key);
        HgStoreTestUtil.println("- get " + delKey + ": " + HgStoreTestUtil.toStr(value));
        Assert.assertEquals(EMPTY_BYTES, value);
    }

    // @Test
    public void deleteSingle() {
        System.out.println("--- test deleteSingle ---");
        HgStoreSession session = getStoreSession();

        String delKey = "del-single-key";
        String delValue = "del-single-value";
        HgOwnerKey key = HgStoreTestUtil.toOwnerKey(delKey);
        byte[] value = HgStoreTestUtil.toBytes(delValue);

        HgStoreTestUtil.println("- put [" + delKey + "] = " + delValue);
        session.put(HgStoreTestUtil.TABLE_NAME, key, value);

        value = session.get(HgStoreTestUtil.TABLE_NAME, key);
        HgStoreTestUtil.println(
                "- before del, get [" + delKey + "] = " + HgStoreTestUtil.toStr(value));
        Assert.assertEquals(delValue, HgStoreTestUtil.toStr(value));

        HgStoreTestUtil.println("- delete-single : [" + delKey + "]");
        session.deleteSingle(HgStoreTestUtil.TABLE_NAME, key);
        value = session.get(HgStoreTestUtil.TABLE_NAME, key);
        HgStoreTestUtil.println(
                "- after del, get [" + delKey + "] = " + HgStoreTestUtil.toStr(value));
        Assert.assertEquals("", HgStoreTestUtil.toStr(value));

    }

    // @Test
    public void deleteRange() {
        HgStoreTestUtil.println("--- test deleteRange ---");
        HgStoreSession session = getStoreSession();

        String rangePrefix = "DEL-RANGE-KEY";
        String owner = "batch-delete-owner";
        Map<HgOwnerKey, byte[]> map =
                HgStoreTestUtil.batchPut(session, HgStoreTestUtil.TABLE_NAME, rangePrefix, 10,
                                         key -> {
                                             return HgStoreTestUtil.toOwnerKey(owner, key);
                                         });

        HgOwnerKey startKey = HgStoreTestUtil.toOwnerKey(owner, rangePrefix + "-00");
        HgOwnerKey endKey = HgStoreTestUtil.toOwnerKey(owner, rangePrefix + "-05");

        Assert.assertTrue(session.deleteRange(HgStoreTestUtil.TABLE_NAME, startKey, endKey));

        HgStoreTestUtil.println("- after delete range from ["
                                + HgStoreTestUtil.toStr(startKey.getKey())
                                + "] to ["
                                + HgStoreTestUtil.toStr(endKey.getKey()) + "]");

        for (int i = 0; i < 10; i++) {
            HgOwnerKey key = HgStoreTestUtil.toOwnerKey(owner, rangePrefix + "-" +
                                                               HgStoreTestUtil.padLeftZeros(
                                                                       String.valueOf(i), 2));
            String value = HgStoreTestUtil.toStr(session.get(HgStoreTestUtil.TABLE_NAME, key));
            HgStoreTestUtil.println(
                    "- get [" + HgStoreTestUtil.toStr(key.getKey()) + "] = " + value);

            // TODO: [start,end)?
            if (i < 5) {
                Assert.assertEquals("", value);
            } else {
                //println(value);
                Assert.assertNotEquals("", value);
            }
        }

    }

    // @Test
    public void deletePrefix() {
        System.out.println("--- test deletePrefix ---");
        HgStoreSession session = getStoreSession();

        String prefixStr = "DEL-PREFIX-KEY";
        String owner = "batch-delete-owner";
        Map<HgOwnerKey, byte[]> map =
                HgStoreTestUtil.batchPut(session, HgStoreTestUtil.TABLE_NAME, prefixStr, 10,
                                         key -> {
                                             return HgStoreTestUtil.toOwnerKey(owner, key);
                                         });

        //printOwner(map.entrySet().stream().map(e -> e.getKey()).collect(Collectors.toList()));

        HgOwnerKey prefix = HgStoreTestUtil.toOwnerKey(owner, prefixStr);

        Assert.assertEquals(10, HgStoreTestUtil.amountOf(session.scanIterator(
                HgStoreTestUtil.TABLE_NAME, prefix)));
        session.deletePrefix(HgStoreTestUtil.TABLE_NAME, prefix);
        Assert.assertEquals(0, HgStoreTestUtil.amountOf(session.scanIterator(
                HgStoreTestUtil.TABLE_NAME, prefix)));

        HgStoreTestUtil.println("- after delete by prefix:[" + prefixStr + "]");

        for (int i = 0; i < 10; i++) {
            HgOwnerKey key =
                    HgStoreTestUtil.toOwnerKey(owner, prefixStr + HgStoreTestUtil.toSuffix(i, 2));
            String value = HgStoreTestUtil.toStr(session.get(HgStoreTestUtil.TABLE_NAME, key));
            System.out.println("- get [" + HgStoreTestUtil.toStr(key.getKey()) + "] = " + value);
            Assert.assertEquals("", value);
        }

    }
//    // @Test
//    public void batchDeleteOwner() {
//        System.out.println("--- test batchDelete ---");
//        HgStoreSession session = getStoreSession();
//        String batchPrefix = "DEL-BATCH-KEY";
//        batchPut(session, batchPrefix, 10);
//
//        for (int i = 0; i < 10; i++) {
//            HgOwnerKey key = toOwnerKey(batchPrefix + toSuffix(i, 2));
//            String value = toStr(session.get(TABLE_NAME, key));
//            println("- get [" + toStr(key.getKey()) + "] = " + value);
//            Assert.assertNotEquals("", value);
//        }
//
//        Set<HgOwnerKey> keys = new HashSet<>();
//        for (int i = 0; i < 5; i++) {
//            keys.add(toOwnerKey(batchPrefix + toSuffix(i, 2)));
//        }
//
//        Map<String, Set<HgOwnerKey>> map = new HashMap<>(1);
//        map.put(TABLE_NAME, keys);
//        session.batchDeleteOwner(map);
//
//        for (int i = 0; i < 10; i++) {
//            HgOwnerKey key = toOwnerKey(batchPrefix + toSuffix(i, 2));
//            String value = toStr(session.get(TABLE_NAME, key));
//            println("- get [" + toStr(key.getKey()) + "] = " + value);
//            // TODO: [,)?
//            if (i < 5) {
//                Assert.assertEquals("", value);
//            } else {
//                Assert.assertNotEquals("", value);
//            }
//
//        }
//    }

//    // @Test
//    public void batchDeleteRangeOwner() {
//        System.out.println("--- test batchDeleteRange ---");
//        HgStoreSession session = getStoreSession();
//        String rangePrefix = "DEL-RANGE-KEY";
//        String owner="batch-delete-owner";
//
//        batchPut(session, TABLE_NAME,rangePrefix, 10,key->{
//            return toOwnerKey(owner,key);
//        });
//        batchPut(session, TABLE_NAME2, rangePrefix, 10,key->{
//            return toOwnerKey(owner,key);
//        });
//
//        HgOwnerKey startKey1 = toOwnerKey(owner,rangePrefix + "-03");
//        HgOwnerKey endKey1 = toOwnerKey(owner,rangePrefix + "-05");
//
//        HgOwnerKey startKey2 = toOwnerKey(owner,rangePrefix + "-06");
//        HgOwnerKey endKey2 = toOwnerKey(owner,rangePrefix + "-09");
//
//        Map<String, HgPair<HgOwnerKey, HgOwnerKey>> map = new HashMap<>();
//        map.put(TABLE_NAME, new HgPair<>(startKey1, endKey1));
//        map.put(TABLE_NAME2, new HgPair<>(startKey2, endKey2));
//
//        session.batchDeleteRangeOwner(map);
//
//        for (int i = 0; i < 10; i++) {
//            HgOwnerKey key = toOwnerKey(owner,rangePrefix + toSuffix(i, 2));
//            String value = toStr(session.get(TABLE_NAME, key));
//            println("- get [" + toStr(key.getKey()) + "] = " + value);
//
//            if (i >= 3 && i < 5) {
//                Assert.assertEquals("", value);
//            } else {
//                Assert.assertNotEquals("", value);
//            }
//
//        }
//
//        for (int i = 0; i < 10; i++) {
//            HgOwnerKey key = toOwnerKey(owner,rangePrefix + toSuffix(i, 2));
//            String value = toStr(session.get(TABLE_NAME2, key));
//            println("- get [" + toStr(key.getKey()) + "] = " + value);
//            if (i >= 6 && i < 9) {
//                Assert.assertEquals("", value);
//            } else {
//                Assert.assertNotEquals("", value);
//            }
//
//        }
//    }

//    // @Test
//    public void batchDeletePrefix() {
//        System.out.println("--- test batchDeletePrefix ---");
//        HgStoreSession session = getStoreSession();
//        String batchKey1 = "DEL-PREFIX-BATCH-1-KEY";
//        String batchKey2 = "DEL-PREFIX-BATCH-2-KEY";
//        String owner="batch-delete-owner";
//
//        batchPut(session, TABLE_NAME, batchKey1, 10,key->{
//            return toOwnerKey(owner,key);
//        });
//        batchPut(session, TABLE_NAME2, batchKey2, 10,key->{
//            return toOwnerKey(owner,key);
//        });
//
//        HgOwnerKey prefix1 = toOwnerKey(owner,batchKey1);
//        HgOwnerKey prefix2 = toOwnerKey(owner,batchKey2);
//
//        Set<HgOwnerKey> set = new HashSet<>();
//
//        set.add(prefix1);
//        set.add(prefix2);
//
//        Map<String, Set<HgOwnerKey>> map = new HashMap<>();
//        map.put(TABLE_NAME, set);
//        map.put(TABLE_NAME2, set);
//
//        Assert.assertEquals(10, amountOf(session.scanIterator(TABLE_NAME, prefix1)));
//        Assert.assertEquals(10, amountOf(session.scanIterator(TABLE_NAME2, prefix2)));
//
//        session.batchDeletePrefixOwner(map);
//
//        Assert.assertEquals(0, amountOf(session.scanIterator(TABLE_NAME, prefix1)));
//        Assert.assertEquals(0, amountOf(session.scanIterator(TABLE_NAME2, prefix2)));
//
//    }

    // @Test
    public void scanIterator() {
        HgStoreTestUtil.println("--- test scanIterator ---");
        String tableName = "UNIT_SCAN";
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

        HgStoreTestUtil.println("-- test limit 1 to 10 --");
        for (int i = 1; i <= 10; i++) {
            HgStoreTestUtil.println("- limit " + i + " -");
            limit = i;
            iterator = session.scanIterator(tableName,
                                            HgStoreTestUtil.toAllPartitionKey(keyName + "-0"),
                                            HgStoreTestUtil.toAllPartitionKey(keyName + "-1"),
                                            limit);
            count = 0;
            while (iterator.hasNext()) {
                count++;
                HgKvEntry entry = iterator.next();
                HgStoreTestUtil.println(entry);
            }
            Assert.assertEquals(limit, count);
        }

        HgStoreTestUtil.println("-- test limit 1 to 10 not enough --");
        for (int i = 1; i <= 10; i++) {
            HgStoreTestUtil.println("- limit " + i + " -");
            limit = i;
            iterator = session.scanIterator(tableName,
                                            HgStoreTestUtil.toAllPartitionKey(keyName + "-00001"),
                                            HgStoreTestUtil.toAllPartitionKey(keyName + "-00005"),
                                            limit);
            count = 0;
            while (iterator.hasNext()) {
                count++;
                HgKvEntry entry = iterator.next();
                HgStoreTestUtil.println(entry);
            }
            if (i <= 5) {
                Assert.assertEquals(limit, count);
            } else {
                Assert.assertEquals(5, count);
            }

        }

        HgStoreTestUtil.println("-- test limit 0 (no limit) --");
        limit = 0;
        iterator =
                session.scanIterator(tableName, HgStoreTestUtil.toAllPartitionKey(keyName + "-0"),
                                     HgStoreTestUtil.toAllPartitionKey(keyName + "-1"), limit);

        count = 0;
        while (iterator.hasNext()) {
            count++;
            HgKvEntry entry = iterator.next();
            if (count % 1000 == 0) {
                HgStoreTestUtil.println(entry);
            }
            if (count >= max) break;
        }
        Assert.assertEquals(10000, count);

        HgStoreTestUtil.println("-- test scan all --");
        iterator = session.scanIterator(tableName);
        count = 0;
        while (iterator.hasNext()) {
            count++;
            HgKvEntry entry = iterator.next();
            if (count % 1000 == 0) {
                HgStoreTestUtil.println(entry);
            }
            if (count >= max) break;
        }
        Assert.assertEquals(10000, count);

        HgStoreTestUtil.println("-- test scan prefix --");
        iterator =
                session.scanIterator(tableName, HgStoreTestUtil.toAllPartitionKey(keyName + "-01"));
        count = 0;
        while (iterator.hasNext()) {
            count++;
            HgKvEntry entry = iterator.next();
            if (count % 100 == 0) {
                HgStoreTestUtil.println(entry);
            }
            if (count >= max) break;
        }
        Assert.assertEquals(1000, count);
    }

    // @Test
    public void paging() {
        HgStoreTestUtil.println("--- test scanIterator_range ---");
        String graph = "UNIT/paging";
        String tableName = "UNIT_SCAN_PAGING";
        String keyName = "SCAN-PAGING";
        int keyAmt = 100;
        HgStoreSession session = getStoreSession(graph);
        HgStoreTestUtil.batchPut(session, tableName, keyName, keyAmt);
        int count = 0;
        int limit = 0;
        int max = 99999;
        HgKvIterator<HgKvEntry> iterator = null;

        count = 0;

        iterator = session.scanIterator(tableName
                , HgStoreTestUtil.toAllPartitionKey(keyName + "-000")
                , HgStoreClientConst.EMPTY_OWNER_KEY
                , 0, HgKvStore.SCAN_ANY, EMPTY_BYTES
        );
        //HgStoreTestUtil.println(iterator);
        //   Assert.assertEquals(100, HgStoreTestUtil.amountOf(iterator));
        List<byte[]> positionList = new LinkedList<>();
        while (iterator.hasNext()) {
            HgStoreTestUtil.print((count++) + " ");
            HgKvEntry entry = iterator.next();
            HgStoreTestUtil.print(entry);
            HgStoreTestUtil.print(" " + Arrays.toString(iterator.position()) + "\n");
            positionList.add(iterator.position());
            if (count >= max) break;
        }


/*        iterator = session.scanIterator(tableName
                , toAllPartitionKey(keyName + "-000")
                , HgStoreClientConst.EMPTY_OWNER_KEY
                , 0, HgKvStore.SCAN_ANY, EMPTY_BYTES
        );

        byte[] position=positionList.get(50);
        println("seek: "+Arrays.toString(position));
        iterator.seek(position);
        //HgStoreTestUtil.println(iterator);
        Assert.assertEquals(50, HgStoreTestUtil.amountOf(iterator));*/

        iterator = session.scanIterator(tableName, 100);

        byte[] position = positionList.get(50);
        HgStoreTestUtil.println("seek: " + Arrays.toString(position));
        iterator.seek(position);
        HgStoreTestUtil.println(iterator);
        //Assert.assertEquals(50, HgStoreTestUtil.amountOf(iterator));

    }

    // @Test
    public void truncate() {
        HgStoreTestUtil.println("--- test truncate ---");
        String graph = "graph_truncate";
        String tableName = "UNIT_TRUNCATE_1";
        String keyName = "KEY_TRUNCATE";

        HgStoreSession session = getStoreSession(graph);
        HgStoreTestUtil.batchPut(session, tableName, keyName, 100);
        Assert.assertEquals(100, HgStoreTestUtil.amountOf(session.scanIterator(tableName)));

        String tableName2 = "UNIT_TRUNCATE_2";
        HgStoreTestUtil.batchPut(session, tableName2, keyName, 100);
        Assert.assertEquals(100, HgStoreTestUtil.amountOf(session.scanIterator(tableName2)));


        session.truncate();
        Assert.assertEquals(0, HgStoreTestUtil.amountOf(session.scanIterator(tableName)));
        Assert.assertEquals(0, HgStoreTestUtil.amountOf(session.scanIterator(tableName2)));
    }

    //// @Test
    public void scanIteratorHuge() {

        /*************** test no limit, with 10 millions **************/
        String tableName = "UNIT_HUGE";
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
            if (count % (amount / 10) == 0) {
                HgStoreTestUtil.println(entry);
            }
        }

        Assert.assertEquals(amount, count);
    }


    // @Test
    public void scanTable() {
        HgStoreSession session = getStoreSession("DEFAULT/hg1/g");
        HgStoreTestUtil.println(session.scanIterator("g+v", 10));


    }

    // @Test
    public void testDelGraph() {
        HgStoreSession session = getStoreSession();
        session.deleteGraph(HgStoreTestUtil.GRAPH_NAME);
    }

    // @Test
    public void benchmark_scanBatch() {
        HgStoreTestUtil.println("--- Benchmark scanBatch ---");
        String tableName = "Benchmark_SCAN_BATCH";
        String keyName = "SCAN-BATCH";
        int keyAmt = 30001;

        HgStoreSession session = getStoreSession();

        /*if (amountOf(session.scanIterator(tableName, toAllPartitionKey(keyName), 1)) < 1) {
            batchPut(session, tableName, keyName, keyAmt);
        }*/
        HgStoreTestUtil.println("-- Starting scan --");
        MetricX metrics = MetricX.ofStart();
        //HgStoreTestUtil.println(session.scanIterator(tableName));
        List<HgKvIterator<HgKvEntry>> iterators = session.scanBatch(HgScanQuery.tableOf(tableName));
        Assert.assertEquals(keyAmt, HgStoreTestUtil.amountIn(iterators));
        metrics.end();
        log.info("*************************************************");
        log.info("***********  Batch Scan Completed  **************");
        log.info("Total : {} (sec)", metrics.past() / 1000);
        log.info("  Sum : {} (sec)", MetricX.getIteratorWait() / 1000);
        log.info("  Amt : {} (scans).", MetricX.getIteratorCount());
        log.info("  Avg : {} (ms)", MetricX.getIteratorWaitAvg());
        log.info("  Max : {} (ms)", MetricX.getIteratorWaitMax());
        log.info(" Fail : {} (times)", metrics.getFailureCount());
        log.info(" Page : {} (KVs)", HgStoreClientConfig.of().getNetKvScannerPageSize());
        log.info("Iters : {}", iterators.size());
        log.info("*************************************************");

    }

    @Test
    public void benchmark_scanBatch2() throws IOException {
        HgStoreTestUtil.println("--- Benchmark scanBatch2 ---");
        //String tableName = TABLE_NAME;
        HgStoreSession session = getStoreSession();
        String keyName = "SCAN-BATCH";
        int keyAmt = 3000;

        Map<HgOwnerKey, byte[]> data =
                HgStoreTestUtil.batchPut(session, HgStoreTestUtil.TABLE_NAME, keyName, keyAmt,
                                         //(key) -> toOwnerKey(key.substring(0, keyName.length()
                                         // + 2), key)
                                         (key) -> HgStoreTestUtil.toOwnerKey(0, key)
                );

        HgStoreTestUtil.batchPut(session, HgStoreTestUtil.TABLE_NAME2, keyName, keyAmt,
                                 //(key) -> toOwnerKey(key.substring(0, keyName.length() + 2), key)
                                 (key) -> HgStoreTestUtil.toOwnerKey(0, key)
        );

        MetricX metrics = MetricX.ofStart();
        long t = System.currentTimeMillis();
        int count = 0;
        String queryTable = null;
        KvCloseableIterator<HgKvIterator<HgKvEntry>> iterators = null;
        List<HgOwnerKey> queryKeys = null;
        List<HgOwnerKey> keys = new ArrayList<>();
        data.forEach((k, v) -> keys.add(k));

        List<HgOwnerKey> prefixKeys = new ArrayList<>();
        prefixKeys.add(HgStoreTestUtil.toOwnerKey(0, keyName + "-0"));
        prefixKeys.add(HgStoreTestUtil.toOwnerKey(0, keyName + "-1"));
        prefixKeys.add(HgStoreTestUtil.toOwnerKey(0, keyName + "-2"));
        prefixKeys.add(HgStoreTestUtil.toOwnerKey(0, keyName + "-3"));

        HgStoreTestUtil.println("-- test every key, one table --");
        count = 0;
        queryTable = HgStoreTestUtil.TABLE_NAME;
        queryKeys = keys;
        iterators = session.scanBatch2(
                HgScanQuery.prefixIteratorOf(queryTable, queryKeys.iterator(),
                                             ScanOrderType.ORDER_NONE)
                           .builder()
                           .setScanType(0x40)
                           .build());
        while (iterators.hasNext()) {
            HgKvIterator<HgKvEntry> iterator = iterators.next();
            count += HgStoreTestUtil.amountOf(iterator);
        }
        iterators.close();
        Assert.assertEquals(keyAmt, count);
        log.info(" size is {}", count);

        HgStoreTestUtil.println("-- test prefix key, one table --");
        count = 0;
        queryTable = HgStoreTestUtil.TABLE_NAME;
        queryKeys = prefixKeys;
        iterators = session.scanBatch2(
                HgScanQuery.prefixIteratorOf(queryTable, queryKeys.iterator(),
                                             ScanOrderType.ORDER_STRICT)
                           .builder()
                           .setScanType(0x40)
                           .build());
        while (iterators.hasNext()) {
            HgKvIterator<HgKvEntry> iterator = iterators.next();
            count += HgStoreTestUtil.amountOf(iterator);
        }
        iterators.close();
        Assert.assertEquals(keyAmt, count);
        log.info(" size is {}", count);

        HgStoreTestUtil.println("-- test prefix key, two table --");
        count = 0;
        queryTable = HgStoreTestUtil.TABLE_NAME + "," + HgStoreTestUtil.TABLE_NAME2;
        queryKeys = prefixKeys;
        iterators = session.scanBatch2(
                HgScanQuery.prefixIteratorOf(queryTable, queryKeys.iterator(),
                                             ScanOrderType.ORDER_NONE)
                           .builder()
                           .setScanType(0x40)
                           .build());
        while (iterators.hasNext()) {
            HgKvIterator<HgKvEntry> iterator = iterators.next();
            count += HgStoreTestUtil.amountOf(iterator);
        }
        iterators.close();
        Assert.assertEquals(keyAmt * 2, count);
        log.info(" size is {}", count);

        HgStoreTestUtil.println("-- test prefix key, two table, perKeyMax --");
        count = 0;
        queryTable = HgStoreTestUtil.TABLE_NAME + "," + HgStoreTestUtil.TABLE_NAME2;
        queryKeys = prefixKeys;
        queryKeys.remove(queryKeys.size() - 1);//remove the last one.
        iterators = session.scanBatch2(
                HgScanQuery.prefixIteratorOf(queryTable, queryKeys.iterator(),
                                             ScanOrderType.ORDER_NONE)
                           .builder()
                           .setScanType(0x40)
                           .setPerKeyMax(10)
                           .build());
        while (iterators.hasNext()) {
            HgKvIterator<HgKvEntry> iterator = iterators.next();
            count += HgStoreTestUtil.amountOf(iterator);
        }
        iterators.close();
        Assert.assertEquals(queryKeys.size() * 10, count);
        log.info(" size is {}", count);

        keys.clear();

        log.info("time is {}", System.currentTimeMillis() - t);
        metrics.end();
        log.info("*************************************************");
        log.info("***********  Batch Scan Completed  **************");
        log.info("Total : {} (sec)", metrics.past() / 1000);
        log.info("  Sum : {} (sec)", MetricX.getIteratorWait() / 1000);
        log.info("  Amt : {} (scans).", MetricX.getIteratorCount());
        log.info("  Avg : {} (ms)", MetricX.getIteratorWaitAvg());
        log.info("  Max : {} (ms)", MetricX.getIteratorWaitMax());
        log.info(" Fail : {} (times)", metrics.getFailureCount());
        log.info(" Page : {} (KVs)", HgStoreClientConfig.of().getNetKvScannerPageSize());
        log.info(" size is {}", count);
        log.info("*************************************************");


    }

    // @Test
    public void benchmark_scanBatch_SkipDegree() throws IOException {
        HgStoreTestUtil.println("--- Benchmark scanBatch2 1Owner---");
        String tableName = HgStoreTestUtil.TABLE_NAME;
        HgStoreSession session = getStoreSession();
        String keyName = "SCAN-BATCH";
        int keyAmt = 300000;
        byte[] owner = "Owner".getBytes();
        Map<HgOwnerKey, byte[]> data =
                HgStoreTestUtil.batchPut(session, tableName, keyName, keyAmt, key -> {
                    return HgStoreTestUtil.toOwnerKey(owner, key);
                });
        HgStoreTestUtil.println("-- Starting scan --");
        MetricX metrics = MetricX.ofStart();
        //HgStoreTestUtil.println(session.scanIterator(tableName));
        long t = System.currentTimeMillis();
        int count = 0;
        {
            List<HgOwnerKey> keys = new ArrayList<>();
            keys.add(HgStoreTestUtil.toOwnerKey(owner, keyName));

            KvCloseableIterator<HgKvIterator<HgKvEntry>> iterators =
                    session.scanBatch2(HgScanQuery.prefixIteratorOf(tableName, keys.iterator(),
                                                                    ScanOrderType.ORDER_NONE)
                                                  .builder().setScanType(0x40).setSkipDegree(1)
                                                  .build());
            //KvCloseableIterator<HgKvIterator<Kv>> iterators = session.scanBatch2(HgScanQuery
            // .tableOf(tableName));
            while (iterators.hasNext()) {
                HgKvIterator<HgKvEntry> iterator = iterators.next();
//            while (iterator.hasNext()){
//                System.out.println(new String(iterator.next().key()));
//            }
                count += HgStoreTestUtil.amountOf(iterator);
                //    log.info("{} - {}", new String(iterator.key()), iterator.value());
            }
            keys.clear();
            iterators.close();
            iterators = null;
        }
        log.info("time is {}", System.currentTimeMillis() - t);
        metrics.end();
        log.info("*************************************************");
        log.info("***********  Batch Scan Completed  **************");
        log.info("Total : {} (sec)", metrics.past() / 1000);
        log.info("  Sum : {} (sec)", MetricX.getIteratorWait() / 1000);
        log.info("  Amt : {} (scans).", MetricX.getIteratorCount());
        log.info("  Avg : {} (ms)", MetricX.getIteratorWaitAvg());
        log.info("  Max : {} (ms)", MetricX.getIteratorWaitMax());
        log.info(" Fail : {} (times)", metrics.getFailureCount());
        log.info(" Page : {} (KVs)", HgStoreClientConfig.of().getNetKvScannerPageSize());
        log.info(" size is {}", count);
        log.info("*************************************************");


    }
}