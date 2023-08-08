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

import static org.apache.hugegraph.store.client.util.HgStoreClientConst.ALL_PARTITION_OWNER;
import static org.apache.hugegraph.store.client.util.HgStoreClientConst.EMPTY_BYTES;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.hugegraph.pd.common.PartitionUtils;
import org.apache.hugegraph.store.client.HgStoreNodeManager;
import org.apache.hugegraph.store.client.util.ExecutorPool;
import org.apache.hugegraph.store.client.util.HgStoreClientConfig;
import org.apache.hugegraph.store.client.util.HgStoreClientConst;
import org.apache.hugegraph.store.client.util.MetricX;
import org.apache.hugegraph.store.util.HgStoreTestUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HgSessionManagerTest {
    private static final Map<Integer, Long> leaderMap = new ConcurrentHashMap<>();
    private static final Map<Long, String> storeMap = new ConcurrentHashMap<>();

    private static final ExecutorService pool = Executors.newFixedThreadPool(100,
                                                                             ExecutorPool.newThreadFactory(
                                                                                     "unit-test"));

    private static final int partitionCount = 10;
    // 需要与store的application.yml的fake-pd.partition-count保持一致

    //private static String[] storeAddress = {"127.0.0.1:8500"};
    private static final String[] storeAddress =
            {"127.0.0.1:8501", "127.0.0.1:8502", "127.0.0.1:8503"};

    private static final int PARTITION_LENGTH = getPartitionLength();

    private static int getPartitionLength() {
        return PartitionUtils.MAX_VALUE /
               (partitionCount == 0 ? storeAddress.length : partitionCount) + 1;
    }

    @BeforeClass
    public static void init() {
        for (String address : storeAddress) {
            storeMap.put((long) address.hashCode(), address);
        }
        for (int i = 0; i < partitionCount; i++) {
            leaderMap.put(i, storeMap.keySet().iterator().next());
        }

        HgStoreNodeManager nodeManager = HgStoreNodeManager.getInstance();
        nodeManager.setNodePartitioner((builder, graphName, startKey, endKey) -> {
            int startCode = PartitionUtils.calcHashcode(startKey);
            int endCode = PartitionUtils.calcHashcode(endKey);
            if (ALL_PARTITION_OWNER == startKey) {
                storeMap.forEach((k, v) -> {
                    builder.add(k, -1);
                });
            } else if (endKey == HgStoreClientConst.EMPTY_BYTES || startKey == endKey ||
                       Arrays.equals(startKey, endKey)) {
                //log.info("leader-> {}",leaderMap.get(startCode / PARTITION_LENGTH));
                builder.add(leaderMap.get(startCode / PARTITION_LENGTH), startCode);
            } else {
                Assert.fail("OwnerKey转成HashCode后已经无序了， 按照OwnerKey范围查询没意义");
                builder.add(leaderMap.get(startCode / PARTITION_LENGTH), startCode);
                builder.add(leaderMap.get(endCode / PARTITION_LENGTH), endCode);
            }
            return 0;
        });
        nodeManager.setNodeProvider((graphName, nodeId) -> {
            //   System.out.println("HgStoreNodeProvider apply " + graphName + " " + nodeId +
            //   " " + storeMap.get(nodeId));
            return nodeManager.getNodeBuilder().setNodeId(nodeId)
                              .setAddress(storeMap.get(nodeId)).build();
        });
        nodeManager.setNodeNotifier((graphName, storeNotice) -> {
            System.out.println("recv node notifier " + storeNotice);
            if (storeNotice.getPartitionLeaders().size() > 0) {
                leaderMap.putAll(storeNotice.getPartitionLeaders());
                System.out.println("leader changed ");
                leaderMap.forEach((k, v) -> {
                    System.out.print("   " + k + " " + v + ",");
                });
                System.out.println();
            }
            return 0;
        });
    }

    protected static HgStoreSession getStoreSession() {
        return HgSessionManager.getInstance().openSession(HgStoreTestUtil.GRAPH_NAME);
    }

    protected static HgStoreSession getStoreSession(String graph) {
        return HgSessionManager.getInstance().openSession(graph);
    }

    @Test
    public void put_get() {
        System.out.println("--- test put & get ---");

        HgStoreSession session = getStoreSession();

        HgOwnerKey key = HgStoreTestUtil.toOwnerKey("FOR-PUT-KEY");
        //HgOwnerKey key = toAllNodeKey("FOR-PUT-KEY");
        byte[] value = HgStoreTestUtil.toBytes("FOR-PUT-VALUE");

        Assert.assertTrue(session.put(HgStoreTestUtil.TABLE_NAME, key, value));
        Assert.assertEquals(HgStoreTestUtil.toStr(value), HgStoreTestUtil.toStr(session.get(
                HgStoreTestUtil.TABLE_NAME, key)));
    }

    @Test
    public void batchPrefix() {
        System.out.println("--- test batchGet ---");
        HgStoreSession session = getStoreSession();
        String keyPrefix = "UNIT-BATCH-GET";

        Map<HgOwnerKey, byte[]> map = HgStoreTestUtil.batchPut(session, keyPrefix);
        List<HgOwnerKey> keyList =
                map.entrySet().stream().map(e -> e.getKey()).collect(Collectors.toList());

        Map<String, byte[]> keyMap = new HashMap<>();
        map.entrySet().forEach(e -> {
            keyMap.put(Arrays.toString(e.getKey().getKey()), e.getValue());
        });

        //printOwner(keyList);
        HgKvIterator<HgKvEntry> iterator = session.batchPrefix(HgStoreTestUtil.TABLE_NAME, keyList);
        int amount = HgStoreTestUtil.println(iterator);
        Assert.assertEquals(amount, keyList.size());
/*

        println("--- batch-get result ---");
        iterator = session.batchGet(TABLE_NAME, keyList);
        while(iterator.hasNext()){
            HgKvEntry kv=iterator.next();
            Assert.assertEquals(true,keyMap.containsKey(Arrays.toString(kv.key())));
        }
*/

    }

    @Test
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

    @Test
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
                                         key -> HgStoreTestUtil.toOwnerKey(owner, key)
                );

        HgOwnerKey startKey = HgStoreTestUtil.toOwnerKey(owner, rangePrefix + "-00");
        HgOwnerKey endKey = HgStoreTestUtil.toOwnerKey(owner, rangePrefix + "-05");

        Assert.assertTrue(session.deleteRange(HgStoreTestUtil.TABLE_NAME, startKey, endKey));

        HgStoreTestUtil.println("- after delete range from ["
                                + HgStoreTestUtil.toStr(startKey.getKey()) + "] to ["
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

    // @Test
    public void scan() {
        HgStoreTestUtil.println("--- test scanIterator ---");
        String tableName = "UNIT_SCAN_ITERATOR";
        String keyName = "SCAN-ITER";
        int keyAmt = 100;

        HgStoreSession session = getStoreSession();
        if (HgStoreTestUtil.amountOf(
                session.scanIterator(tableName, HgStoreTestUtil.toAllPartitionKey(keyName), 10)) <
            10) {
            HgStoreTestUtil.batchPut(session, tableName, keyName, keyAmt);
        }
        int count = 0;
        int limit = 0;
        int max = 99999;
        HgKvIterator<HgKvEntry> iterator = null;

        //    println("-- test scan all --");
        iterator = session.scanIterator(tableName);
        count = 0;
        while (iterator.hasNext()) {
            count++;
            HgKvEntry entry = iterator.next();
            if (count % 10 == 0) {
                //            println(entry);
            }
            if (count >= max) break;
        }
        iterator.close();
        Assert.assertEquals(keyAmt, count);

        //     println("-- test 0 element --");
        iterator = session.scanIterator(tableName, HgStoreTestUtil.toAllPartitionKey("__SCAN-001"),
                                        HgStoreTestUtil.toAllPartitionKey("__SCAN-100"), 0);
        Assert.assertFalse(iterator.hasNext());
        try {
            iterator.next();
            Assert.fail();
        } catch (Throwable t) {
            //         println("-- test NoSuchElementException --");
            Assert.assertTrue(t instanceof NoSuchElementException);
        }
        iterator.close();

        //     println("-- test limit 1 to 10 --");
        for (int i = 1; i <= 10; i++) {
            //        println("- limit " + i + " -");
            limit = i;
            iterator = session.scanIterator(tableName,
                                            HgStoreTestUtil.toAllPartitionKey(keyName + "-0"),
                                            HgStoreTestUtil.toAllPartitionKey(keyName + "-1"),
                                            limit);
            count = 0;
            while (iterator.hasNext()) {
                count++;
                HgKvEntry entry = iterator.next();
                //            println(entry);
            }
            iterator.close();
            Assert.assertEquals(limit, count);
        }

        //      println("-- test limit 1 to 10 not enough --");
        for (int i = 1; i <= 10; i++) {
            //        println("- limit " + i + " -");
            limit = i;
            iterator = session.scanIterator(tableName
                    , HgStoreTestUtil.toAllPartitionKey(keyName + "-001")
                    , HgStoreTestUtil.toAllPartitionKey(keyName + "-005"), limit);
            count = 0;
            while (iterator.hasNext()) {
                count++;
                HgKvEntry entry = iterator.next();
                //               println(entry);
            }
            iterator.close();
            if (i <= 5) {
                Assert.assertEquals(limit, count);
            } else {
                Assert.assertEquals(5, count);
            }

        }

        //     println("-- test limit 0 (no limit) --");
        limit = 0;
        iterator =
                session.scanIterator(tableName, HgStoreTestUtil.toAllPartitionKey(keyName + "-0"),
                                     HgStoreTestUtil.toAllPartitionKey(keyName + "-2"), limit);

        count = 0;
        while (iterator.hasNext()) {
            count++;
            HgKvEntry entry = iterator.next();
            if (count % 10 == 0) {
                //            println(entry);
            }
            if (count >= max) break;
        }
        iterator.close();
        Assert.assertEquals(keyAmt, count);

        //     println("-- test scan prefix --");
        iterator =
                session.scanIterator(tableName, HgStoreTestUtil.toAllPartitionKey(keyName + "-"));
        count = 0;
        while (iterator.hasNext()) {
            count++;
            HgKvEntry entry = iterator.next();
            if (count % 10 == 0) {
                //             println(entry);
            }
            if (count >= max) break;
        }
        iterator.close();
        Assert.assertEquals(keyAmt, count);

        //      println("-- test scan via hash code --");
        iterator = session.scanIterator(tableName, 0, 65535, HgKvStore.SCAN_HASHCODE, EMPTY_BYTES);
        count = 0;

        while (iterator.hasNext()) {
            count++;
            HgKvEntry entry = iterator.next();
            if (count % 10 == 0) {
                //               println(entry);
            }
            if (count >= max) break;
        }
        iterator.close();
        Assert.assertEquals(keyAmt, count);

        //      println("-- test range limit scan type --");
        iterator = session.scanIterator(tableName
                , HgStoreTestUtil.toAllPartitionKey("WWWWWWW")
                , HgStoreClientConst.EMPTY_OWNER_KEY
                , 0, HgKvStore.SCAN_ANY, EMPTY_BYTES
        );
        iterator.close();
        //      println("-- test range limit scan type -session.scanIterator over--");
        Assert.assertEquals(0, HgStoreTestUtil.amountOf(iterator));
        //      println("-- test range limit scan type -session.assertEquals over--");

        //      println("-- test range limit scan type -100");
        iterator = session.scanIterator(tableName
                , HgStoreTestUtil.toAllPartitionKey(keyName + "-100")
                , HgStoreClientConst.EMPTY_OWNER_KEY
                , 0, HgKvStore.SCAN_ANY, EMPTY_BYTES
        );

        //      println("-- test range limit scan type -100 over");
        Assert.assertEquals(1, HgStoreTestUtil.amountOf(iterator));
        //       println("-- test range limit scan type -100 assertEquals 1");

        //      println("-- test range limit scan type -51 ");
        iterator = session.scanIterator(tableName
                , HgStoreTestUtil.toAllPartitionKey(keyName + "-051")
                , HgStoreClientConst.EMPTY_OWNER_KEY
        );

        //      println("-- test range limit scan type -51 over");
        //HgStoreTestUtil.println(iterator);
        Assert.assertEquals(50, HgStoreTestUtil.amountOf(iterator));
        //       println("-- test range limit scan type -51 assertEquals");
        //TODO: add more...
        HgStoreTestUtil.println("--  test scanIterator end");
    }

    // @Test
    public void scan_close() {
        /*************** test scan close **************/
        HgStoreTestUtil.println("--- test scan close ---");
        String tableName = "UNIT_ITER_CLOSE";
        String keyName = "UNIT_ITER_CLOSE";

        int amount = 1_000_000;
        HgStoreSession session = getStoreSession();
        if (HgStoreTestUtil.amountOf(
                session.scanIterator(tableName, HgStoreTestUtil.toAllPartitionKey(keyName), 1)) <
            1) {
            HgStoreTestUtil.batchPut(session, tableName, keyName, amount);
        }

        HgKvIterator<HgKvEntry> iterator = null;

        /* server over, all -> close */
        iterator = session.scanIterator(tableName);
        Assert.assertEquals(amount, HgStoreTestUtil.amountOf(iterator));

        /* server over, one page, all -> close */
        iterator = session.scanIterator(tableName, 100_000);
        Assert.assertEquals(100_000, HgStoreTestUtil.amountOf(iterator));

        /* server over, two page, all -> close */
        iterator = session.scanIterator(tableName, 200_000);
        Assert.assertEquals(200_000, HgStoreTestUtil.amountOf(iterator));

        /* server not over, enough -> close */
        iterator = session.scanIterator(tableName);
        iterator.next();
        iterator.close();

        /* server not over, one page, enough -> close */
        iterator = session.scanIterator(tableName, 100_000);
        iterator.next();
        iterator.close();

        /* server not over, two page, enough -> close */
        iterator = session.scanIterator(tableName, 200_000);
        iterator.next();
        iterator.close();

        /* server not over, enough -> close */
        iterator = session.scanIterator(tableName);
        iterator.next();
        iterator.close();
        iterator = session.scanIterator(tableName);
        for (int i = 0; iterator.hasNext() && i < 500_000; i++) {
            iterator.next();
        }
        iterator.close();

        /* server timeout, enough -> close */
        iterator = session.scanIterator(tableName);
        iterator.next();
        //    HgStoreTestUtil.sleeping(11000);

        try {
            HgStoreTestUtil.amountOf(iterator);
        } catch (Throwable t) {
            HgStoreTestUtil.println("-- passed server waiting timeout --");
        }

        iterator.close();

    }

    // @Test
    public void paging() {
        HgStoreTestUtil.println("--- test scanIterator_range ---");
        String graph = "UNIT/paging";
        String tableName = "UNIT_SCAN_PAGING";
        String keyName = "SCAN-PAGING";
        int keyAmt = 100;
        HgStoreSession session = getStoreSession(graph);

        if (HgStoreTestUtil.amountOf(
                session.scanIterator(tableName, HgStoreTestUtil.toAllPartitionKey(keyName), 1)) <
            1) {
            HgStoreTestUtil.batchPut(session, tableName, keyName, keyAmt);
        }

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


        iterator = session.scanIterator(tableName
                , HgStoreTestUtil.toAllPartitionKey(keyName + "-000")
                , HgStoreClientConst.EMPTY_OWNER_KEY
                , 0, HgKvStore.SCAN_ANY, EMPTY_BYTES
        );

        byte[] position = positionList.get(50);
        HgStoreTestUtil.println("seek: " + Arrays.toString(position));
        iterator.seek(position);
        //println("amt after seek: "+HgStoreTestUtil.println(iterator));
        Assert.assertEquals(50, HgStoreTestUtil.amountOf(iterator));

    }

    @Test
    public void scanBatch() {
        HgStoreTestUtil.println("--- test scanBatch ---");
        String tableName = "UNIT_SCAN_BATCH_1";
        String keyName = "SCAN-BATCH";
        int keyAmt = 10_000;

        HgStoreSession session = getStoreSession();

        if (HgStoreTestUtil.amountOf(
                session.scanIterator(tableName, HgStoreTestUtil.toAllPartitionKey(keyName),
                                     keyAmt)) < keyAmt) {
            HgStoreTestUtil.batchPut(session, tableName, keyName, keyAmt);
        }

        Assert.assertEquals(keyAmt, HgStoreTestUtil.amountOf(session.scanIterator(tableName)));

        List<HgKvIterator<HgKvEntry>> iterators = null;
        List<HgOwnerKey> prefixes = null;

        List<HgOwnerKey> startList = Arrays.asList(
                HgStoreTestUtil.toAllPartitionKey(keyName + "-001")
                , HgStoreTestUtil.toAllPartitionKey(keyName + "-003")
                , HgStoreTestUtil.toAllPartitionKey(keyName + "-005")
                , HgStoreTestUtil.toAllPartitionKey(keyName + "-007")
                , HgStoreTestUtil.toAllPartitionKey(keyName + "-009")
        );

        List<HgOwnerKey> endList = Arrays.asList(
                HgStoreTestUtil.toAllPartitionKey(keyName + "-002")
                , HgStoreTestUtil.toAllPartitionKey(keyName + "-004")
                , HgStoreTestUtil.toAllPartitionKey(keyName + "-006")
                , HgStoreTestUtil.toAllPartitionKey(keyName + "-008")
                , HgStoreTestUtil.toAllPartitionKey(keyName + "-010")
        );

        List<HgOwnerKey> prefixList = Arrays.asList(
                HgStoreTestUtil.toAllPartitionKey(keyName + "-001")
                , HgStoreTestUtil.toAllPartitionKey(keyName + "-002")
                , HgStoreTestUtil.toAllPartitionKey(keyName + "-003")
                , HgStoreTestUtil.toAllPartitionKey(keyName + "-004")
                , HgStoreTestUtil.toAllPartitionKey(keyName + "-005")
                , HgStoreTestUtil.toAllPartitionKey(keyName + "-006")
                , HgStoreTestUtil.toAllPartitionKey(keyName + "-007")
                , HgStoreTestUtil.toAllPartitionKey(keyName + "-008")
                , HgStoreTestUtil.toAllPartitionKey(keyName + "-009")
        );

        HgStoreTestUtil.println("-- test scan-batch all --");

        HgScanQuery scanQuery = HgScanQuery.tableOf(tableName);
        iterators = session.scanBatch(scanQuery);
        Assert.assertEquals(3, iterators.size());

        Assert.assertEquals(keyAmt, HgStoreTestUtil.println(iterators));

        HgStoreTestUtil.println("-- test scan-batch prefix --");


        iterators = session.scanBatch(
                HgScanQuery.prefixOf(tableName, prefixList)
        );
        Assert.assertEquals(3, iterators.size());
        Assert.assertEquals(900,
                            iterators.parallelStream().mapToInt(e -> HgStoreTestUtil.amountOf(e))
                                     .sum()
        );

        HgStoreTestUtil.println("-- test scan-batch range --");

        iterators = session.scanBatch(HgScanQuery.rangeOf(tableName, startList, endList));
        Assert.assertEquals(3, iterators.size());
        Assert.assertEquals(
                HgStoreTestUtil.amountOf(
                        session.scanIterator(tableName
                                , HgStoreTestUtil.toAllPartitionKey(keyName + "-001")
                                , HgStoreTestUtil.toAllPartitionKey(keyName + "-010")
                        )
                )
                ,
                iterators.parallelStream().mapToInt(e -> HgStoreTestUtil.amountOf(e)).sum()
        );

        HgStoreTestUtil.println("-- test scan-batch limit --");

        int limit = 1;
        iterators = session.scanBatch(
                HgScanQuery.rangeOf(tableName, startList, endList)
                           .builder()
                           .setLimit(limit)
                           .build()
        );

        //HgStoreTestUtil.println(iterators);
        Assert.assertEquals(iterators.size() * limit,
                            iterators.parallelStream().mapToInt(e -> HgStoreTestUtil.amountOf(e))
                                     .sum()
        );

        HgStoreTestUtil.println("-- test scan-batch multi-table --");
        if (HgStoreTestUtil.amountOf(
                session.scanIterator("g+oe", HgStoreTestUtil.toAllPartitionKey(keyName), keyAmt)) <
            keyAmt) {
            HgStoreTestUtil.batchPut(session, "g+oe", keyName, keyAmt);
            HgStoreTestUtil.batchPut(session, "g+ie", keyName, keyAmt);
        }

        prefixes = Collections.singletonList(HgStoreTestUtil.toAllPartitionKey(keyName));

        tableName = "g+oe,g+ie";
        iterators = session.scanBatch(
                HgScanQuery.prefixOf(tableName, prefixes)
        );

        //HgStoreTestUtil.println(iterators);
        Assert.assertEquals(keyAmt * 2, HgStoreTestUtil.amountIn(iterators));
        iterators = session.scanBatch(
                HgScanQuery.prefixOf(tableName, prefixList)
        );
        Assert.assertEquals(900 * 2, HgStoreTestUtil.amountIn(iterators));

        HgStoreTestUtil.println("-- test scan-batch per-key-limit --");

        tableName = "PER_KEY_LIMIT_TABLE";
        keyName = "PER_KEY_LIMIT";
        if (HgStoreTestUtil.amountOf(
                session.scanIterator(tableName, HgStoreTestUtil.toAllPartitionKey(keyName),
                                     keyAmt)) < keyAmt) {
            HgStoreTestUtil.batchPut(session, tableName, keyName, keyAmt);
        }
        prefixes = Arrays.asList(
                HgStoreTestUtil.toAllPartitionKey(keyName + "-01")
                , HgStoreTestUtil.toAllPartitionKey(keyName + "-02")
                , HgStoreTestUtil.toAllPartitionKey(keyName + "-03")
                , HgStoreTestUtil.toAllPartitionKey(keyName + "-04")
                , HgStoreTestUtil.toAllPartitionKey(keyName + "-05")
        );

        iterators = session.scanBatch(
                HgScanQuery.prefixOf(tableName, prefixes).builder().setPerKeyLimit(1).build()
        );
        //HgStoreTestUtil.println(iterators);
        Assert.assertEquals(prefixes.size() * 3, HgStoreTestUtil.amountIn(iterators));

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

    // @Test
    public void handle_table() {
        HgStoreTestUtil.println("--- test table ---");

        String tableName = "UNIT_TABLE_" + System.currentTimeMillis();
        HgOwnerKey key = HgStoreTestUtil.toOwnerKey("FOR-PUT-KEY");
        byte[] value = HgStoreTestUtil.toBytes("FOR-PUT-VALUE");
        HgStoreSession session = getStoreSession();

        HgStoreTestUtil.println("-- test createTable --");
        session.createTable(tableName);
        Assert.assertTrue(session.existsTable(tableName));

        HgStoreTestUtil.println("-- test deleteTable --");
        session.put(tableName, key, value);
        Assert.assertEquals(HgStoreTestUtil.toStr(value),
                            HgStoreTestUtil.toStr(session.get(tableName, key)));
        session.deleteTable(tableName);
        Assert.assertNotEquals(HgStoreTestUtil.toStr(value),
                               HgStoreTestUtil.toStr(session.get(tableName, key)));
        Assert.assertTrue(session.existsTable(tableName));


        HgStoreTestUtil.println("-- test dropTable --");
        Assert.assertTrue(session.dropTable(tableName));
        Assert.assertFalse(session.existsTable(tableName));


        HgStoreTestUtil.println("-- test existsTable --");
        Assert.assertFalse(session.existsTable(tableName));

    }

    // @Test
    public void tx() {
        HgStoreTestUtil.println("--- test tx ---");
        HgStoreSession session = getStoreSession();
        String tableName = "UNIT_TABLE_TX";
        String keyPrefix = "TX";
        int keyAmt = 100;

        HgStoreTestUtil.println("-- unsupported tx operation --");
        HgStoreTestUtil.println("- tx deleteTable -");
        HgStoreTestUtil.batchPut(session, tableName, keyPrefix, keyAmt);
        Assert.assertEquals(keyAmt, HgStoreTestUtil.amountOf(session.scanIterator(tableName)));
        session.beginTx();
        session.deleteTable(tableName);
        session.commit();
        Assert.assertEquals(0, HgStoreTestUtil.amountOf(session.scanIterator(tableName)));

        HgStoreTestUtil.println("- tx dropTable -");
        HgStoreTestUtil.batchPut(session, tableName, keyPrefix, keyAmt);
        Assert.assertEquals(keyAmt, HgStoreTestUtil.amountOf(session.scanIterator(tableName)));
        session.beginTx();
        session.dropTable(tableName);
        session.commit();
        Assert.assertFalse(session.existsTable(tableName));
        Assert.assertEquals(0, HgStoreTestUtil.amountOf(session.scanIterator(tableName)));

        HgStoreTestUtil.println("- tx truncate -");
        HgStoreTestUtil.batchPut(session, tableName, keyPrefix, keyAmt);
        Assert.assertEquals(keyAmt, HgStoreTestUtil.amountOf(session.scanIterator(tableName)));
        session.beginTx();
        session.truncate();
        session.commit();
        Assert.assertFalse(session.existsTable(tableName));
        Assert.assertEquals(0, HgStoreTestUtil.amountOf(session.scanIterator(tableName)));

        //TODO : add others
    }

    //// @Test
    public void scanIterator_WithNonePartition() {
        HgStoreTestUtil.println("--- test scanIterator with none partition ---");
        int count = 0;
        HgStoreSession session = getStoreSession();

        for (int i = 0; i < 100; i++) {
            HgKvIterator<HgKvEntry> iterator = session.scanIterator("XXXXXXXX");
            while (iterator.hasNext()) {
                count++;
                HgKvEntry entry = iterator.next();
                HgStoreTestUtil.println(entry);
            }
        }

        Assert.assertEquals(0, count);

    }

    //// @Test
    public void repeatedly_parallel_scan() {
        MetricX metrics = MetricX.ofStart();
        HgStoreTestUtil.repeatedly_test(100, () -> parallel_scan());
        metrics.end();

        log.info("*************************************************");
        log.info("*************  Scanning Completed  **************");
        log.info("Total: {} sec.", metrics.past() / 1000);
        log.info("Sum: {} sec.", MetricX.getIteratorWait() / 1000);
        log.info("Amt: {} scans.", MetricX.getIteratorCount());
        log.info("Avg: {} ms.", MetricX.getIteratorWaitAvg());
        log.info("Max: {} ms.", MetricX.getIteratorWaitMax());
        log.info("Fail: {} times.", metrics.getFailureCount());
        log.info("Page: {}", HgStoreClientConfig.of().getNetKvScannerPageSize());
        log.info("*************************************************");

        // runWaiting();
    }

    // @Test
    public void parallel_scan() {
        HgStoreTestUtil.println("--- test scanIterator in parallel ---");
        HgStoreTestUtil.parallel_test(100, () -> {
            this.scan();
        }, t -> t.printStackTrace());
    }

    //// @Test
    public void put_Benchmark() {
        /*************** Put Benchmark **************/
        String tableName = "UNIT_PUT_BENCHMARK";
        String keyPrefix = "PUT-BENCHMARK";
        int amount = 1_000_000;
        HgStoreSession session = getStoreSession();

        int length = String.valueOf(amount).length();

        session.beginTx();

        long start = System.currentTimeMillis();
        for (int i = 0; i < amount; i++) {
            HgOwnerKey key = HgStoreTestUtil.toOwnerKey(
                    keyPrefix + "-" + HgStoreTestUtil.padLeftZeros(String.valueOf(i), length));
            byte[] value = HgStoreTestUtil.toBytes(keyPrefix + "-V-" + i);

            session.put(tableName, key, value);

            if ((i + 1) % 100_000 == 0) {
                HgStoreTestUtil.println("---------- " + (i + 1) + " --------");
                HgStoreTestUtil.println(
                        "Preparing took: " + (System.currentTimeMillis() - start) + " ms.");
                session.commit();
                HgStoreTestUtil.println(
                        "Committing took: " + (System.currentTimeMillis() - start) + " ms.");
                start = System.currentTimeMillis();
                session.beginTx();
            }
        }

        if (session.isTx()) {
            session.commit();
        }

        Assert.assertEquals(amount, HgStoreTestUtil.amountOf(session.scanIterator(tableName)));


    }

    //// @Test
    public void put_Benchmark_Parallel() {
        int threadsAmount = 100;
        CountDownLatch countDownLatch = new CountDownLatch(threadsAmount);

        for (int j = 0; j < threadsAmount; j++) {
            for (int i = 0; i < threadsAmount; i++) {
                pool.submit(() -> {
                    this.put_Benchmark();
                    // this.scanIterator_WithNonePartition();
                    countDownLatch.countDown();
                });
            }
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    //// @Test
    public void parallel_scanBatch() {
        int threadsAmount = 50;
        CountDownLatch countDownLatch = new CountDownLatch(threadsAmount);

        for (int j = 0; j < threadsAmount; j++) {
            for (int i = 0; i < threadsAmount; i++) {
                pool.submit(() -> {
                    try {
                        this.scanBatch();
                    } catch (Throwable t) {
                        t.printStackTrace();
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    //// @Test
    public void benchmark_scanBatch() {
        HgStoreTestUtil.println("--- Benchmark scanBatch ---");
        String tableName = "Benchmark_SCAN_BATCH";
        String keyName = "SCAN-BATCH";
        int keyAmt = 10_000_000;

        HgStoreSession session = getStoreSession();

        if (HgStoreTestUtil.amountOf(
                session.scanIterator(tableName, HgStoreTestUtil.toAllPartitionKey(keyName), 1)) <
            1) {
            HgStoreTestUtil.batchPut(session, tableName, keyName, keyAmt);
        }
        HgStoreTestUtil.println("-- Starting scan --");
        //  while (true) {

        MetricX metrics = MetricX.ofStart();
        //HgStoreTestUtil.println(session.scanIterator(tableName));
        List<HgKvIterator<HgKvEntry>> iterators = session.scanBatch(HgScanQuery.tableOf(tableName));
        //HgStoreTestUtil.println(iterators);
        //Assert.assertEquals(keyAmt, HgStoreTestUtil.amountIn(iterators));
        HgStoreTestUtil.amountIn(iterators);
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
        HgStoreTestUtil.sleeping(100);

        //    }
    }

    //// @Test
    public void benchmark_scan() {
        /*************** test no limit, with 10 millions **************/
        String tableName = "UNIT_HUGE";
        String keyName = "SCAN-HUGE";
        int amount = 10_000_000;
        int max = 10_000_000;
        HgStoreSession session = getStoreSession();

        /*Initialization*/
        if (HgStoreTestUtil.amountOf(
                session.scanIterator(tableName, HgStoreTestUtil.toAllPartitionKey(keyName), 1)) <
            1) {
            HgStoreTestUtil.batchPut(session, tableName, keyName, amount);
        }

        MetricX metricX = MetricX.ofStart();

        int count = 0;
        HgKvIterator<HgKvEntry> iterator = session.scanIterator(tableName);
        //HgStoreTestUtil.println(iterator, e -> (e % (amount / 100) == 0));
        while (iterator.hasNext()) {
            count++;
            HgKvEntry entry = iterator.next();
            if (count % (amount / 10) == 0) {
                HgStoreTestUtil.println(entry);
            }
            if (count >= max) break;
        }

        metricX.end();

        log.info("*************************************************");
        log.info("*************  Benchmark Completed  *************");
        log.info("Keys: {}", count);
        log.info("Waiting: {} ms.", MetricX.getIteratorWait());
        log.info("Total: {} ms.", metricX.past());
        log.info("Iterator: [{}]", iterator.getClass().getSimpleName());
        log.info("Page: {}", HgStoreClientConfig.of().getNetKvScannerPageSize());
        log.info("*************************************************");

        Assert.assertEquals(amount, count);
    }


    //// @Test
    public void extreme_scan_close() {
        /*************** test close **************/
        String tableName = "UNIT_ITER_CLOSE_EXTREME";
        String keyName = "UNIT_ITER_CLOSE_EXTREME";
        int amount = 1_000_000;
        HgStoreSession session = getStoreSession();

        if (HgStoreTestUtil.amountOf(
                session.scanIterator(tableName, HgStoreTestUtil.toAllPartitionKey(keyName), 1)) <
            1) {
            HgStoreTestUtil.batchPut(session, tableName, keyName, amount);
        }
        HgKvIterator<HgKvEntry> iterator = null;
        /* server not over, enough, extreme -> close */
        for (int i = 0; i <= 10_000; i++) {
            iterator = session.scanIterator(tableName);
            iterator.next();
            iterator.close();
            HgStoreTestUtil.println("extreme loop: " + i);
        }
        HgStoreTestUtil.runWaiting();
    }

    //// @Test
    public void parallel_scan_close() {
        HgStoreTestUtil.parallel_test(10, () -> this.scan_close(), t -> t.printStackTrace());
    }

    //// @Test
    public void repeat_parallel_scan_close() {
        HgStoreTestUtil.repeatedly_test(1000, () -> this.parallel_scan_close());
        HgStoreTestUtil.runWaiting();
    }

    //// @Test
    public void parallel_huge_scan() {
        int threadsAmount = 3;
        CountDownLatch countDownLatch = new CountDownLatch(threadsAmount);
        ExecutorService poll = Executors.newFixedThreadPool(threadsAmount);

        for (int i = 0; i < threadsAmount; i++) {
            poll.submit(() -> {
                this.benchmark_scan();
                countDownLatch.countDown();
            });
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}