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

import static org.apache.hugegraph.store.client.util.HgAssert.isInvalid;
import static org.apache.hugegraph.store.client.util.HgStoreClientConst.ALL_PARTITION_OWNER;
import static org.apache.hugegraph.store.client.util.HgStoreClientConst.EMPTY_BYTES;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.hugegraph.pd.common.PartitionUtils;
import org.apache.hugegraph.store.client.HgStoreNodeManager;
import org.apache.hugegraph.store.client.util.HgStoreClientConst;
import org.apache.hugegraph.store.util.HgStoreTestUtil;
import org.junit.Assert;

/**
 * 使用fake-pd，支持raft的单元测试
 */
public class HgSessionManagerOneRaftFakePDTest {
    private static final Map<Integer, Long> leaderMap = new ConcurrentHashMap<>();
    private static final Map<Long, String> storeMap = new ConcurrentHashMap<>();

    private static final int partitionCount = 3;
    // 需要与store的application.yml的fake-pd.partition-count保持一致
    private static final String[] storeAddress = {
            "127.0.0.1:8500"
    };

    // @BeforeClass
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
                builder.add(leaderMap.get(startCode % partitionCount), startCode);
            } else {
                Assert.fail("OwnerKey转成HashCode后已经无序了， 按照OwnerKey范围查询没意义");
                builder.add(leaderMap.get(startCode % partitionCount), startCode);
                builder.add(leaderMap.get(endCode % partitionCount), endCode);
            }
            return 0;
        });
        nodeManager.setNodeProvider((graphName, nodeId) -> {
            System.out.println("HgStoreNodeProvider apply " + graphName + " " + nodeId + " " +
                               storeMap.get(nodeId));
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

    private static HgStoreSession getStoreSession() {
        return HgSessionManager.getInstance().openSession(HgStoreTestUtil.GRAPH_NAME);
    }

    private static HgStoreSession getStoreSession(String graph) {
        return HgSessionManager.getInstance().openSession(graph);
    }

    // @Test
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

    // @Test
    public void put_get2() {
        System.out.println("--- test put & get 2---");

        String GRAPH_NAME = "default/hugegraph/g2";
        String TABLE_NAME = "put_get2";

        HgStoreSession session = getStoreSession(GRAPH_NAME);

        HgOwnerKey key = HgStoreTestUtil.toOwnerKey("FOR-PUT-KEY");
        //HgOwnerKey key = toAllNodeKey("FOR-PUT-KEY");
        byte[] value = HgStoreTestUtil.toBytes("FOR-PUT-VALUE");

        Assert.assertTrue(session.put(TABLE_NAME, key, value));

        Assert.assertEquals(HgStoreTestUtil.toStr(value),
                            HgStoreTestUtil.toStr(session.get(TABLE_NAME, key)));

        HgKvIterator<HgKvEntry> iterator = session.scanIterator(TABLE_NAME);
        while (iterator.hasNext()) {
            HgKvEntry entry = iterator.next();

            System.out.println(new String(entry.key()));
            Assert.assertEquals(HgStoreTestUtil.toStr(key.getKey()),
                                HgStoreTestUtil.toStr(entry.key()));
        }

    }


    // @Test
    public void batchGet() {
        System.out.println("--- test batchGet ---");
        HgStoreSession session = getStoreSession();
        String keyPrefix = "BATCH-GET-UNIT";

        Map<HgOwnerKey, byte[]> map = HgStoreTestUtil.batchPut(session, keyPrefix);
        List<HgOwnerKey> keyList =
                map.entrySet().stream().map(e -> e.getKey()).collect(Collectors.toList());

        //printOwner(keyList);
        List<HgKvEntry> resList = session.batchGetOwner(HgStoreTestUtil.TABLE_NAME, keyList);

        Assert.assertFalse(isInvalid(resList));
        Assert.assertEquals(resList.size(), keyList.size());

        // println(list);
        HgStoreTestUtil.println("--- batch-get result ---");
        Assert.assertTrue((resList.stream()
                                  .map(e -> map.containsKey(HgStoreTestUtil.toOwnerKey(e.key())))
                                  .allMatch(Boolean::booleanValue))
        );

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
}