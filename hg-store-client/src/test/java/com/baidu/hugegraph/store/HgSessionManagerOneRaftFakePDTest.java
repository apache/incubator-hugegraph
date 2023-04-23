package com.baidu.hugegraph.store;

import com.baidu.hugegraph.pd.common.PartitionUtils;
import com.baidu.hugegraph.store.client.*;
import com.baidu.hugegraph.store.client.util.HgStoreClientConst;
import org.junit.Assert;
// import org.junit.BeforeClass;
// import org.junit.Test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.baidu.hugegraph.store.client.util.HgAssert.isInvalid;
import static com.baidu.hugegraph.store.client.util.HgStoreClientConst.ALL_PARTITION_OWNER;
import static com.baidu.hugegraph.store.client.util.HgStoreClientConst.EMPTY_BYTES;
import static com.baidu.hugegraph.store.util.HgStoreTestUtil.*;

/**
 * 使用fake-pd，支持raft的单元测试
 */
public class HgSessionManagerOneRaftFakePDTest {
    private static Map<Integer, Long> leaderMap = new ConcurrentHashMap<>();
    private static Map<Long, String> storeMap = new ConcurrentHashMap<>();

    private static int partitionCount = 3;  // 需要与store的application.yml的fake-pd.partition-count保持一致
    private static String[] storeAddress = {
            "127.0.0.1:8500"
    };

    // @BeforeClass
    public static void init() {
        for (String address : storeAddress) {
            storeMap.put((long) address.hashCode(), address);
        }
        for (int i = 0; i < partitionCount; i++)
            leaderMap.put(i, (long) storeMap.keySet().iterator().next());

        HgStoreNodeManager nodeManager = HgStoreNodeManager.getInstance();
        nodeManager.setNodePartitioner(new HgStoreNodePartitioner() {
            @Override
            public int partition(HgNodePartitionerBuilder builder, String graphName, byte[] startKey, byte[] endKey) {
                int startCode = PartitionUtils.calcHashcode(startKey);
                int endCode = PartitionUtils.calcHashcode(endKey);
                if (ALL_PARTITION_OWNER == startKey) {
                    storeMap.forEach((k,v)->{
                        builder.add(k, -1);
                    });
                } else if (endKey == HgStoreClientConst.EMPTY_BYTES || startKey == endKey || Arrays.equals(startKey, endKey)) {
                    builder.add(leaderMap.get(startCode % partitionCount), startCode);
                } else {
                    Assert.fail("OwnerKey转成HashCode后已经无序了， 按照OwnerKey范围查询没意义");
                    builder.add(leaderMap.get(startCode % partitionCount), startCode);
                    builder.add(leaderMap.get(endCode % partitionCount), endCode);
                }
                return 0;
            }
        });
        nodeManager.setNodeProvider(new HgStoreNodeProvider() {
            @Override
            public HgStoreNode apply(String graphName, Long nodeId) {
                System.out.println("HgStoreNodeProvider apply " + graphName + " " + nodeId + " " + storeMap.get(nodeId));
                return nodeManager.getNodeBuilder().setNodeId(nodeId)
                        .setAddress(storeMap.get(nodeId)).build();
            }
        });
        nodeManager.setNodeNotifier(new HgStoreNodeNotifier() {
            @Override
            public int notice(String graphName, HgStoreNotice storeNotice) {
                System.out.println("recv node notifier " + storeNotice);
                if ( storeNotice.getPartitionLeaders().size() > 0) {
                    leaderMap.putAll(storeNotice.getPartitionLeaders());
                    System.out.println("leader changed ");
                    leaderMap.forEach((k, v)->{
                        System.out.print("   " + k + " " + v + ",");
                    });
                    System.out.println();
                }
                return 0;
            }
        });
    }

    private static HgStoreSession getStoreSession() {
        return HgSessionManager.getInstance().openSession(GRAPH_NAME);
    }
    private static HgStoreSession getStoreSession(String graph) {
        return HgSessionManager.getInstance().openSession(graph);
    }
    // @Test
    public void put_get() {
        System.out.println("--- test put & get ---");

        HgStoreSession session = getStoreSession();

        HgOwnerKey key = toOwnerKey("FOR-PUT-KEY");
        //HgOwnerKey key = toAllNodeKey("FOR-PUT-KEY");
        byte[] value = toBytes("FOR-PUT-VALUE");

        Assert.assertTrue(session.put(TABLE_NAME, key, value));

        Assert.assertEquals(toStr(value), toStr(session.get(TABLE_NAME, key)));
    }

    // @Test
    public void put_get2() {
        System.out.println("--- test put & get 2---");

        String GRAPH_NAME = "default/hugegraph/g2";
        String TABLE_NAME = "put_get2";

        HgStoreSession session = getStoreSession(GRAPH_NAME);

        HgOwnerKey key = toOwnerKey("FOR-PUT-KEY");
        //HgOwnerKey key = toAllNodeKey("FOR-PUT-KEY");
        byte[] value = toBytes("FOR-PUT-VALUE");

        Assert.assertTrue(session.put(TABLE_NAME, key, value));

        Assert.assertEquals(toStr(value), toStr(session.get(TABLE_NAME, key)));

        HgKvIterator<HgKvEntry> iterator = session.scanIterator(TABLE_NAME);
        while (iterator.hasNext()){
            HgKvEntry entry = iterator.next();

            System.out.println(new String(entry.key()));
            Assert.assertEquals(toStr(key.getKey()), toStr(entry.key()));
        }

    }


    // @Test
    public void batchGet() {
        System.out.println("--- test batchGet ---");
        HgStoreSession session = getStoreSession();
        String keyPrefix = "BATCH-GET-UNIT";

        Map<HgOwnerKey, byte[]> map = batchPut(session, keyPrefix);
        List<HgOwnerKey> keyList = map.entrySet().stream().map(e -> e.getKey()).collect(Collectors.toList());

        //printOwner(keyList);
        List<HgKvEntry> resList = session.batchGetOwner(TABLE_NAME, keyList);

        Assert.assertFalse(isInvalid(resList));
        Assert.assertEquals(resList.size(), keyList.size());

       // println(list);
        println("--- batch-get result ---");
        Assert.assertTrue((resList.stream()
                .map(e -> map.containsKey(toOwnerKey(e.key()))).allMatch(Boolean::booleanValue))
        );

    }

    // @Test
    //CAUTION: ONLY FOR LONG！
    //注意：目前只可以对long类型value进行Merge操作。
    public void merge() {
        System.out.println("--- test merge (1+1=2)---");
        HgStoreSession session = getStoreSession();
        String mergeKey = "merge-key";
        HgOwnerKey key = toOwnerKey(mergeKey);
        byte[] value = toBytes(1l);

        System.out.println("- put " + mergeKey + ":1 -");
        session.put(TABLE_NAME, key, value);
        System.out.println("- merge " + mergeKey + ":1 -");
        session.merge(TABLE_NAME, key, value);
        long res = toLong(session.get(TABLE_NAME, key));
        System.out.printf("after merge " + mergeKey + "=%s%n", res);
        Assert.assertEquals(2l, res);

        String putStr = "19";
        session.put(TABLE_NAME, key, toBytes(putStr));
        byte[] b1 = session.get(TABLE_NAME, key);
        Assert.assertEquals(putStr, toStr(b1));
    }

    // @Test
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
        Assert.assertTrue(EMPTY_BYTES.equals(value));
    }

    // @Test
    public void deleteSingle() {
        System.out.println("--- test deleteSingle ---");
        HgStoreSession session = getStoreSession();

        String delKey = "del-single-key";
        String delValue = "del-single-value";
        HgOwnerKey key = toOwnerKey(delKey);
        byte[] value = toBytes(delValue);

        println("- put [" + delKey + "] = " + delValue);
        session.put(TABLE_NAME, key, value);

        value = session.get(TABLE_NAME, key);
        println("- before del, get [" + delKey + "] = " + toStr(value));
        Assert.assertEquals(delValue, toStr(value));

        println("- delete-single : [" + delKey + "]");
        session.deleteSingle(TABLE_NAME, key);
        value = session.get(TABLE_NAME, key);
        println("- after del, get [" + delKey + "] = " + toStr(value));
        Assert.assertEquals("", toStr(value));

    }

    // @Test
    public void deleteRange() {
        println("--- test deleteRange ---");
        HgStoreSession session = getStoreSession();

        String rangePrefix = "DEL-RANGE-KEY";
        String owner="batch-delete-owner";
        Map<HgOwnerKey, byte[]> map = batchPut(session,TABLE_NAME, rangePrefix, 10,key->{
            return toOwnerKey(owner,key);
        });

        HgOwnerKey startKey = toOwnerKey(owner,rangePrefix + "-00");
        HgOwnerKey endKey = toOwnerKey(owner,rangePrefix + "-05");

        Assert.assertTrue(session.deleteRange(TABLE_NAME, startKey, endKey));

        println("- after delete range from ["
                + toStr(startKey.getKey())
                + "] to ["
                + toStr(endKey.getKey()) + "]");

        for (int i = 0; i < 10; i++) {
            HgOwnerKey key = toOwnerKey(owner,rangePrefix + "-" + padLeftZeros(String.valueOf(i), 2));
            String value = toStr(session.get(TABLE_NAME, key));
            println("- get [" + toStr(key.getKey()) + "] = " + value);

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
        String owner="batch-delete-owner";
        Map<HgOwnerKey, byte[]> map = batchPut(session,TABLE_NAME, prefixStr, 10,key->{
            return toOwnerKey(owner,key);
        });

        //printOwner(map.entrySet().stream().map(e -> e.getKey()).collect(Collectors.toList()));

        HgOwnerKey prefix = toOwnerKey(owner,prefixStr);

        Assert.assertEquals(10, amountOf(session.scanIterator(TABLE_NAME, prefix)));
        session.deletePrefix(TABLE_NAME, prefix);
        Assert.assertEquals(0, amountOf(session.scanIterator(TABLE_NAME, prefix)));

        println("- after delete by prefix:[" + prefixStr + "]");

        for (int i = 0; i < 10; i++) {
            HgOwnerKey key = toOwnerKey(owner,prefixStr + toSuffix(i, 2));
            String value = toStr(session.get(TABLE_NAME, key));
            System.out.println("- get [" + toStr(key.getKey()) + "] = " + value);
            Assert.assertEquals("", value);
        }

    }

    // @Test
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
        iterator = session.scanIterator(tableName, toAllPartitionKey("__SCAN-001"), toAllPartitionKey("__SCAN-100"), 0);
        Assert.assertFalse(iterator.hasNext());
        try {
            iterator.next();
            Assert.assertTrue(false);
        } catch (Throwable t) {
            println("-- test NoSuchElementException --");
            Assert.assertTrue(NoSuchElementException.class.isInstance(t));
        }

        println("-- test limit 1 to 10 --");
        for (int i = 1; i <= 10; i++) {
            println("- limit " + i + " -");
            limit = i;
            iterator = session.scanIterator(tableName, toAllPartitionKey(keyName + "-0"), toAllPartitionKey(keyName + "-1"), limit);
            count = 0;
            while (iterator.hasNext()) {
                count++;
                HgKvEntry entry = iterator.next();
                println(entry);
            }
            Assert.assertEquals(limit, count);
        }

        println("-- test limit 1 to 10 not enough --");
        for (int i = 1; i <= 10; i++) {
            println("- limit " + i + " -");
            limit = i;
            iterator = session.scanIterator(tableName,
                    toAllPartitionKey(keyName + "-00001"), toAllPartitionKey(keyName + "-00005"), limit);
            count = 0;
            while (iterator.hasNext()) {
                count++;
                HgKvEntry entry = iterator.next();
                println(entry);
            }
            if (i <= 5) {
                Assert.assertEquals(limit, count);
            } else {
                Assert.assertEquals(5, count);
            }

        }

        println("-- test limit 0 (no limit) --");
        limit = 0;
        iterator = session.scanIterator(tableName, toAllPartitionKey(keyName + "-0"), toAllPartitionKey(keyName + "-1"), limit);

        count = 0;
        while (iterator.hasNext()) {
            count++;
            HgKvEntry entry = iterator.next();
            if (count % 1000 == 0) {
                println(entry);
            }
            if (count >= max) break;
        }
        Assert.assertEquals(10000, count);

        println("-- test scan all --");
        iterator = session.scanIterator(tableName);
        count = 0;
        while (iterator.hasNext()) {
            count++;
            HgKvEntry entry = iterator.next();
            if (count % 1000 == 0) {
                println(entry);
            }
            if (count >= max) break;
        }
        Assert.assertEquals(10000, count);

        println("-- test scan prefix --");
        iterator = session.scanIterator(tableName, toAllPartitionKey(keyName + "-01"));
        count = 0;
        while (iterator.hasNext()) {
            count++;
            HgKvEntry entry = iterator.next();
            if (count % 100 == 0) {
                println(entry);
            }
            if (count >= max) break;
        }
        Assert.assertEquals(1000, count);
    }



    // @Test
    public void truncate() {
        println("--- test truncate ---");
        String graph="graph_truncate";
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

    //// @Test
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
            if (count % (amount / 10) == 0) {
                println(entry);
            }
        }

        Assert.assertEquals(amount, count);
    }
}