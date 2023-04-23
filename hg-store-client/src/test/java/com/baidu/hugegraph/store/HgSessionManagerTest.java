package com.baidu.hugegraph.store;

import com.baidu.hugegraph.pd.common.PartitionUtils;
import com.baidu.hugegraph.store.client.*;
import com.baidu.hugegraph.store.client.util.ExecutorPool;
import com.baidu.hugegraph.store.client.util.HgStoreClientConfig;
import com.baidu.hugegraph.store.client.util.HgStoreClientConst;
import com.baidu.hugegraph.store.client.util.MetricX;
import com.baidu.hugegraph.store.util.HgStoreTestUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
// import org.junit.BeforeClass;
// import org.junit.Test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.baidu.hugegraph.store.client.util.HgAssert.isInvalid;
import static com.baidu.hugegraph.store.client.util.HgStoreClientConst.ALL_PARTITION_OWNER;
import static com.baidu.hugegraph.store.client.util.HgStoreClientConst.EMPTY_BYTES;
import static com.baidu.hugegraph.store.util.HgStoreTestUtil.*;
import static com.baidu.hugegraph.store.util.HgStoreTestUtil.println;

@Slf4j
public class HgSessionManagerTest {
    private static Map<Integer, Long> leaderMap = new ConcurrentHashMap<>();
    private static Map<Long, String> storeMap = new ConcurrentHashMap<>();

    private static ExecutorService pool = Executors.newFixedThreadPool(100,
                                                                       ExecutorPool.newThreadFactory("unit-test"));

    private static int partitionCount = 10;  // 需要与store的application.yml的fake-pd.partition-count保持一致

    //private static String[] storeAddress = {"127.0.0.1:8500"};
    private static String[] storeAddress = {"127.0.0.1:8501", "127.0.0.1:8502", "127.0.0.1:8503"};

    private static int PARTITION_LENGTH = getPartitionLength();

    private static int getPartitionLength() {
        return PartitionUtils.MAX_VALUE / (partitionCount == 0 ? storeAddress.length : partitionCount) + 1;
    }

     @BeforeClass
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
                    storeMap.forEach((k, v) -> {
                        builder.add(k, -1);
                    });
                } else if (endKey == HgStoreClientConst.EMPTY_BYTES || startKey == endKey || Arrays.equals(startKey, endKey)) {
                    //log.info("leader-> {}",leaderMap.get(startCode / PARTITION_LENGTH));
                    builder.add(leaderMap.get(startCode / PARTITION_LENGTH), startCode);
                } else {
                    Assert.fail("OwnerKey转成HashCode后已经无序了， 按照OwnerKey范围查询没意义");
                    builder.add(leaderMap.get(startCode / PARTITION_LENGTH), startCode);
                    builder.add(leaderMap.get(endCode / PARTITION_LENGTH), endCode);
                }
                return 0;
            }
        });
        nodeManager.setNodeProvider(new HgStoreNodeProvider() {
            @Override
            public HgStoreNode apply(String graphName, Long nodeId) {
                //   System.out.println("HgStoreNodeProvider apply " + graphName + " " + nodeId + " " + storeMap.get(nodeId));
                return nodeManager.getNodeBuilder().setNodeId(nodeId)
                        .setAddress(storeMap.get(nodeId)).build();
            }
        });
        nodeManager.setNodeNotifier(new HgStoreNodeNotifier() {
            @Override
            public int notice(String graphName, HgStoreNotice storeNotice) {
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
            }
        });
    }

    protected static HgStoreSession getStoreSession() {
        return HgSessionManager.getInstance().openSession(GRAPH_NAME);
    }

    protected static HgStoreSession getStoreSession(String graph) {
        return HgSessionManager.getInstance().openSession(graph);
    }

     @Test
    public void put_get() {
        System.out.println("--- test put & get ---");

        HgStoreSession session = getStoreSession();

        HgOwnerKey key = toOwnerKey("FOR-PUT-KEY");
        //HgOwnerKey key = toAllNodeKey("FOR-PUT-KEY");
        byte[] value = toBytes("FOR-PUT-VALUE");

        Assert.assertTrue(session.put(TABLE_NAME, key, value));
        Assert.assertEquals(toStr(value), toStr(session.get(TABLE_NAME, key)));
    }

    @Test
    public void batchPrefix() {
        System.out.println("--- test batchGet ---");
        HgStoreSession session = getStoreSession();
        String keyPrefix = "UNIT-BATCH-GET";

        Map<HgOwnerKey, byte[]> map = batchPut(session, keyPrefix);
        List<HgOwnerKey> keyList = map.entrySet().stream().map(e -> e.getKey()).collect(Collectors.toList());

        Map<String, byte[]> keyMap = new HashMap<>();
        map.entrySet().forEach(e -> {
            keyMap.put(Arrays.toString(e.getKey().getKey()), e.getValue());
        });

        //printOwner(keyList);
        HgKvIterator<HgKvEntry> iterator = session.batchPrefix(TABLE_NAME, keyList);
        int amount = println(iterator);
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
        String owner = "batch-delete-owner";
        Map<HgOwnerKey, byte[]> map = batchPut(session, TABLE_NAME, rangePrefix, 10,
                key -> toOwnerKey(owner, key)
        );

        HgOwnerKey startKey = toOwnerKey(owner, rangePrefix + "-00");
        HgOwnerKey endKey = toOwnerKey(owner, rangePrefix + "-05");

        Assert.assertTrue(session.deleteRange(TABLE_NAME, startKey, endKey));

        println("- after delete range from ["
                + toStr(startKey.getKey()) + "] to ["
                + toStr(endKey.getKey()) + "]");

        for (int i = 0; i < 10; i++) {
            HgOwnerKey key = toOwnerKey(owner, rangePrefix + "-" + padLeftZeros(String.valueOf(i), 2));
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
        String owner = "batch-delete-owner";
        Map<HgOwnerKey, byte[]> map = batchPut(session, TABLE_NAME, prefixStr, 10, key -> {
            return toOwnerKey(owner, key);
        });

        //printOwner(map.entrySet().stream().map(e -> e.getKey()).collect(Collectors.toList()));

        HgOwnerKey prefix = toOwnerKey(owner, prefixStr);

        Assert.assertEquals(10, amountOf(session.scanIterator(TABLE_NAME, prefix)));
        session.deletePrefix(TABLE_NAME, prefix);
        Assert.assertEquals(0, amountOf(session.scanIterator(TABLE_NAME, prefix)));

        println("- after delete by prefix:[" + prefixStr + "]");

        for (int i = 0; i < 10; i++) {
            HgOwnerKey key = toOwnerKey(owner, prefixStr + toSuffix(i, 2));
            String value = toStr(session.get(TABLE_NAME, key));
            System.out.println("- get [" + toStr(key.getKey()) + "] = " + value);
            Assert.assertEquals("", value);
        }

    }

    // @Test
    public void scan() {
        println("--- test scanIterator ---");
        String tableName = "UNIT_SCAN_ITERATOR";
        String keyName = "SCAN-ITER";
        int keyAmt = 100;

        HgStoreSession session = getStoreSession();
        if (amountOf(session.scanIterator(tableName, toAllPartitionKey(keyName), 10)) < 10) {
            batchPut(session, tableName, keyName, keyAmt);
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
        iterator = session.scanIterator(tableName, toAllPartitionKey("__SCAN-001"), toAllPartitionKey("__SCAN-100"), 0);
        Assert.assertFalse(iterator.hasNext());
        try {
            iterator.next();
            Assert.assertTrue(false);
        } catch (Throwable t) {
            //         println("-- test NoSuchElementException --");
            Assert.assertTrue(NoSuchElementException.class.isInstance(t));
        }
        iterator.close();

        //     println("-- test limit 1 to 10 --");
        for (int i = 1; i <= 10; i++) {
            //        println("- limit " + i + " -");
            limit = i;
            iterator = session.scanIterator(tableName, toAllPartitionKey(keyName + "-0"), toAllPartitionKey(keyName + "-1"), limit);
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
                    , toAllPartitionKey(keyName + "-001")
                    , toAllPartitionKey(keyName + "-005"), limit);
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
        iterator = session.scanIterator(tableName, toAllPartitionKey(keyName + "-0"), toAllPartitionKey(keyName + "-2"), limit);

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
        iterator = session.scanIterator(tableName, toAllPartitionKey(keyName + "-"));
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
                , toAllPartitionKey("WWWWWWW")
                , HgStoreClientConst.EMPTY_OWNER_KEY
                , 0, HgKvStore.SCAN_ANY, EMPTY_BYTES
        );
        iterator.close();
        //      println("-- test range limit scan type -session.scanIterator over--");
        Assert.assertEquals(0, HgStoreTestUtil.amountOf(iterator));
        //      println("-- test range limit scan type -session.assertEquals over--");

        //      println("-- test range limit scan type -100");
        iterator = session.scanIterator(tableName
                , toAllPartitionKey(keyName + "-100")
                , HgStoreClientConst.EMPTY_OWNER_KEY
                , 0, HgKvStore.SCAN_ANY, EMPTY_BYTES
        );

        //      println("-- test range limit scan type -100 over");
        Assert.assertEquals(1, HgStoreTestUtil.amountOf(iterator));
        //       println("-- test range limit scan type -100 assertEquals 1");

        //      println("-- test range limit scan type -51 ");
        iterator = session.scanIterator(tableName
                , toAllPartitionKey(keyName + "-051")
                , HgStoreClientConst.EMPTY_OWNER_KEY
        );

        //      println("-- test range limit scan type -51 over");
        //HgStoreTestUtil.println(iterator);
        Assert.assertEquals(50, HgStoreTestUtil.amountOf(iterator));
        //       println("-- test range limit scan type -51 assertEquals");
        //TODO: add more...
        println("--  test scanIterator end");
    }

    // @Test
    public void scan_close() {
        /*************** test scan close **************/
        println("--- test scan close ---");
        String tableName = "UNIT_ITER_CLOSE";
        String keyName = "UNIT_ITER_CLOSE";

        int amount = 1_000_000;
        HgStoreSession session = getStoreSession();
        if (amountOf(session.scanIterator(tableName, toAllPartitionKey(keyName), 1)) < 1) {
            batchPut(session, tableName, keyName, amount);
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
            println("-- passed server waiting timeout --");
        }

        iterator.close();

    }

    // @Test
    public void paging() {
        println("--- test scanIterator_range ---");
        String graph = "UNIT/paging";
        String tableName = "UNIT_SCAN_PAGING";
        String keyName = "SCAN-PAGING";
        int keyAmt = 100;
        HgStoreSession session = getStoreSession(graph);

        if (amountOf(session.scanIterator(tableName, toAllPartitionKey(keyName), 1)) < 1) {
            batchPut(session, tableName, keyName, keyAmt);
        }

        int count = 0;
        int limit = 0;
        int max = 99999;
        HgKvIterator<HgKvEntry> iterator = null;

        count = 0;

        iterator = session.scanIterator(tableName
                , toAllPartitionKey(keyName + "-000")
                , HgStoreClientConst.EMPTY_OWNER_KEY
                , 0, HgKvStore.SCAN_ANY, EMPTY_BYTES
        );
        //HgStoreTestUtil.println(iterator);
        //   Assert.assertEquals(100, HgStoreTestUtil.amountOf(iterator));
        List<byte[]> positionList = new LinkedList<>();
        while (iterator.hasNext()) {
            print((count++) + " ");
            HgKvEntry entry = iterator.next();
            print(entry);
            print(" " + Arrays.toString(iterator.position()) + "\n");
            positionList.add(iterator.position());
            if (count >= max) break;
        }


        iterator = session.scanIterator(tableName
                , toAllPartitionKey(keyName + "-000")
                , HgStoreClientConst.EMPTY_OWNER_KEY
                , 0, HgKvStore.SCAN_ANY, EMPTY_BYTES
        );

        byte[] position = positionList.get(50);
        println("seek: " + Arrays.toString(position));
        iterator.seek(position);
        //println("amt after seek: "+HgStoreTestUtil.println(iterator));
        Assert.assertEquals(50, HgStoreTestUtil.amountOf(iterator));

    }

     @Test
    public void scanBatch() {
        println("--- test scanBatch ---");
        String tableName = "UNIT_SCAN_BATCH_1";
        String keyName = "SCAN-BATCH";
        int keyAmt = 10_000;

        HgStoreSession session = getStoreSession();

        if (amountOf(session.scanIterator(tableName, toAllPartitionKey(keyName), keyAmt)) < keyAmt) {
            batchPut(session, tableName, keyName, keyAmt);
        }

        Assert.assertEquals(keyAmt, HgStoreTestUtil.amountOf(session.scanIterator(tableName)));

        List<HgKvIterator<HgKvEntry>> iterators = null;
        List<HgOwnerKey> prefixes = null;

        List<HgOwnerKey> startList = Arrays.asList(
                toAllPartitionKey(keyName + "-001")
                , toAllPartitionKey(keyName + "-003")
                , toAllPartitionKey(keyName + "-005")
                , toAllPartitionKey(keyName + "-007")
                , toAllPartitionKey(keyName + "-009")
        );

        List<HgOwnerKey> endList = Arrays.asList(
                toAllPartitionKey(keyName + "-002")
                , toAllPartitionKey(keyName + "-004")
                , toAllPartitionKey(keyName + "-006")
                , toAllPartitionKey(keyName + "-008")
                , toAllPartitionKey(keyName + "-010")
        );

        List<HgOwnerKey> prefixList = Arrays.asList(
                toAllPartitionKey(keyName + "-001")
                , toAllPartitionKey(keyName + "-002")
                , toAllPartitionKey(keyName + "-003")
                , toAllPartitionKey(keyName + "-004")
                , toAllPartitionKey(keyName + "-005")
                , toAllPartitionKey(keyName + "-006")
                , toAllPartitionKey(keyName + "-007")
                , toAllPartitionKey(keyName + "-008")
                , toAllPartitionKey(keyName + "-009")
        );

        println("-- test scan-batch all --");

        HgScanQuery scanQuery = HgScanQuery.tableOf(tableName);
        iterators = session.scanBatch(scanQuery);
        Assert.assertEquals(3, iterators.size());

        Assert.assertEquals(keyAmt, HgStoreTestUtil.println(iterators));

        println("-- test scan-batch prefix --");


        iterators = session.scanBatch(
                HgScanQuery.prefixOf(tableName, prefixList)
        );
        Assert.assertEquals(3, iterators.size());
        Assert.assertEquals(900,
                iterators.parallelStream().mapToInt(e -> HgStoreTestUtil.amountOf(e)).sum()
        );

        println("-- test scan-batch range --");

        iterators = session.scanBatch(HgScanQuery.rangeOf(tableName, startList, endList));
        Assert.assertEquals(3, iterators.size());
        Assert.assertEquals(
                HgStoreTestUtil.amountOf(
                        session.scanIterator(tableName
                                , toAllPartitionKey(keyName + "-001")
                                , toAllPartitionKey(keyName + "-010")
                        )
                )
                ,
                iterators.parallelStream().mapToInt(e -> HgStoreTestUtil.amountOf(e)).sum()
        );

        println("-- test scan-batch limit --");

        int limit = 1;
        iterators = session.scanBatch(
                HgScanQuery.rangeOf(tableName, startList, endList)
                        .builder()
                        .setLimit(limit)
                        .build()
        );

        //HgStoreTestUtil.println(iterators);
        Assert.assertEquals(iterators.size() * limit,
                iterators.parallelStream().mapToInt(e -> HgStoreTestUtil.amountOf(e)).sum()
        );

        println("-- test scan-batch multi-table --");
        if (amountOf(session.scanIterator("g+oe", toAllPartitionKey(keyName), keyAmt)) < keyAmt) {
            batchPut(session, "g+oe", keyName, keyAmt);
            batchPut(session, "g+ie", keyName, keyAmt);
        }

        prefixes = Collections.singletonList(toAllPartitionKey(keyName));

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

        println("-- test scan-batch per-key-limit --");

        tableName = "PER_KEY_LIMIT_TABLE";
        keyName = "PER_KEY_LIMIT";
        if (amountOf(session.scanIterator(tableName, toAllPartitionKey(keyName), keyAmt)) < keyAmt) {
            batchPut(session, tableName, keyName, keyAmt);
        }
        prefixes = Arrays.asList(
                toAllPartitionKey(keyName + "-01")
                , toAllPartitionKey(keyName + "-02")
                , toAllPartitionKey(keyName + "-03")
                , toAllPartitionKey(keyName + "-04")
                , toAllPartitionKey(keyName + "-05")
        );

        iterators = session.scanBatch(
                HgScanQuery.prefixOf(tableName, prefixes).builder().setPerKeyLimit(1).build()
        );
        //HgStoreTestUtil.println(iterators);
        Assert.assertEquals(prefixes.size() * 3, HgStoreTestUtil.amountIn(iterators));

    }

    // @Test
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

    // @Test
    public void handle_table() {
        println("--- test table ---");

        String tableName = "UNIT_TABLE_" + System.currentTimeMillis();
        HgOwnerKey key = toOwnerKey("FOR-PUT-KEY");
        byte[] value = toBytes("FOR-PUT-VALUE");
        HgStoreSession session = getStoreSession();

        println("-- test createTable --");
        session.createTable(tableName);
        Assert.assertEquals(true, session.existsTable(tableName));

        println("-- test deleteTable --");
        session.put(tableName, key, value);
        Assert.assertEquals(toStr(value), toStr(session.get(tableName, key)));
        session.deleteTable(tableName);
        Assert.assertNotEquals(toStr(value), toStr(session.get(tableName, key)));
        Assert.assertEquals(true, session.existsTable(tableName));


        println("-- test dropTable --");
        Assert.assertEquals(true, session.dropTable(tableName));
        Assert.assertEquals(false, session.existsTable(tableName));


        println("-- test existsTable --");
        Assert.assertEquals(false, session.existsTable(tableName));

    }

    // @Test
    public void tx() {
        println("--- test tx ---");
        HgStoreSession session = getStoreSession();
        String tableName = "UNIT_TABLE_TX";
        String keyPrefix = "TX";
        int keyAmt = 100;

        println("-- unsupported tx operation --");
        println("- tx deleteTable -");
        batchPut(session, tableName, keyPrefix, keyAmt);
        Assert.assertEquals(keyAmt, amountOf(session.scanIterator(tableName)));
        session.beginTx();
        session.deleteTable(tableName);
        session.commit();
        Assert.assertEquals(0, amountOf(session.scanIterator(tableName)));

        println("- tx dropTable -");
        batchPut(session, tableName, keyPrefix, keyAmt);
        Assert.assertEquals(keyAmt, amountOf(session.scanIterator(tableName)));
        session.beginTx();
        session.dropTable(tableName);
        session.commit();
        Assert.assertEquals(false, session.existsTable(tableName));
        Assert.assertEquals(0, amountOf(session.scanIterator(tableName)));

        println("- tx truncate -");
        batchPut(session, tableName, keyPrefix, keyAmt);
        Assert.assertEquals(keyAmt, amountOf(session.scanIterator(tableName)));
        session.beginTx();
        session.truncate();
        session.commit();
        Assert.assertEquals(false, session.existsTable(tableName));
        Assert.assertEquals(0, amountOf(session.scanIterator(tableName)));

        //TODO : add others
    }

    //// @Test
    public void scanIterator_WithNonePartition() {
        println("--- test scanIterator with none partition ---");
        int count = 0;
        HgStoreSession session = getStoreSession();

        for (int i = 0; i < 100; i++) {
            HgKvIterator<HgKvEntry> iterator = session.scanIterator("XXXXXXXX");
            while (iterator.hasNext()) {
                count++;
                HgKvEntry entry = iterator.next();
                println(entry);
            }
        }

        Assert.assertEquals(0, count);

    }

    //// @Test
    public void repeatedly_parallel_scan() {
        MetricX metrics = MetricX.ofStart();
        repeatedly_test(100, () -> parallel_scan());
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
        println("--- test scanIterator in parallel ---");
        parallel_test(100, () -> {this.scan();}, t -> t.printStackTrace());
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
            HgOwnerKey key = toOwnerKey(keyPrefix + "-" + padLeftZeros(String.valueOf(i), length));
            byte[] value = toBytes(keyPrefix + "-V-" + i);

            session.put(tableName, key, value);

            if ((i + 1) % 100_000 == 0) {
                println("---------- " + (i + 1) + " --------");
                println("Preparing took: " + (System.currentTimeMillis() - start) + " ms.");
                session.commit();
                println("Committing took: " + (System.currentTimeMillis() - start) + " ms.");
                start = System.currentTimeMillis();
                session.beginTx();
            }
        }

        if (session.isTx()) {
            session.commit();
        }

        Assert.assertEquals(amount, amountOf(session.scanIterator(tableName)));


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
        println("--- Benchmark scanBatch ---");
        String tableName = "Benchmark_SCAN_BATCH";
        String keyName = "SCAN-BATCH";
        int keyAmt = 10_000_000;

        HgStoreSession session = getStoreSession();

        if (amountOf(session.scanIterator(tableName, toAllPartitionKey(keyName), 1)) < 1) {
            batchPut(session, tableName, keyName, keyAmt);
        }
        println("-- Starting scan --");
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
        if (amountOf(session.scanIterator(tableName, toAllPartitionKey(keyName), 1)) < 1) {
            batchPut(session, tableName, keyName, amount);
        }

        MetricX metricX = MetricX.ofStart();

        int count = 0;
        HgKvIterator<HgKvEntry> iterator = session.scanIterator(tableName);
        //HgStoreTestUtil.println(iterator, e -> (e % (amount / 100) == 0));
        while (iterator.hasNext()) {
            count++;
            HgKvEntry entry = iterator.next();
            if (count % (amount / 10) == 0) {
                println(entry);
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

        if (amountOf(session.scanIterator(tableName, toAllPartitionKey(keyName), 1)) < 1) {
            batchPut(session, tableName, keyName, amount);
        }
        HgKvIterator<HgKvEntry> iterator = null;
        /* server not over, enough, extreme -> close */
        for (int i = 0; i <= 10_000; i++) {
            iterator = session.scanIterator(tableName);
            iterator.next();
            iterator.close();
            println("extreme loop: " + i);
        }
        runWaiting();
    }

    //// @Test
    public void parallel_scan_close() {
        parallel_test(10, () -> this.scan_close(), t -> t.printStackTrace());
    }

    //// @Test
    public void repeat_parallel_scan_close() {
        repeatedly_test(1000, () -> this.parallel_scan_close());
        runWaiting();
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