package com.baidu.hugegraph.store.client;

import com.baidu.hugegraph.store.*;
import org.junit.Assert;
// import org.junit.BeforeClass;
// import org.junit.Test;

import java.util.NoSuchElementException;

import static com.baidu.hugegraph.store.util.HgStoreTestUtil.*;

/**
 * @author lynn.bond@hotmail.com created on 2021/10/12
 */
public class HgStoreNodeStreamTest {
    private static HgStoreNodeManager nodeManager = HgStoreNodeManager.getInstance();
    private static HgStoreNode node;

    // @BeforeClass
    public static void init() {
        node = nodeManager.addNode(GRAPH_NAME, nodeManager.getNodeBuilder().setAddress("localhost:9080").build());
    }

    private HgStoreNode getOneNode() {
        return node;
    }

    private static HgStoreSession getStoreSession() {
        return node.openSession(GRAPH_NAME);
    }


    // @Test
    public void scanIterator() {

        println("--- test scanIterator ---");
        String tableName = "UNIT_SCAN";
        String keyName = "SCAN-ITER";
        HgStoreSession session = getStoreSession();
        batchPut(session,tableName, keyName, 10000);
        int count = 0;
        int limit = 0;
        int max = 99999;
        HgKvIterator<HgKvEntry> iterator = null;

        println("-- test 0 element --");
        iterator = session.scanIterator(tableName, toOwnerKey("__SCAN-001"), toOwnerKey("__SCAN-100"), 0);
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
            iterator = session.scanIterator(tableName, toOwnerKey(keyName + "-0"), toOwnerKey(keyName + "-1"), limit);
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
                    toOwnerKey(keyName + "-00001"), toOwnerKey(keyName + "-00005"), limit);
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
        iterator = session.scanIterator(tableName, toOwnerKey(keyName + "-0"), toOwnerKey(keyName + "-1"), limit);

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
            if (count % 100 == 0) {
                println(entry);
            }
            if (count >= max) break;
        }
        Assert.assertEquals(10000, count);

        println("-- test scan prefix --");
        iterator = session.scanIterator(tableName, toOwnerKey(keyName + "-01"));
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

    //// @Test
    public void scanIteratorBenchmark() {
        /*************** test no limit, with 10 millions **************/
        String tableName = "UNIT_HUGE";
        String keyName = "SCAN-HUGE";
        HgStoreSession session = getStoreSession();
        //batchPut(session,tableName, keyName, 10000000);
        int count = 0;
        int limit = 0;
        HgKvIterator<HgKvEntry> iterator = null;

        limit = 0;
        iterator = session.scanIterator(tableName);
        count = 0;
        while (iterator.hasNext()) {
            count++;
            HgKvEntry entry = iterator.next();
            if (count % 100000 == 0) {
                println(entry);
            }
        }

        Assert.assertEquals(10000000, count);

    }
}