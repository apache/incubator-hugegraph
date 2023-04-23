package com.baidu.hugegraph.store.client;

import com.baidu.hugegraph.store.HgStoreSession;
import org.junit.Assert;
// import org.junit.BeforeClass;
// import org.junit.Test;

import static com.baidu.hugegraph.store.util.HgStoreTestUtil.*;

/**
 * @author lynn.bond@hotmail.com created on 2021/10/12
 */
public class HgStoreNodeSessionTest {
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
    public void truncate() {
        println("--- test truncate ---");
        String tableName = "UNIT_TRUNCATE_1";
        String keyName = "KEY_TRUNCATE";

        HgStoreSession session = getStoreSession();
        batchPut(session,tableName, keyName, 100);
        Assert.assertEquals(100, amountOf(session.scanIterator(tableName)));

        String tableName2 = "UNIT_TRUNCATE_2";
        batchPut(session,tableName2, keyName, 100);
        Assert.assertEquals(100, amountOf(session.scanIterator(tableName2)));


        session.truncate();
        Assert.assertEquals(0, amountOf(session.scanIterator(tableName)));
        Assert.assertEquals(0, amountOf(session.scanIterator(tableName2)));
    }

}