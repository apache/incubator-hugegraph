package com.baidu.hugegraph.store.test;

import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.store.HgKvEntry;
import com.baidu.hugegraph.store.HgKvIterator;
import com.baidu.hugegraph.store.HgStoreSession;
import com.baidu.hugegraph.store.client.HgStoreClientBase;
import com.baidu.hugegraph.store.util.HgStoreTestUtil;
import org.junit.Assert;
// import org.junit.Test;

/**
 * 测试修改副本数
 */
public class ChangeShardNumTest extends HgStoreClientBase {

    // @Test
    public void test3To1() throws PDException {
        int number = 10000;
        HgStoreSession session = storeClient.openSession(Graph_Name);
        HgStoreTestUtil.batchPut(session, Table_Name, "testKey", number);

        try(HgKvIterator<HgKvEntry> iterators = session.scanIterator(Table_Name)){
            Assert.assertEquals(number, HgStoreTestUtil.amountOf(iterators));
        }

        Metapb.PDConfig pdConfig = pdClient.getPDConfig();
        pdConfig = pdConfig.toBuilder().setShardCount(1).build();

        pdClient.setPDConfig(pdConfig);
        pdClient.balancePartition();
    }

    // @Test
    public void test1To3() throws PDException {
        int number = 10000;
        HgStoreSession session = storeClient.openSession(Graph_Name);
        HgStoreTestUtil.batchPut(session, Table_Name, "testKey", number);

        try(HgKvIterator<HgKvEntry> iterators = session.scanIterator(Table_Name)){
            Assert.assertEquals(number, HgStoreTestUtil.amountOf(iterators));
        }

        Metapb.PDConfig pdConfig = pdClient.getPDConfig();
        pdConfig = pdConfig.toBuilder().setShardCount(3).build();

        pdClient.setPDConfig(pdConfig);
        pdClient.balancePartition();
    }

}
