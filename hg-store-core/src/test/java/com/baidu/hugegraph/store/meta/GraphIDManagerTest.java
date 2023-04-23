package com.baidu.hugegraph.store.meta;

import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.rocksdb.access.RocksDBSession;
import com.baidu.hugegraph.store.UnitTestBase;
import com.baidu.hugegraph.store.meta.base.DBSessionBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class GraphIDManagerTest extends UnitTestBase {
    @Before
    public void init(){
        String dbPath = "/tmp/junit";
        deleteDir(new File(dbPath));
        super.initDB(dbPath);
    }

    @Test
    public void test() throws PDException {
        GraphIdManager.maxGraphID = 64;
        int max = GraphIdManager.maxGraphID;
        try(RocksDBSession session = getDBSession("test")) {
            GraphIdManager gid = new GraphIdManager(new DBSessionBuilder() {
                @Override
                public RocksDBSession getSession(int partId) {
                    return session.clone();
                }
            }, 0);
            for(int i = 0; i < max; i++ ){
                Assert.assertEquals(i, gid.getCId("Test", max));
            }

            Assert.assertEquals(-1, gid.getCId("Test", max));

            gid.delCId("Test", 3);
            Assert.assertEquals(3, gid.getCId("Test", max));
            Assert.assertEquals(-1, gid.getCId("Test", max));

            long start = System.currentTimeMillis();
            for (int i = 0; i < GraphIdManager.maxGraphID; i++) {
                long id = gid.getGraphId("g" + i);
                Assert.assertEquals(i, id);
            }
            System.out.println("time is " + (System.currentTimeMillis() - start));
            {
                gid.releaseGraphId("g" + 10);
                long id = gid.getGraphId("g" + 10);
                Assert.assertEquals(10, id);
            }
            start = System.currentTimeMillis();
            for (int i = 0; i < GraphIdManager.maxGraphID; i++) {
                long id = gid.releaseGraphId("g" + i);
                Assert.assertEquals(i, id);
            }
            System.out.println("time is " + (System.currentTimeMillis() - start));
            start = System.currentTimeMillis();
            for (int i = 0; i < GraphIdManager.maxGraphID; i++) {
                long id = gid.getCId(GraphIdManager.GRAPH_ID_PREFIX, GraphIdManager.maxGraphID);
              //  long id = gid.getGraphId("g" + i);
                Assert.assertTrue(id >= 0);
            }
            System.out.println("time is " + (System.currentTimeMillis() - start));
        }
    }
}
