package com.baidu.hugegraph.store.client;

import com.baidu.hugegraph.pd.client.PDClient;
import com.baidu.hugegraph.pd.client.PDConfig;
import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.store.HgKvEntry;
import com.baidu.hugegraph.store.HgKvIterator;
import com.baidu.hugegraph.store.HgKvStore;
import com.baidu.hugegraph.store.HgOwnerKey;
import com.baidu.hugegraph.store.HgStoreClient;
import com.baidu.hugegraph.store.HgStoreSession;
import com.baidu.hugegraph.store.util.HgStoreTestUtil;
import org.apache.commons.codec.binary.Hex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
// import org.junit.Test;

import java.util.List;

import static com.baidu.hugegraph.store.client.util.HgStoreClientConst.ALL_PARTITION_OWNER;

public class HgStoreClientTest {

    private HgStoreClient storeClient;
    private PDClient pdClient;
    public final static byte[] EMPTY_BYTES = new byte[0];
    private static String Graph_Name = "testGraphName";
    private static String Table_Name = "testTableName";

    @Before
    public void init() {
        storeClient = HgStoreClient.create(PDConfig.of("127.0.0.1:8686")
                .setEnableCache(true));
        pdClient = storeClient.getPdClient();
    }


    @Test
    public void testPutData() {
        HgStoreSession session = storeClient.openSession(Graph_Name);
        long start = System.currentTimeMillis();
        HgStoreTestUtil.batchPut(session, Table_Name, "testKey", 100000);

        System.out.println("Time is " + (System.currentTimeMillis() - start));
        try (HgKvIterator<HgKvEntry> iterator = session.scanIterator(Table_Name)) {
            Assert.assertEquals(100000, HgStoreTestUtil.amountOf(iterator));
        }
    }

    @Test
    public void testScan() throws PDException {

        HgStoreSession session = storeClient.openSession(Graph_Name);
        HgStoreTestUtil.batchPut(session, Table_Name, "testKey", 12);

        int count = 0;
        byte[] position = null;
        try (HgKvIterator<HgKvEntry> iterator = session.scanIterator(Table_Name)) {
            while (iterator.hasNext()) {
                iterator.next();
                position = iterator.position();
                dumpPosition(position);
                if (++count > 5) {
                    break;
                }
            }
        }


        System.out.println("--------------------------------");
        try (HgKvIterator<HgKvEntry> iterator = session.scanIterator(Table_Name)) {
            iterator.seek(position);
            while (iterator.hasNext()) {
                iterator.next();
                dumpPosition(iterator.position());
            }
        }

        System.out.println("--------------------------------");


        byte[] start = new byte[]{0x0};
        byte[] end = new byte[]{-1};
        try (HgKvIterator<HgKvEntry> iterator = session.scanIterator(Table_Name,
                HgOwnerKey.of(ALL_PARTITION_OWNER, start),
                HgOwnerKey.of(ALL_PARTITION_OWNER, end))) {
            iterator.seek(position);
            while (iterator.hasNext()) {
                iterator.next();
                dumpPosition(iterator.position());
            }
        }
    }

    public void dumpPosition(byte[] b) {
        byte[] buf = new byte[Long.BYTES];
        System.arraycopy(b, 0, buf, 0, Long.BYTES);
        long storeId = HgStoreTestUtil.toLong(buf);
        buf = new byte[Integer.BYTES];
        System.arraycopy(b, Long.BYTES, buf, 0, Integer.BYTES);
        int partId = HgStoreTestUtil.toInt(buf);
        String key = new String(b);

        System.out.println(" " + storeId + ", " + partId + ", " + key);
    }

    // @Test
    public void testDeleteData() {
        Table_Name = "deleteData5";
        HgStoreSession session = storeClient.openSession(Graph_Name);
        int ownerCode = 1;
        HgStoreTestUtil.batchPut(session, Table_Name, "T", 10, (key) -> {
                    return HgStoreTestUtil.toOwnerKey(ownerCode, key);
                }
        );
        try (HgKvIterator<HgKvEntry> iterators = session.scanIterator(Table_Name)) {
//            while (iterators.hasNext()){
//                System.out.println(new String(iterators.next().key()));
//            }
            Assert.assertEquals(10, HgStoreTestUtil.amountOf(iterators));
        }
        session.beginTx();
        session.deletePrefix(Table_Name, HgStoreTestUtil.toOwnerKey(ownerCode, "T"));
        session.commit();

        System.out.println("=================================");
        try (HgKvIterator<HgKvEntry> iterators = session.scanIterator(Table_Name)) {
            Assert.assertEquals(0, HgStoreTestUtil.amountOf(iterators));
//            while (iterators.hasNext()){
//                System.out.println(new String(iterators.next().key()));
//            }
        }
    }

    // @Test
    public void testDropTable() throws PDException {
        HgStoreSession session = storeClient.openSession(Graph_Name);

        String table1 = "Table1";
        session.createTable(table1);
        HgStoreTestUtil.batchPut(session, table1, "testKey", 1000);

        try (HgKvIterator<HgKvEntry> iterators = session.scanIterator(table1)) {
            Assert.assertEquals(1000, HgStoreTestUtil.amountOf(iterators));
        }

        session.dropTable(table1);
        try (HgKvIterator<HgKvEntry> iterators = session.scanIterator(table1)) {
            Assert.assertEquals(0, HgStoreTestUtil.amountOf(iterators));
        }

        deleteGraph(Graph_Name);
    }


    public void deleteGraph(String graphName) throws PDException {
        HgStoreSession session = storeClient.openSession(graphName);
        session.deleteGraph(graphName);
        pdClient.delGraph(graphName);

        Metapb.Graph graph = null;
        try {
            graph = pdClient.getGraph(graphName);
        } catch (PDException e) {
            Assert.assertEquals(103, e.getErrorCode());
        }
        Assert.assertNull(graph);
    }

    // @Test
    public void testScanPartition() throws PDException {
        // testPutData();
        List<Metapb.Partition> partitions = pdClient.getPartitions(0, "DEFAULT/hugegraph/g");
        HgStoreSession session = storeClient.openSession("DEFAULT/hugegraph/g");
        for (Metapb.Partition partition : partitions) {
            try (HgKvIterator<HgKvEntry> iterators = session.scanIterator("g+v",
                    (int) (partition.getStartKey()), (int) (partition.getEndKey()),
                    HgKvStore.SCAN_HASHCODE, EMPTY_BYTES)) {

                System.out.println(" " + partition.getId() + " " + HgStoreTestUtil.amountOf(iterators));
            }
        }
    }


}
