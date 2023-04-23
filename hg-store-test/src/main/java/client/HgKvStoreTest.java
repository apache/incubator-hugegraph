package client;

import com.baidu.hugegraph.store.HgKvEntry;
import com.baidu.hugegraph.store.HgKvIterator;
import com.baidu.hugegraph.store.HgOwnerKey;
import com.baidu.hugegraph.store.HgStoreSession;
import org.junit.Assert;
import org.junit.Test;

import static com.baidu.hugegraph.store.client.util.HgStoreClientUtil.toBytes;
import static com.baidu.hugegraph.store.client.util.HgStoreClientUtil.toOwnerKey;
import static com.baidu.hugegraph.store.client.util.HgStoreClientUtil.toStr;

public class HgKvStoreTest extends BaseClientTest{
    public static final String TABLE_NAME = "unit-table";

    @Test
    public void truncateTest() {
        System.out.println("--- test truncateTest ---");
        HgStoreSession graph0 = storeClient.openSession("hugegraph0");
        HgStoreSession graph1 = storeClient.openSession("hugegraph1");

        graph0.truncate();
        graph1.truncate();

        for (int i=0; i < 3; i++) {
            HgOwnerKey key = toOwnerKey("owner-" + i, "ownerKey-" + i);
            byte[] value0 = toBytes("g0 owner-" + i + ";ownerKey-" + i);
            graph0.put(TABLE_NAME, key, value0);

            byte[] value1 = toBytes("g1 owner-" + i + ";ownerKey-" + i);
            graph1.put(TABLE_NAME, key, value1);
        }

        HgKvIterator<HgKvEntry> iterator = graph0.scanIterator(TABLE_NAME);
        Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()){
            HgKvEntry entry = iterator.next();
//            log.info("key:{} value:{}", toStr(entry.key()), toStr(entry.value()));
            Assert.assertEquals(0, toStr(entry.value()).indexOf("g0"));
        }
        iterator = graph1.scanIterator(TABLE_NAME);
        Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()){
            HgKvEntry entry = iterator.next();
//            log.info("key:{} value:{}", toStr(entry.key()), toStr(entry.value()));
            Assert.assertEquals(0, toStr(entry.value()).indexOf("g1"));
        }

        graph0.truncate();
        iterator = graph0.scanIterator(TABLE_NAME);
        Assert.assertFalse(iterator.hasNext());

        iterator = graph1.scanIterator(TABLE_NAME);
        Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()){
            HgKvEntry entry = iterator.next();
            // System.out.println("key:" + toStr(entry.key()) + " value:" + toStr(entry.value()));
            Assert.assertEquals(0, toStr(entry.value()).indexOf("g1"));
        }
    }

    @Test
    public void deleteTableTest() {
        System.out.println("--- test deleteTableTest ---");
        HgStoreSession graph0 = storeClient.openSession("hugegraph0");
        HgStoreSession graph1 = storeClient.openSession("hugegraph1");
        graph0.truncate();
        graph1.truncate();

        for (int i=0; i < 2; i++) {
            HgOwnerKey key = toOwnerKey("owner-" + i, "ownerKey-" + i);
            byte[] value0 = toBytes("g0 owner-" + i + ";ownerKey-" + i);
            graph0.put(TABLE_NAME, key, value0);

            byte[] value1 = toBytes("g1 owner-" + i + ";ownerKey-" + i);
            graph1.put(TABLE_NAME, key, value1);
        }

        HgKvIterator<HgKvEntry> iterator = graph0.scanIterator(TABLE_NAME);
        Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()){
            HgKvEntry entry = iterator.next();
            Assert.assertEquals(0, toStr(entry.value()).indexOf("g0"));
        }
        iterator = graph1.scanIterator(TABLE_NAME);
        Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()){
            HgKvEntry entry = iterator.next();
            Assert.assertEquals(0, toStr(entry.value()).indexOf("g1"));
        }

        graph0.deleteTable(TABLE_NAME);
        Assert.assertTrue(graph0.existsTable(TABLE_NAME));
        iterator = graph0.scanIterator(TABLE_NAME);
        Assert.assertFalse(iterator.hasNext());
        while (iterator.hasNext()){
            HgKvEntry entry = iterator.next();
        }

        iterator = graph1.scanIterator(TABLE_NAME);
        Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()){
            HgKvEntry entry = iterator.next();
            Assert.assertEquals(0, toStr(entry.value()).indexOf("g1"));
        }
    }

    @Test
    public void dropTableTest() {
        System.out.println("--- test dropTableTest ---");
        HgStoreSession graph0 = storeClient.openSession("hugegraph0");
        HgStoreSession graph1 = storeClient.openSession("hugegraph1");
        graph0.truncate();
        graph1.truncate();

        for (int i=0; i < 2; i++) {
            HgOwnerKey key = toOwnerKey("owner-" + i, "ownerKey-" + i);
            byte[] value0 = toBytes("g0 owner-" + i + ";ownerKey-" + i);
            graph0.put(TABLE_NAME, key, value0);

            byte[] value1 = toBytes("g1 owner-" + i + ";ownerKey-" + i);
            graph1.put(TABLE_NAME, key, value1);
        }

        HgKvIterator<HgKvEntry> iterator = graph0.scanIterator(TABLE_NAME);
        Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()){
            HgKvEntry entry = iterator.next();
            Assert.assertEquals(0, toStr(entry.value()).indexOf("g0"));
        }
        iterator = graph1.scanIterator(TABLE_NAME);
        Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()){
            HgKvEntry entry = iterator.next();
            Assert.assertEquals(0, toStr(entry.value()).indexOf("g1"));
        }

        graph0.dropTable(TABLE_NAME);
        Assert.assertTrue(graph0.existsTable(TABLE_NAME));
        iterator = graph0.scanIterator(TABLE_NAME);
        Assert.assertFalse(iterator.hasNext());
        while (iterator.hasNext()){
            HgKvEntry entry = iterator.next();
        }

        iterator = graph1.scanIterator(TABLE_NAME);
        Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()){
            HgKvEntry entry = iterator.next();
            Assert.assertEquals(0, toStr(entry.value()).indexOf("g1"));
        }
    }

    @Test
    public void moveDataPathTest() {
        System.out.println("--- test moveDataPathTest ---");
        HgStoreSession graph0 = storeClient.openSession("hugegraph0");
        graph0.truncate();

        for (int i=0; i < 2; i++) {
            HgOwnerKey key = toOwnerKey("owner-" + i, "ownerKey-" + i);
            byte[] value0 = toBytes("g0 owner-" + i + ";ownerKey-" + i);
            graph0.put(TABLE_NAME, key, value0);
        }

        HgKvIterator<HgKvEntry> iterator = graph0.scanIterator(TABLE_NAME);
        Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()){
            HgKvEntry entry = iterator.next();
            Assert.assertEquals(0, toStr(entry.value()).indexOf("g0"));
        }
    }
}
