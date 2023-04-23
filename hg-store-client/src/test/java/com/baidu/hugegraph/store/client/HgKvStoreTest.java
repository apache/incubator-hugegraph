package com.baidu.hugegraph.store.client;

import com.baidu.hugegraph.pd.client.PDClient;
import com.baidu.hugegraph.pd.client.PDConfig;
import com.baidu.hugegraph.store.HgKvEntry;
import com.baidu.hugegraph.store.HgKvIterator;
import com.baidu.hugegraph.store.HgOwnerKey;
import com.baidu.hugegraph.store.HgStoreClient;
import com.baidu.hugegraph.store.HgStoreSession;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.baidu.hugegraph.store.client.util.HgStoreClientUtil.toBytes;
import static com.baidu.hugegraph.store.client.util.HgStoreClientUtil.toOwnerKey;
import static com.baidu.hugegraph.store.client.util.HgStoreClientUtil.toStr;
import static com.baidu.hugegraph.store.util.HgStoreTestUtil.TABLE_NAME;


@Slf4j
public class HgKvStoreTest {
//    @Autowired
//    private AppConfig appConfig;

    private HgStoreClient storeClient;
    private PDClient pdClient;
    private String tableName0 = "cli-table0";

    @Before
    public void init(){
        storeClient = HgStoreClient.create(PDConfig.of("127.0.0.1:8686")
                .setEnableCache(true));
        pdClient = storeClient.getPdClient();
    }

    @Test
    public void truncateTest() {
        log.info("--- test truncateTest ---");
        HgStoreSession graph0 = storeClient.openSession("hugegraph0");
        HgStoreSession graph1 = storeClient.openSession("hugegraph1");

        graph0.truncate();
        graph1.truncate();

        for (int i=0; i < 100; i++) {
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
            log.info("key:{} value:{}", toStr(entry.key()), toStr(entry.value()));
            Assert.assertEquals(0, toStr(entry.value()).indexOf("g1"));
        }
    }

    @Test
    public void deleteTableTest() {
        log.info("--- test deleteTableTest ---");
        HgStoreSession graph0 = storeClient.openSession("hugegraph0");
        HgStoreSession graph1 = storeClient.openSession("hugegraph1");
        graph0.truncate();
        graph1.truncate();

        for (int i=0; i < 10; i++) {
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

        graph0.deleteTable(TABLE_NAME);
        Assert.assertTrue(graph0.existsTable(TABLE_NAME));
        iterator = graph0.scanIterator(TABLE_NAME);
        Assert.assertFalse(iterator.hasNext());
        while (iterator.hasNext()){
            HgKvEntry entry = iterator.next();
            log.info("key:{} value:{}", toStr(entry.key()), toStr(entry.value()));
//            Assert.assertEquals(0, toStr(entry.value()).indexOf("g1"));
        }

        iterator = graph1.scanIterator(TABLE_NAME);
        Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()){
            HgKvEntry entry = iterator.next();
//            log.info("key:{} value:{}", toStr(entry.key()), toStr(entry.value()));
            Assert.assertEquals(0, toStr(entry.value()).indexOf("g1"));
        }
    }

    @Test
    public void dropTableTest() {
        log.info("--- test dropTableTest ---");
        HgStoreSession graph0 = storeClient.openSession("hugegraph0");
        HgStoreSession graph1 = storeClient.openSession("hugegraph1");
        graph0.truncate();
        graph1.truncate();

        for (int i=0; i < 10; i++) {
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

        graph0.dropTable(TABLE_NAME);
        Assert.assertTrue(graph0.existsTable(TABLE_NAME));
        iterator = graph0.scanIterator(TABLE_NAME);
        Assert.assertFalse(iterator.hasNext());
        while (iterator.hasNext()){
            HgKvEntry entry = iterator.next();
            log.info("key:{} value:{}", toStr(entry.key()), toStr(entry.value()));
//            Assert.assertEquals(0, toStr(entry.value()).indexOf("g1"));
        }

        iterator = graph1.scanIterator(TABLE_NAME);
        Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()){
            HgKvEntry entry = iterator.next();
//            log.info("key:{} value:{}", toStr(entry.key()), toStr(entry.value()));
            Assert.assertEquals(0, toStr(entry.value()).indexOf("g1"));
        }
    }

    @Test
    public void moveDataPathTest() {
        log.info("--- test moveDataPathTest ---");
//        log.info("data path=", appConfig.getDataPath());
        HgStoreSession graph0 = storeClient.openSession("hugegraph0");
        graph0.truncate();

        for (int i=0; i < 10; i++) {
            HgOwnerKey key = toOwnerKey("owner-" + i, "ownerKey-" + i);
            byte[] value0 = toBytes("g0 owner-" + i + ";ownerKey-" + i);
            graph0.put(TABLE_NAME, key, value0);
        }

        HgKvIterator<HgKvEntry> iterator = graph0.scanIterator(TABLE_NAME);
        Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()){
            HgKvEntry entry = iterator.next();
            log.info("key:{} value:{}", toStr(entry.key()), toStr(entry.value()));
            Assert.assertEquals(0, toStr(entry.value()).indexOf("g0"));
        }

    }
}