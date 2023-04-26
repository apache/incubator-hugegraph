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

package org.apache.hugegraph.store.client;

import static org.apache.hugegraph.store.client.util.HgStoreClientUtil.toBytes;
import static org.apache.hugegraph.store.client.util.HgStoreClientUtil.toOwnerKey;
import static org.apache.hugegraph.store.client.util.HgStoreClientUtil.toStr;
import static org.apache.hugegraph.store.util.HgStoreTestUtil.TABLE_NAME;

import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.HgStoreClient;
import org.apache.hugegraph.store.HgStoreSession;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class HgKvStoreTest {
//    @Autowired
//    private AppConfig appConfig;

    private final String tableName0 = "cli-table0";
    private HgStoreClient storeClient;
    private PDClient pdClient;

    @Before
    public void init() {
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

        for (int i = 0; i < 100; i++) {
            HgOwnerKey key = toOwnerKey("owner-" + i, "ownerKey-" + i);
            byte[] value0 = toBytes("g0 owner-" + i + ";ownerKey-" + i);
            graph0.put(TABLE_NAME, key, value0);

            byte[] value1 = toBytes("g1 owner-" + i + ";ownerKey-" + i);
            graph1.put(TABLE_NAME, key, value1);
        }

        HgKvIterator<HgKvEntry> iterator = graph0.scanIterator(TABLE_NAME);
        Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()) {
            HgKvEntry entry = iterator.next();
//            log.info("key:{} value:{}", toStr(entry.key()), toStr(entry.value()));
            Assert.assertEquals(0, toStr(entry.value()).indexOf("g0"));
        }
        iterator = graph1.scanIterator(TABLE_NAME);
        Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()) {
            HgKvEntry entry = iterator.next();
//            log.info("key:{} value:{}", toStr(entry.key()), toStr(entry.value()));
            Assert.assertEquals(0, toStr(entry.value()).indexOf("g1"));
        }

        graph0.truncate();
        iterator = graph0.scanIterator(TABLE_NAME);
        Assert.assertFalse(iterator.hasNext());

        iterator = graph1.scanIterator(TABLE_NAME);
        Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()) {
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

        for (int i = 0; i < 10; i++) {
            HgOwnerKey key = toOwnerKey("owner-" + i, "ownerKey-" + i);
            byte[] value0 = toBytes("g0 owner-" + i + ";ownerKey-" + i);
            graph0.put(TABLE_NAME, key, value0);

            byte[] value1 = toBytes("g1 owner-" + i + ";ownerKey-" + i);
            graph1.put(TABLE_NAME, key, value1);
        }

        HgKvIterator<HgKvEntry> iterator = graph0.scanIterator(TABLE_NAME);
        Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()) {
            HgKvEntry entry = iterator.next();
//            log.info("key:{} value:{}", toStr(entry.key()), toStr(entry.value()));
            Assert.assertEquals(0, toStr(entry.value()).indexOf("g0"));
        }
        iterator = graph1.scanIterator(TABLE_NAME);
        Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()) {
            HgKvEntry entry = iterator.next();
//            log.info("key:{} value:{}", toStr(entry.key()), toStr(entry.value()));
            Assert.assertEquals(0, toStr(entry.value()).indexOf("g1"));
        }

        graph0.deleteTable(TABLE_NAME);
        Assert.assertTrue(graph0.existsTable(TABLE_NAME));
        iterator = graph0.scanIterator(TABLE_NAME);
        Assert.assertFalse(iterator.hasNext());
        while (iterator.hasNext()) {
            HgKvEntry entry = iterator.next();
            log.info("key:{} value:{}", toStr(entry.key()), toStr(entry.value()));
//            Assert.assertEquals(0, toStr(entry.value()).indexOf("g1"));
        }

        iterator = graph1.scanIterator(TABLE_NAME);
        Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()) {
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

        for (int i = 0; i < 10; i++) {
            HgOwnerKey key = toOwnerKey("owner-" + i, "ownerKey-" + i);
            byte[] value0 = toBytes("g0 owner-" + i + ";ownerKey-" + i);
            graph0.put(TABLE_NAME, key, value0);

            byte[] value1 = toBytes("g1 owner-" + i + ";ownerKey-" + i);
            graph1.put(TABLE_NAME, key, value1);
        }

        HgKvIterator<HgKvEntry> iterator = graph0.scanIterator(TABLE_NAME);
        Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()) {
            HgKvEntry entry = iterator.next();
//            log.info("key:{} value:{}", toStr(entry.key()), toStr(entry.value()));
            Assert.assertEquals(0, toStr(entry.value()).indexOf("g0"));
        }
        iterator = graph1.scanIterator(TABLE_NAME);
        Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()) {
            HgKvEntry entry = iterator.next();
//            log.info("key:{} value:{}", toStr(entry.key()), toStr(entry.value()));
            Assert.assertEquals(0, toStr(entry.value()).indexOf("g1"));
        }

        graph0.dropTable(TABLE_NAME);
        Assert.assertTrue(graph0.existsTable(TABLE_NAME));
        iterator = graph0.scanIterator(TABLE_NAME);
        Assert.assertFalse(iterator.hasNext());
        while (iterator.hasNext()) {
            HgKvEntry entry = iterator.next();
            log.info("key:{} value:{}", toStr(entry.key()), toStr(entry.value()));
//            Assert.assertEquals(0, toStr(entry.value()).indexOf("g1"));
        }

        iterator = graph1.scanIterator(TABLE_NAME);
        Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()) {
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

        for (int i = 0; i < 10; i++) {
            HgOwnerKey key = toOwnerKey("owner-" + i, "ownerKey-" + i);
            byte[] value0 = toBytes("g0 owner-" + i + ";ownerKey-" + i);
            graph0.put(TABLE_NAME, key, value0);
        }

        HgKvIterator<HgKvEntry> iterator = graph0.scanIterator(TABLE_NAME);
        Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()) {
            HgKvEntry entry = iterator.next();
            log.info("key:{} value:{}", toStr(entry.key()), toStr(entry.value()));
            Assert.assertEquals(0, toStr(entry.value()).indexOf("g0"));
        }

    }
}