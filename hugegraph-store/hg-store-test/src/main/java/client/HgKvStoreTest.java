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

package client;

import static org.apache.hugegraph.store.client.util.HgStoreClientUtil.toBytes;
import static org.apache.hugegraph.store.client.util.HgStoreClientUtil.toOwnerKey;
import static org.apache.hugegraph.store.client.util.HgStoreClientUtil.toStr;

import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.HgStoreSession;
import org.junit.Assert;
import org.junit.Test;

public class HgKvStoreTest extends BaseClientTest {
    public static final String TABLE_NAME = "unit-table";

    @Test
    public void truncateTest() {
        System.out.println("--- test truncateTest ---");
        HgStoreSession graph0 = storeClient.openSession("hugegraph0");
        HgStoreSession graph1 = storeClient.openSession("hugegraph1");

        graph0.truncate();
        graph1.truncate();

        for (int i = 0; i < 3; i++) {
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

        for (int i = 0; i < 2; i++) {
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
            Assert.assertEquals(0, toStr(entry.value()).indexOf("g0"));
        }
        iterator = graph1.scanIterator(TABLE_NAME);
        Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()) {
            HgKvEntry entry = iterator.next();
            Assert.assertEquals(0, toStr(entry.value()).indexOf("g1"));
        }

        graph0.deleteTable(TABLE_NAME);
        Assert.assertTrue(graph0.existsTable(TABLE_NAME));
        iterator = graph0.scanIterator(TABLE_NAME);
        Assert.assertFalse(iterator.hasNext());
        while (iterator.hasNext()) {
            HgKvEntry entry = iterator.next();
        }

        iterator = graph1.scanIterator(TABLE_NAME);
        Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()) {
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

        for (int i = 0; i < 2; i++) {
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
            Assert.assertEquals(0, toStr(entry.value()).indexOf("g0"));
        }
        iterator = graph1.scanIterator(TABLE_NAME);
        Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()) {
            HgKvEntry entry = iterator.next();
            Assert.assertEquals(0, toStr(entry.value()).indexOf("g1"));
        }

        graph0.dropTable(TABLE_NAME);
        Assert.assertTrue(graph0.existsTable(TABLE_NAME));
        iterator = graph0.scanIterator(TABLE_NAME);
        Assert.assertFalse(iterator.hasNext());
        while (iterator.hasNext()) {
            HgKvEntry entry = iterator.next();
        }

        iterator = graph1.scanIterator(TABLE_NAME);
        Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()) {
            HgKvEntry entry = iterator.next();
            Assert.assertEquals(0, toStr(entry.value()).indexOf("g1"));
        }
    }

    @Test
    public void moveDataPathTest() {
        System.out.println("--- test moveDataPathTest ---");
        HgStoreSession graph0 = storeClient.openSession("hugegraph0");
        graph0.truncate();

        for (int i = 0; i < 2; i++) {
            HgOwnerKey key = toOwnerKey("owner-" + i, "ownerKey-" + i);
            byte[] value0 = toBytes("g0 owner-" + i + ";ownerKey-" + i);
            graph0.put(TABLE_NAME, key, value0);
        }

        HgKvIterator<HgKvEntry> iterator = graph0.scanIterator(TABLE_NAME);
        Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()) {
            HgKvEntry entry = iterator.next();
            Assert.assertEquals(0, toStr(entry.value()).indexOf("g0"));
        }
    }
}
