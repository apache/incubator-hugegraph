/*
 * Copyright 2017 HugeGraph Authors
 *
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

package com.baidu.hugegraph.unit.rocksdb;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.rocksdb.RocksDBException;

import com.baidu.hugegraph.backend.store.rocksdb.RocksDBMetrics;
import com.baidu.hugegraph.backend.store.rocksdb.RocksDBOptions;
import com.baidu.hugegraph.backend.store.rocksdb.RocksDBSessions;
import com.baidu.hugegraph.backend.store.rocksdb.RocksDBStdSessions;
import com.baidu.hugegraph.backend.store.rocksdbsst.RocksDBSstSessions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.unit.FakeObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class RocksDBSessionsTest extends BaseRocksDBUnitTest {

    @Test
    public void testTable() throws RocksDBException {
        final String TABLE2 = "test-table2";

        Assert.assertTrue(this.rocks.existsTable(TABLE));
        Assert.assertFalse(this.rocks.existsTable(TABLE2));

        this.rocks.createTable(TABLE2);
        Assert.assertTrue(this.rocks.existsTable(TABLE2));
        Assert.assertEquals(ImmutableSet.of(TABLE, TABLE2),
                            this.rocks.openedTables());

        this.rocks.session().put(TABLE, b("person:1gname"), b("James"));
        this.rocks.session().put(TABLE2, b("person:1gname"), b("James2"));
        this.commit();

        String value = s(this.rocks.session().get(TABLE, b("person:1gname")));
        Assert.assertEquals("James", value);

        String value2 = s(this.rocks.session().get(TABLE2, b("person:1gname")));
        Assert.assertEquals("James2", value2);

        this.rocks.dropTable(TABLE2);
        Assert.assertFalse(this.rocks.existsTable(TABLE2));
        Assert.assertEquals(ImmutableSet.of(TABLE),
                            this.rocks.openedTables());
    }

    @Test
    public void testProperty() throws RocksDBException {
        final String TABLE2 = "test-table2";
        this.rocks.createTable(TABLE2);

        this.rocks.session().put(TABLE, b("person:1gname"), b("James"));
        this.rocks.session().put(TABLE, b("person:2gname"), b("James2"));
        this.commit();

        Assert.assertEquals(ImmutableList.of("0"),
                            this.rocks.property(RocksDBMetrics.KEY_DISK_USAGE));
        Assert.assertEquals(ImmutableList.of("2", "0"),
                            this.rocks.property(RocksDBMetrics.KEY_NUM_KEYS));

        this.rocks.session().put(TABLE2, b("person:1gname"), b("James1"));
        this.rocks.session().put(TABLE2, b("person:2gname"), b("James2"));
        this.rocks.session().put(TABLE2, b("person:3gname"), b("James3"));
        this.commit();

        Assert.assertEquals(ImmutableList.of("0"),
                            this.rocks.property(RocksDBMetrics.KEY_DISK_USAGE));
        Assert.assertEquals(ImmutableList.of("2", "3"),
                            this.rocks.property(RocksDBMetrics.KEY_NUM_KEYS));
    }

    @Test
    public void testCompactRange() throws RocksDBException {
        this.rocks.session().put(TABLE, b("person:1gname"), b("James"));
        this.rocks.session().put(TABLE, b("person:2gname"), b("James2"));
        this.commit();

        this.rocks.compactRange();

        String value = s(this.rocks.session().get(TABLE, b("person:1gname")));
        Assert.assertEquals("James", value);

        value = s(this.rocks.session().get(TABLE, b("person:2gname")));
        Assert.assertEquals("James2", value);
    }

    @Test
    public void testSnapshot() throws RocksDBException, IOException {
        this.rocks.session().put(TABLE, b("person:1gname"), b("James"));
        this.rocks.session().commit();

        String snapshotPath = SNAPSHOT_PATH + "/rocksdb";
        try {
            this.rocks.createSnapshot(snapshotPath);

            byte[] value = this.rocks.session().get(TABLE, b("person:1gname"));
            Assert.assertEquals("James", s(value));

            this.rocks.session().put(TABLE, b("person:1gname"), b("James2"));
            this.rocks.session().commit();

            value = this.rocks.session().get(TABLE, b("person:1gname"));
            Assert.assertEquals("James2", s(value));

            this.rocks.resumeSnapshot(snapshotPath);

            value = this.rocks.session().get(TABLE, b("person:1gname"));
            Assert.assertEquals("James", s(value));
        } finally {
            File snapshotFile = FileUtils.getFile(SNAPSHOT_PATH);
            if (snapshotFile.exists()) {
                FileUtils.forceDelete(snapshotFile);
            }
        }
    }

    @Test
    public void testCopySessions() throws RocksDBException {
        Assert.assertFalse(this.rocks.closed());

        HugeConfig config = FakeObjects.newConfig();
        RocksDBSessions copy = this.rocks.copy(config, "db2", "store2");
        Assert.assertFalse(this.rocks.closed());

        final String TABLE2 = "test-table2";
        copy.createTable(TABLE2);

        copy.session().put(TABLE2, b("person:1gname"), b("James"));
        copy.session().commit();

        String value = s(copy.session().get(TABLE2, b("person:1gname")));
        Assert.assertEquals("James", value);

        copy.close();
        Assert.assertTrue(copy.closed());
        Assert.assertFalse(this.rocks.closed());
    }

    @Test
    public void testIngestSst() throws RocksDBException {
        HugeConfig config = FakeObjects.newConfig();
        String sstPath = DB_PATH + "/sst";
        config.addProperty(RocksDBOptions.SST_PATH.name(), sstPath);
        RocksDBSstSessions sstSessions = new RocksDBSstSessions(config,
                                                                "sst", "store",
                                                                sstPath);
        final String TABLE1 = "test-table1";
        final String TABLE2 = "test-table2";
        sstSessions.createTable(TABLE1);
        Assert.assertEquals(1, sstSessions.openedTables().size());
        sstSessions.createTable(TABLE2);
        Assert.assertEquals(2, sstSessions.openedTables().size());
        Assert.assertTrue(sstSessions.existsTable(TABLE1));
        Assert.assertTrue(sstSessions.existsTable(TABLE2));

        // Write some data to sst file
        for (int i = 0; i < 1000; i++) {
            String k = String.format("%03d", i);
            sstSessions.session().put(TABLE1, b("person:" + k), b("James" + i));
        }
        for (int i = 0; i < 2000; i++) {
            String k = String.format("%04d", i);
            sstSessions.session().put(TABLE2, b("book:" + k), b("Java" + i));
        }
        sstSessions.session().commit();
        sstSessions.close();

        sstSessions.dropTable(TABLE1);
        sstSessions.dropTable(TABLE2);
        Assert.assertEquals(0, sstSessions.openedTables().size());
        Assert.assertFalse(sstSessions.existsTable(TABLE1));
        Assert.assertFalse(sstSessions.existsTable(TABLE2));

        RocksDBSessions rocks = new RocksDBStdSessions(config, "db", "store",
                                                       sstPath, sstPath);
        // Will ingest sst file of TABLE1
        rocks.createTable(TABLE1);
        Assert.assertEquals(ImmutableList.of("1000"),
                            rocks.property(RocksDBMetrics.KEY_NUM_KEYS));
        String value = s(rocks.session().get(TABLE1, b("person:001")));
        Assert.assertEquals("James1", value);
        value = s(rocks.session().get(TABLE1, b("person:010")));
        Assert.assertEquals("James10", value);
        value = s(rocks.session().get(TABLE1, b("person:999")));
        Assert.assertEquals("James999", value);

        // Will ingest sst file of TABLE2
        rocks.createTable(TABLE2);
        Assert.assertEquals(ImmutableList.of("1000", "2000"),
                            rocks.property(RocksDBMetrics.KEY_NUM_KEYS));
        value = s(rocks.session().get(TABLE2, b("book:0001")));
        Assert.assertEquals("Java1", value);
        value = s(rocks.session().get(TABLE2, b("book:0010")));
        Assert.assertEquals("Java10", value);
        value = s(rocks.session().get(TABLE2, b("book:0999")));
        Assert.assertEquals("Java999", value);
        value = s(rocks.session().get(TABLE2, b("book:1999")));
        Assert.assertEquals("Java1999", value);
    }
}
