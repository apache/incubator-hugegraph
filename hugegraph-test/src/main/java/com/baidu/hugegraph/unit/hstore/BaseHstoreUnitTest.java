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

package com.baidu.hugegraph.unit.hstore;

import com.baidu.hugegraph.backend.store.hstore.HstoreOptions;
import com.baidu.hugegraph.backend.store.hstore.HstoreSessions;
import com.baidu.hugegraph.backend.store.hstore.HstoreSessionsImpl;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.backend.store.hstore.HstoreOptions;
import com.baidu.hugegraph.backend.store.hstore.HstoreSessions;
import com.baidu.hugegraph.backend.store.hstore.HstoreSessionsImpl;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.unit.BaseUnitTest;
import com.baidu.hugegraph.unit.FakeObjects;
import org.rocksdb.RocksDBException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import com.baidu.hugegraph.unit.FakeObjects;
import org.junit.After;
import org.junit.Before;
import org.rocksdb.RocksDBException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;

public class BaseHstoreUnitTest extends BaseUnitTest {

    protected static final String TABLE = "test-table";

    protected HstoreSessions sessions;

    @Before
    public void setup() throws RocksDBException {
        this.sessions = open(TABLE);
        this.sessions.session();
    }

    @After
    public void teardown() throws RocksDBException {
        this.clearData();
        close(this.sessions);
    }

    protected void put(byte[] ownerKey,String key, String value) {
        this.sessions.session().put(TABLE,ownerKey,b(key), b(value));
        this.commit();
    }

    protected String get(byte[] ownerKey,String key) throws RocksDBException {
        return s(this.sessions.session().get(TABLE,ownerKey,b(key)));
    }

    protected void clearData() {
        for (String table : new ArrayList<>(this.sessions.openedTables())) {
            this.sessions.session().deleteRange(table,new byte[]{0},new byte[]{-1},
                                                new byte[]{0}, new byte[]{-1});
        }
        this.commit();
    }

    protected void commit() {
        try {
            this.sessions.session().commit();
        } finally {
            this.sessions.session().rollback();
        }
    }

    protected static byte[] b(String str) {
        return str.getBytes();
    }

    protected static String s(byte[] bytes) {
        return bytes == null ? null : new String(bytes);
    }

    protected static byte[] b(long val) {
        ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.nativeOrder());
        buf.putLong(val);
        return buf.array();
    }

    protected static long l(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder());
        return buf.getLong();
    }

    private static HstoreSessions open(String table) throws RocksDBException {
        HugeConfig config = FakeObjects.newConfig();
        config.addProperty(HstoreOptions.PD_PEERS.name(),
                           "localhost:9000");
        HstoreSessions sessions = new HstoreSessionsImpl(config, "db", "store");
        return sessions;
    }

    private static void close(HstoreSessions rocks) throws RocksDBException {
        for (String table : new ArrayList<>(rocks.openedTables())) {
            if (table.equals("default")) {
                continue;
            }
            rocks.dropTable(table);
        }
        rocks.close();
    }
}
