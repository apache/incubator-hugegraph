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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.junit.Test;
import org.rocksdb.RocksDBException;

import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumn;
import com.baidu.hugegraph.backend.store.rocksdb.RocksDBSessions.Session;

public class RocksDBPerfTest extends BaseRocksDBUnitTest {

    private static final int TIMES = 10000 * 1000;

    @Test
    public void testPut() throws RocksDBException {
        for (int i = 0; i < TIMES; i++) {
            put("person-" + i, "value-" + i);
        }
    }

    @Test
    public void testGet3Keys() throws RocksDBException {

        put("person:1gname", "James");
        put("person:1gage", "19");
        put("person:1gcity", "Beijing");

        put("person:2gname", "Lisa");
        put("person:2gage", "20");
        put("person:2gcity", "Beijing");

        Session session = this.rocks.session();
        for (int i = 0; i < TIMES; i++) {
            s(session.get(TABLE, b("person:1gname")));
            s(session.get(TABLE, b("person:1gage")));
            s(session.get(TABLE, b("person:1gcity")));
        }
    }

    @Test
    public void testGet1KeyWithMultiValues() throws RocksDBException {

        put("person:1gname", "James");
        put("person:1gage", "19");
        put("person:1gcity", "Beijing");

        put("person:2gname", "Lisa");
        put("person:2gage", "20");
        put("person:2gcity", "Beijing");

        put("person:2all", "name=Lisa,age=20,city=Beijing");

        Session session = this.rocks.session();
        for (int i = 0; i < TIMES; i++) {
            s(session.get(TABLE, b("person:2all")));
        }
    }

    @Test
    public void testScanByPrefix() throws RocksDBException {

        put("person:1gname", "James");
        put("person:1gage", "19");
        put("person:1gcity", "Beijing");

        put("person:2gname", "Lisa");
        put("person:2gage", "20");
        put("person:2gcity", "Beijing");

        Session session = this.rocks.session();
        for (int i = 0; i < TIMES; i++) {
            Iterator<BackendColumn> iter = session.scan(TABLE, b("person:1"));
            while (iter.hasNext()) {
                BackendColumn col = iter.next();
                s(col.name);
                s(col.value);
            }
        }
    }

    @Test
    public void testGet3KeysWithData() throws RocksDBException {
        testPut();
        testGet3Keys();
    }

    @Test
    public void testGet1KeyWithData() throws RocksDBException {
        testPut();
        testGet1KeyWithMultiValues();
    }

    @Test
    public void testScanByPrefixWithData() throws RocksDBException {
        testPut();
        testScanByPrefix();
    }

    @Test
    public void testUpdate() throws RocksDBException {
        Session session = this.rocks.session();

        Random r = new Random();
        Map<Integer, Integer> comms = new HashMap<>();
        byte[] empty = new byte[0];

        int n = 1000;
        for (int i = 0; i < n; i++) {
            int value = i;
            comms.put(i, value);
            String key = String.format("index:%3d:%d", i, value);
            session.put(TABLE, b(key), empty);
        }
        session.commit();

        int updateTimes = 300; // 30w
        for (int j = 0; j < updateTimes; j++) {
            for (int i = 0; i < n; i++) {
                int value =  comms.get(i);
                String old = String.format("index:%3d:%d", i, value);
                session.delete(TABLE, b(old));

                value = r.nextInt(n); // TODO: aggregate
                value =  i + 1;
                comms.put(i, value);
                String key = String.format("index:%3d:%d", i, value);
                session.put(TABLE, b(key), empty);
            }
            session.commit();
        }
    }

    @Test
    public void testScanByPrefixAfterUpdate() throws RocksDBException {
        Session session = this.rocks.session();

        this.testUpdate();

        int n = 1000;
        int queryTimes = 300; // 30w
        for (int j = 0; j < queryTimes; j++) {
            for (int i = 0; i < n; i++) {
                String key = String.format("index:%3d", i);
                Iterator<BackendColumn> iter = session.scan(TABLE, b(key));
                while (iter.hasNext()) {
                    iter.next();
                }
            }
        }
    }
}
