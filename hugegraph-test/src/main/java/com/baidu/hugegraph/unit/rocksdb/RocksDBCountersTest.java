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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDBException;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.store.rocksdb.RocksDBSessions.Session;
import com.baidu.hugegraph.backend.store.rocksdb.RocksDBTables;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.type.HugeType;

public class RocksDBCountersTest extends BaseRocksDBUnitTest {

    private static final String DATABASE = "test-db";
    private static final int THREADS_NUM = 8;

    private RocksDBTables.Counters counters;

    @Override
    @Before
    public void setup() throws RocksDBException {
        super.setup();
        this.counters = new RocksDBTables.Counters(DATABASE);
        this.rocks.createTable(this.counters.table());
    }

    @Test
    public void testCounter() throws RocksDBException {
        Session session = this.rocks.session();
        for (int i = 1; i < 10000; i++) {
            this.counters.increaseCounter(session, HugeType.PROPERTY_KEY, 1L);
            long id = this.counters.getCounter(session, HugeType.PROPERTY_KEY);
            Assert.assertEquals(i, id);
        }
    }

    @Test
    public void testCounterWithMultiTypes() throws RocksDBException {
        Session session = this.rocks.session();
        for (int i = 1; i < 1000; i++) {
            this.counters.increaseCounter(session, HugeType.PROPERTY_KEY, 1L);
            long id = this.counters.getCounter(session, HugeType.PROPERTY_KEY);
            Assert.assertEquals(i, id);

            this.counters.increaseCounter(session, HugeType.VERTEX_LABEL, 1L);
            id = this.counters.getCounter(session, HugeType.VERTEX_LABEL);
            Assert.assertEquals(i, id);

            this.counters.increaseCounter(session, HugeType.EDGE_LABEL, 1L);
            id = this.counters.getCounter(session, HugeType.EDGE_LABEL);
            Assert.assertEquals(i, id);

            this.counters.increaseCounter(session, HugeType.INDEX_LABEL, 1L);
            id = this.counters.getCounter(session, HugeType.INDEX_LABEL);
            Assert.assertEquals(i, id);
        }
    }

    @Test
    public void testCounterWithMutiThreads() {
        final int TIMES = 1000;

        AtomicLong times = new AtomicLong(0);
        Map<Id, Boolean> ids = new ConcurrentHashMap<>();

        runWithThreads(THREADS_NUM, () -> {
            Session session = this.rocks.session();

            for (int i = 0; i < TIMES; i++) {
                Id id = nextId(session, HugeType.PROPERTY_KEY);
                Assert.assertFalse(ids.containsKey(id));
                ids.put(id, true);

                times.incrementAndGet();
            }
            this.rocks.close();
        });
        Assert.assertEquals(THREADS_NUM * TIMES, times.get());
    }

    private Id nextId(Session session, HugeType type) {
        final int MAX_TIMES = 1000;
        // Do get-increase-get-compare operation
        long counter = 0L;
        long expect = -1L;
        synchronized(this) {
            for (int i = 0; i < MAX_TIMES; i++) {
                counter = this.counters.getCounter(session, type);

                if (counter == expect) {
                    break;
                }
                // Increase local counter
                expect = counter + 1L;
                // Increase remote counter
                this.counters.increaseCounter(session, type, 1L);
            }
        }
        return IdGenerator.of(expect);
    }
}
