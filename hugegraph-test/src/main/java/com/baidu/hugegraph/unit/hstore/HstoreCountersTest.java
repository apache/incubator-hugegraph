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

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.store.hstore.HstoreSessions.Session;
import com.baidu.hugegraph.backend.store.hstore.HstoreTables;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.type.HugeType;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDBException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.*;

public class HstoreCountersTest extends BaseHstoreUnitTest {

    private static final String DATABASE = "test-db";
    private static final int THREADS_NUM = 500;

    private HstoreTables.Counters counters;

    @Override
    @Before
    public void setup() throws RocksDBException {
        super.setup();
        this.counters = new HstoreTables.Counters(DATABASE);
        this.sessions.createTable(this.counters.table());
    }

    @Test
    public void testCounter()  {
        Session session = this.sessions.session();
        long property_keyId = this.counters.getCounterFromPd(session, HugeType.PROPERTY_KEY);
        for (int i = 1; i < 10000; i++) {
            this.counters.increaseCounter(session, HugeType.PROPERTY_KEY, 1L);
            long id = this.counters.getCounterFromPd(session, HugeType.PROPERTY_KEY);
            Assert.assertEquals(i+property_keyId, id);
        }
    }

    @Test
    public void testCounterWithMultiTypes() {
        Session session = this.sessions.session();
        long property_keyId = this.counters.getCounterFromPd(session, HugeType.PROPERTY_KEY);
        long vertex_labelId = this.counters.getCounterFromPd(session, HugeType.VERTEX_LABEL);
        long edge_labelId = this.counters.getCounterFromPd(session, HugeType.EDGE_LABEL);
        long index_labelId = this.counters.getCounterFromPd(session, HugeType.INDEX_LABEL);
        for (int i = 1; i < 10000; i++) {
            this.counters.increaseCounter(session, HugeType.PROPERTY_KEY, 1L);
            long id =  this.counters.getCounterFromPd(session, HugeType.PROPERTY_KEY);
            Assert.assertEquals(i+property_keyId, id);

            this.counters.increaseCounter(session, HugeType.VERTEX_LABEL, 1L);
            id = this.counters.getCounterFromPd(session, HugeType.VERTEX_LABEL);
            Assert.assertEquals(i+vertex_labelId, id);

            this.counters.increaseCounter(session, HugeType.EDGE_LABEL, 1L);
            id = this.counters.getCounterFromPd(session, HugeType.EDGE_LABEL);
            Assert.assertEquals(i+edge_labelId, id);

            this.counters.increaseCounter(session, HugeType.INDEX_LABEL, 1L);
            id = this.counters.getCounterFromPd(session, HugeType.INDEX_LABEL);
            Assert.assertEquals(i+index_labelId, id);
        }
    }

    @Test
    public void testCounterWithMutiThreads() {
        final int TIMES = 2000;
        Map<Id, Boolean> ids = new ConcurrentHashMap<>();
        Map<Id, Boolean> vlIds = new ConcurrentHashMap<>();
        Map<Id, Boolean> elIds = new ConcurrentHashMap<>();
        runWithThreads(THREADS_NUM, () -> {
            Session session = this.sessions.session();
            for (int i = 0; i < TIMES; i++) {
                Id id = nextId(session, HugeType.PROPERTY_KEY);
                Assert.assertFalse(ids.containsKey(id));
                ids.put(id, true);
                Id idVl = nextId(session, HugeType.VERTEX_LABEL);
                Assert.assertFalse(vlIds.containsKey(idVl));
                vlIds.put(idVl, true);
                Id idEl = nextId(session, HugeType.EDGE_LABEL);
                Assert.assertFalse(elIds.containsKey(idEl));
                elIds.put(idEl, true);
            }
            this.sessions.close();
        });
        long max=Long.MIN_VALUE;
        long min=Long.MAX_VALUE;
        for (Id id : ids.keySet()) {
            max = Math.max(max,id.asLong());
            min = Math.min(min,id.asLong());
        }
        Assert.assertEquals(THREADS_NUM * TIMES, max - min + 1 );
        Assert.assertEquals(THREADS_NUM * TIMES, ids.size());
        Assert.assertEquals(THREADS_NUM * TIMES, vlIds.size());
        Assert.assertEquals(THREADS_NUM * TIMES, elIds.size());
    }

    private Id nextId(Session session, HugeType type) {
        // Do get-increase-get-compare operation
        long counter = this.counters.getCounterFromPd(session, type);
        return IdGenerator.of(counter);
    }
}
