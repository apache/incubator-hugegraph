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

package com.baidu.hugegraph.unit.cache;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.cache.CachedSchemaTransaction;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.unit.BaseUnitTest;
import com.baidu.hugegraph.unit.FakeObjects;
import com.baidu.hugegraph.util.Events;
import com.google.common.collect.ImmutableMap;

public class CachedSchemaTransactionTest extends BaseUnitTest {

    private CachedSchemaTransaction cache;
    private HugeGraphParams params;

    @Before
    public void setup() {
        HugeGraph graph = HugeFactory.open(FakeObjects.newConfig());
        this.params = Whitebox.getInternalState(graph, "params");
        this.cache = new CachedSchemaTransaction(this.params,
                                                 this.params.loadSchemaStore());
    }

    @After
    public void teardown() throws Exception {
        this.cache().graph().clearBackend();
        this.cache().graph().close();
    }

    private CachedSchemaTransaction cache() {
        Assert.assertNotNull(this.cache);
        return this.cache;
    }

    @Test
    public void testEventClear() throws Exception {
        CachedSchemaTransaction cache = this.cache();

        FakeObjects objects = new FakeObjects("unit-test");
        cache.addPropertyKey(objects.newPropertyKey(IdGenerator.of(1),
                                                    "fake-pk-1"));
        cache.addPropertyKey(objects.newPropertyKey(IdGenerator.of(2),
                                                    "fake-pk-2"));

        Assert.assertEquals(2L, Whitebox.invoke(cache, "idCache", "size"));
        Assert.assertEquals(2L, Whitebox.invoke(cache, "nameCache", "size"));

        Assert.assertEquals("fake-pk-1",
                            cache.getPropertyKey(IdGenerator.of(1)).name());
        Assert.assertEquals(IdGenerator.of(1),
                            cache.getPropertyKey("fake-pk-1").id());

        Assert.assertEquals("fake-pk-2",
                            cache.getPropertyKey(IdGenerator.of(2)).name());
        Assert.assertEquals(IdGenerator.of(2),
                            cache.getPropertyKey("fake-pk-2").id());

        this.params.schemaEventHub().notify(Events.CACHE, "clear",
                                            null, null).get();

        Assert.assertEquals(0L, Whitebox.invoke(cache, "idCache", "size"));
        Assert.assertEquals(0L, Whitebox.invoke(cache, "nameCache", "size"));

        Assert.assertEquals("fake-pk-1",
                            cache.getPropertyKey(IdGenerator.of(1)).name());
        Assert.assertEquals(IdGenerator.of(1),
                            cache.getPropertyKey("fake-pk-1").id());

        Assert.assertEquals("fake-pk-2",
                            cache.getPropertyKey(IdGenerator.of(2)).name());
        Assert.assertEquals(IdGenerator.of(2),
                            cache.getPropertyKey("fake-pk-2").id());

        Assert.assertEquals(2L, Whitebox.invoke(cache, "idCache", "size"));
        Assert.assertEquals(2L, Whitebox.invoke(cache, "nameCache", "size"));
    }

    @Test
    public void testEventInvalid() throws Exception {
        CachedSchemaTransaction cache = this.cache();

        FakeObjects objects = new FakeObjects("unit-test");
        cache.addPropertyKey(objects.newPropertyKey(IdGenerator.of(1),
                                                    "fake-pk-1"));
        cache.addPropertyKey(objects.newPropertyKey(IdGenerator.of(2),
                                                    "fake-pk-2"));

        Assert.assertEquals(2L, Whitebox.invoke(cache, "idCache", "size"));
        Assert.assertEquals(2L, Whitebox.invoke(cache, "nameCache", "size"));

        Assert.assertEquals("fake-pk-1",
                            cache.getPropertyKey(IdGenerator.of(1)).name());
        Assert.assertEquals(IdGenerator.of(1),
                            cache.getPropertyKey("fake-pk-1").id());

        Assert.assertEquals("fake-pk-2",
                            cache.getPropertyKey(IdGenerator.of(2)).name());
        Assert.assertEquals(IdGenerator.of(2),
                            cache.getPropertyKey("fake-pk-2").id());

        this.params.schemaEventHub().notify(Events.CACHE, "invalid",
                                            HugeType.PROPERTY_KEY,
                                            IdGenerator.of(1)).get();

        Assert.assertEquals(1L, Whitebox.invoke(cache, "idCache", "size"));
        Assert.assertEquals(1L, Whitebox.invoke(cache, "nameCache", "size"));

        Assert.assertEquals("fake-pk-1",
                            cache.getPropertyKey(IdGenerator.of(1)).name());
        Assert.assertEquals(IdGenerator.of(1),
                            cache.getPropertyKey("fake-pk-1").id());

        Assert.assertEquals("fake-pk-2",
                            cache.getPropertyKey(IdGenerator.of(2)).name());
        Assert.assertEquals(IdGenerator.of(2),
                            cache.getPropertyKey("fake-pk-2").id());

        Assert.assertEquals(2L, Whitebox.invoke(cache, "idCache", "size"));
        Assert.assertEquals(2L, Whitebox.invoke(cache, "nameCache", "size"));
    }

    @Test
    public void testGetSchema() throws Exception {
        CachedSchemaTransaction cache = this.cache();

        FakeObjects objects = new FakeObjects("unit-test");
        cache.addPropertyKey(objects.newPropertyKey(IdGenerator.of(1),
                                                    "fake-pk-1"));

        this.params.schemaEventHub().notify(Events.CACHE, "clear",
                                            null, null).get();
        Assert.assertEquals("fake-pk-1",
                            cache.getPropertyKey(IdGenerator.of(1)).name());
        Assert.assertEquals(IdGenerator.of(1),
                            cache.getPropertyKey("fake-pk-1").id());

        this.params.schemaEventHub().notify(Events.CACHE, "clear",
                                            null, null).get();
        Assert.assertEquals(IdGenerator.of(1),
                            cache.getPropertyKey("fake-pk-1").id());
        Assert.assertEquals("fake-pk-1",
                            cache.getPropertyKey(IdGenerator.of(1)).name());
    }

    @Test
    public void testResetCachedAllIfReachedCapacity() throws Exception {
        CachedSchemaTransaction cache = this.cache();

        Object old = Whitebox.getInternalState(cache, "idCache.capacity");
        Whitebox.setInternalState(cache, "idCache.capacity", 2);
        try {
            Assert.assertEquals(0L, Whitebox.invoke(cache, "idCache", "size"));

            FakeObjects objects = new FakeObjects("unit-test");
            cache.addPropertyKey(objects.newPropertyKey(IdGenerator.of(1),
                                                        "fake-pk-1"));
            Assert.assertEquals(1L, Whitebox.invoke(cache, "idCache", "size"));
            Assert.assertEquals(1, cache.getPropertyKeys().size());
            Whitebox.invoke(CachedSchemaTransaction.class, "cachedTypes", cache);
            Assert.assertEquals(ImmutableMap.of(HugeType.PROPERTY_KEY, true),
                                Whitebox.invoke(CachedSchemaTransaction.class,
                                                "cachedTypes", cache));

            cache.addPropertyKey(objects.newPropertyKey(IdGenerator.of(3),
                                                        "fake-pk-2"));
            cache.addPropertyKey(objects.newPropertyKey(IdGenerator.of(2),
                                                        "fake-pk-3"));

            Assert.assertEquals(2L, Whitebox.invoke(cache, "idCache", "size"));
            Assert.assertEquals(3, cache.getPropertyKeys().size());
            Assert.assertEquals(ImmutableMap.of(),
                                Whitebox.invoke(CachedSchemaTransaction.class,
                                                "cachedTypes", cache));
        } finally {
            Whitebox.setInternalState(cache, "idCache.capacity", old);
        }
    }
}
