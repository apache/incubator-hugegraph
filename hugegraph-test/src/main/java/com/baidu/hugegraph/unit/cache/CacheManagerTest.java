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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.baidu.hugegraph.backend.cache.Cache;
import com.baidu.hugegraph.backend.cache.CacheManager;
import com.baidu.hugegraph.backend.cache.RamCache;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.unit.BaseUnitTest;
import com.google.common.collect.ImmutableMap;

public class CacheManagerTest extends BaseUnitTest {

    private Map<String, Cache> originCaches;
    private Map<String, Cache> mockCaches;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Before
    public void setup() {
        CacheManager manager = CacheManager.instance();

        // Mock caches
        this.mockCaches = Mockito.mock(ConcurrentHashMap.class);

        this.originCaches = (Map) Whitebox.getInternalState(manager, "caches");
        Whitebox.setInternalState(manager, "caches", this.mockCaches);
    }

    @After
    public void teardown() {
        CacheManager manager = CacheManager.instance();

        Assert.assertNotNull(this.originCaches);
        Whitebox.setInternalState(manager, "caches", this.originCaches);
    }

    @Test
    public void testCacheGetPut() {
        final String name = "test-cache";

        CacheManager manager = CacheManager.instance();

        Mockito.when(this.mockCaches.containsKey(name)).thenReturn(false);
        final Cache[] cache = new Cache[1];
        Mockito.when(this.mockCaches.putIfAbsent(Mockito.anyString(), Mockito.any()))
               .thenAnswer(i -> cache[0] = (Cache) i.getArguments()[1]);
        Mockito.when(this.mockCaches.get(name)).thenAnswer(i -> cache[0]);

        Cache cache1 = manager.cache(name);

        Assert.assertNotNull(cache1);
        Mockito.verify(this.mockCaches).putIfAbsent(name, cache1);

        Mockito.when(this.mockCaches.containsKey(name)).thenReturn(true);
        Mockito.when(this.mockCaches.get(name)).thenReturn(cache1);
        Cache cache2 = manager.cache(name);

        Assert.assertSame(cache1, cache2);
        Mockito.verify(this.mockCaches, Mockito.atMost(1))
               .putIfAbsent(Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testCachePutGetWithCapacity() {
        final String name = "test-cache";
        final int capacity = 12345;

        CacheManager manager = CacheManager.instance();

        Mockito.when(this.mockCaches.containsKey(name)).thenReturn(false);
        final Cache[] cache = new Cache[1];
        Mockito.when(this.mockCaches.putIfAbsent(Mockito.anyString(), Mockito.any()))
               .thenAnswer(i -> cache[0] = (Cache) i.getArguments()[1]);
        Mockito.when(this.mockCaches.get(name)).thenAnswer(i -> cache[0]);

        Cache cache1 = manager.cache(name, capacity);

        Assert.assertNotNull(cache1);
        Assert.assertEquals(capacity, cache1.capacity());
        Mockito.verify(this.mockCaches).putIfAbsent(name, cache1);

        Mockito.when(this.mockCaches.containsKey(name)).thenReturn(true);
        Mockito.when(this.mockCaches.get(name)).thenReturn(cache1);
        Cache cache2 = manager.cache(name, capacity);

        Assert.assertEquals(capacity, cache2.capacity());
        Assert.assertSame(cache1, cache2);

        Assert.assertSame(cache1, manager.cache(name));
        Assert.assertSame(cache1, manager.cache(name, 0));
        Assert.assertSame(cache1, manager.cache(name, 1));
        Assert.assertSame(cache1, manager.cache(name, capacity));
        Assert.assertSame(cache1, manager.cache(name, capacity + 10));

        Mockito.verify(this.mockCaches, Mockito.atMost(1))
               .putIfAbsent(Mockito.anyString(), Mockito.any());
    }

    @Test
    public void testCacheList() {
        CacheManager manager = CacheManager.instance();

        Cache cache1 = manager.cache("cache-1");
        Cache cache2 = manager.cache("cache-2");

        Mockito.when(this.mockCaches.get("cache-1")).thenReturn(cache1);
        Mockito.when(this.mockCaches.get("cache-2")).thenReturn(cache2);
        Mockito.when(this.mockCaches.size()).thenReturn(2);

        Map<String, Cache> caches = manager.caches();

        Assert.assertEquals(2, caches.size());
        Assert.assertSame(cache1, caches.get("cache-1"));
        Assert.assertSame(cache2, caches.get("cache-2"));

        Assert.assertArrayEquals(this.mockCaches.values().toArray(),
                                 caches.values().toArray());
    }

    @Test
    public void testCacheListModify() {
        Assert.assertThrows(UnsupportedOperationException.class, () -> {
            CacheManager manager = CacheManager.instance();
            manager.caches().put("test", null);
        });
    }

    @Test
    public void testCacheExpire() {
        Cache cache1 = new RamCache();
        cache1.expire(32);

        Cache cache2 = new RamCache();
        cache2.expire(0);

        Cache mockCache1 = Mockito.spy(cache1);
        Cache mockCache2 = Mockito.spy(cache2);
        Map<String, Cache> caches = ImmutableMap.of("cache1", mockCache1,
                                                    "cache2", mockCache2);
        Mockito.when(this.mockCaches.entrySet()).thenReturn(caches.entrySet());

        cache1.update(IdGenerator.of("fake-id"), "fake-value");
        cache2.update(IdGenerator.of("fake-id"), "fake-value");

        waitTillNext(40);

        // Would call tick() per 10s
        Mockito.verify(mockCache1, Mockito.times(4)).tick();
        Mockito.verify(mockCache2, Mockito.times(4)).tick();

        Assert.assertEquals(0, cache1.size());
        Assert.assertEquals(1, cache2.size());
    }
}

