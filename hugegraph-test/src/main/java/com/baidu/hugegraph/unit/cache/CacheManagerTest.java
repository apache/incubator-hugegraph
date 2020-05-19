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

import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.baidu.hugegraph.backend.cache.Cache;
import com.baidu.hugegraph.backend.cache.CacheManager;
import com.baidu.hugegraph.backend.cache.OffheapCache;
import com.baidu.hugegraph.backend.cache.RamCache;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.unit.BaseUnitTest;
import com.google.common.collect.ImmutableMap;

public class CacheManagerTest extends BaseUnitTest {

    private Map<String, Cache<Id, Object>> originCaches;
    private Map<String, Cache<Id, Object>> mockCaches;

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
    public void testCacheInstance() {
        // Don't mock
        teardown();

        CacheManager manager = CacheManager.instance();

        Cache<Id, Object> c1 = manager.cache("c1");
        Cache<Id, Object> c12 = manager.cache("c1");
        Cache<Id, Object> c13 = manager.cache("c1", 123);
        Assert.assertEquals(c1, c12);
        Assert.assertEquals(c1, c13);
        Assert.assertEquals(c1.capacity(), c13.capacity());

        Cache<Id, Object> c2 = manager.offheapCache(null, "c2", 1, 11);
        Cache<Id, Object> c22 = manager.offheapCache(null, "c2", 2, 22);
        Cache<Id, Object> c23 = manager.offheapCache(null, "c2", 3, 33);
        Assert.assertEquals(c2, c22);
        Assert.assertEquals(c2, c23);
        Assert.assertEquals(c2.capacity(), c23.capacity());

        Cache<Id, Object> c3 = manager.levelCache(null, "c3", 1, 1, 11);
        Cache<Id, Object> c32 = manager.levelCache(null, "c3", 2, 2, 22);
        Cache<Id, Object> c33 = manager.levelCache(null, "c3", 3, 3, 33);
        Assert.assertEquals(c3, c32);
        Assert.assertEquals(c3, c33);
        Assert.assertEquals(c3.capacity(), c33.capacity());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            manager.cache("c2");
        }, e -> {
            Assert.assertContains("Invalid cache implement:", e.getMessage());
            Assert.assertContains("OffheapCache", e.getMessage());
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            manager.cache("c3");
        }, e -> {
            Assert.assertContains("Invalid cache implement:", e.getMessage());
            Assert.assertContains("LevelCache", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            manager.offheapCache(null, "c1", 1, 11);
        }, e -> {
            Assert.assertContains("Invalid cache implement:", e.getMessage());
            Assert.assertContains("RamCache", e.getMessage());
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            manager.offheapCache(null, "c3", 1, 11);
        }, e -> {
            Assert.assertContains("Invalid cache implement:", e.getMessage());
            Assert.assertContains("LevelCache", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            manager.levelCache(null, "c1", 1, 1, 11);
        }, e -> {
            Assert.assertContains("Invalid cache implement:", e.getMessage());
            Assert.assertContains("RamCache", e.getMessage());
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            manager.levelCache(null, "c2", 1, 1, 11);
        }, e -> {
            Assert.assertContains("Invalid cache implement:", e.getMessage());
            Assert.assertContains("OffheapCache", e.getMessage());
        });

        this.originCaches.remove("c1");
        this.originCaches.remove("c2");
        this.originCaches.remove("c3");
    }

    @Test
    public void testCacheGetPut() {
        final String name = "test-cache";

        CacheManager manager = CacheManager.instance();

        Mockito.when(this.mockCaches.containsKey(name)).thenReturn(false);
        @SuppressWarnings("rawtypes")
        final Cache[] cache = new Cache[1];
        Mockito.when(this.mockCaches.putIfAbsent(Mockito.anyString(),
                                                 Mockito.any()))
               .thenAnswer(i -> cache[0] = (Cache<?, ?>) i.getArguments()[1]);
        Mockito.when(this.mockCaches.get(name)).thenAnswer(i -> cache[0]);

        Cache<Id, Object> cache1 = manager.cache(name);

        Assert.assertNotNull(cache1);
        Mockito.verify(this.mockCaches).putIfAbsent(name, cache1);

        Mockito.when(this.mockCaches.containsKey(name)).thenReturn(true);
        Mockito.when(this.mockCaches.get(name)).thenReturn(cache1);
        Cache<Id, Object> cache2 = manager.cache(name);

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
        @SuppressWarnings("rawtypes")
        final Cache[] cache = new Cache[1];
        Mockito.when(this.mockCaches.putIfAbsent(Mockito.anyString(),
                                                 Mockito.any()))
               .thenAnswer(i -> cache[0] = (Cache<?, ?>) i.getArguments()[1]);
        Mockito.when(this.mockCaches.get(name)).thenAnswer(i -> cache[0]);

        Cache<Id, Object> cache1 = manager.cache(name, capacity);

        Assert.assertNotNull(cache1);
        Assert.assertEquals(capacity, cache1.capacity());
        Mockito.verify(this.mockCaches).putIfAbsent(name, cache1);

        Mockito.when(this.mockCaches.containsKey(name)).thenReturn(true);
        Mockito.when(this.mockCaches.get(name)).thenReturn(cache1);
        Cache<Id, Object> cache2 = manager.cache(name, capacity);

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

        Cache<Id, Object> cache1 = Mockito.mock(RamCache.class);
        Cache<Id, Object> cache2 = Mockito.mock(OffheapCache.class);

        Mockito.when(this.mockCaches.get("cache-1")).thenReturn(cache1);
        Mockito.when(this.mockCaches.get("cache-2")).thenReturn(cache2);
        Mockito.when(this.mockCaches.size()).thenReturn(2);

        Map<String, Cache<Id, Object>> caches = manager.caches();

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
        Cache<Id, Object> cache1 = new RamCache();
        cache1.expire(28 * 1000L);

        Cache<Id, Object> cache2 = new RamCache();
        cache2.expire(0);

        Cache<Id, Object> mockCache1 = Mockito.spy(cache1);
        Cache<Id, Object> mockCache2 = Mockito.spy(cache2);
        Map<String, Cache<Id, Object>> caches = ImmutableMap.of("cache1",
                                                                mockCache1,
                                                                "cache2",
                                                                mockCache2);
        Mockito.when(this.mockCaches.entrySet()).thenReturn(caches.entrySet());

        cache1.update(IdGenerator.of("fake-id"), "fake-value");
        cache2.update(IdGenerator.of("fake-id"), "fake-value");

        waitTillNext(40);

        // Would call tick() per 30s
        Mockito.verify(mockCache1, Mockito.times(1)).tick();
        Mockito.verify(mockCache2, Mockito.times(1)).tick();

        Assert.assertEquals(0, cache1.size());
        Assert.assertEquals(1, cache2.size());
    }

    @SuppressWarnings({ "unused", "unchecked" })
    private static Cache<Id, Object> newCacheProxy(Cache<Id, Object> cache) {
        Object p = Proxy.newProxyInstance(Cache.class.getClassLoader(),
                                          new Class[]{Cache.class},
                                          (proxy, method, args) -> {
                                              return method.invoke(cache, args);
                                          });
        return (Cache<Id, Object>) p;
    }
}

