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

package com.baidu.hugegraph.backend.cache;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public class CacheManager {

    private static final Logger LOG = Log.logger(Cache.class);

    private static CacheManager INSTANCE = new CacheManager();

    // Check the cache expiration every 30s by default
    private static final long TIMER_TICK_PERIOD = 30;
    // Log if tick cost time > 1000ms
    private static final long LOG_TICK_COST_TIME = 1000L;

    private final Map<String, Cache<Id, Object>> caches;
    private final Timer timer;

    public static CacheManager instance() {
        return INSTANCE;
    }

    public CacheManager() {
        this.caches = new ConcurrentHashMap<>();
        this.timer = new Timer("cache-expirer", true);

        this.scheduleTimer(TIMER_TICK_PERIOD);
    }

    private TimerTask scheduleTimer(float period) {
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                try {
                    for (Entry<String, Cache<Id, Object>> entry :
                         caches().entrySet()) {
                        this.tick(entry.getKey(), entry.getValue());
                    }
                } catch (Throwable e) {
                    LOG.warn("An exception occurred when running tick", e);
                }
            }

            private void tick(String name, Cache<Id, Object> cache) {
                long start = System.currentTimeMillis();
                long items = cache.tick();
                long cost = System.currentTimeMillis() - start;
                if (cost > LOG_TICK_COST_TIME) {
                    LOG.info("Cache '{}' expired {} items cost {}ms > {}ms " +
                             "(size {}, expire {}ms)", name, items, cost,
                             LOG_TICK_COST_TIME, cache.size(), cache.expire());
                }
                LOG.debug("Cache '{}' expiration tick cost {}ms", name, cost);
            }
        };

        // Schedule task with the period in seconds
        this.timer.schedule(task, 0, (long) (period * 1000.0));

        return task;
    }

    public Map<String, Cache<Id, Object>> caches() {
        return Collections.unmodifiableMap(this.caches);
    }

    public Cache<Id, Object> cache(String name) {
        return this.cache(name, RamCache.DEFAULT_SIZE);
    }

    public Cache<Id, Object> cache(String name, long capacity) {
        if (!this.caches.containsKey(name)) {
            this.caches.putIfAbsent(name, new RamCache(capacity));
        }
        Cache<Id, Object> cache = this.caches.get(name);
        E.checkArgument(cache instanceof RamCache,
                        "Invalid cache implement: %s", cache.getClass());
        return cache;
    }

    public Cache<Id, Object> offheapCache(HugeGraph graph, String name,
                                          long capacity, long avgElemSize) {
        if (!this.caches.containsKey(name)) {
            OffheapCache cache = new OffheapCache(graph, capacity, avgElemSize);
            this.caches.putIfAbsent(name, cache);
        }
        Cache<Id, Object> cache = this.caches.get(name);
        E.checkArgument(cache instanceof OffheapCache,
                        "Invalid cache implement: %s", cache.getClass());
        return cache;
    }

    public Cache<Id, Object> levelCache(HugeGraph graph, String name,
                                        long capacity1, long capacity2,
                                        long avgElemSize) {
        if (!this.caches.containsKey(name)) {
            RamCache cache1 = new RamCache(capacity1);
            OffheapCache cache2 = new OffheapCache(graph, capacity2,
                                                   avgElemSize);
            this.caches.putIfAbsent(name, new LevelCache(cache1, cache2));
        }
        Cache<Id, Object> cache = this.caches.get(name);
        E.checkArgument(cache instanceof LevelCache,
                        "Invalid cache implement: %s", cache.getClass());
        return cache;
    }
}
