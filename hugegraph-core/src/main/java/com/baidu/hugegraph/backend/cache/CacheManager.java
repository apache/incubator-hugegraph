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
import com.baidu.hugegraph.util.Log;

public class CacheManager {

    private static final Logger LOG = Log.logger(Cache.class);

    private static CacheManager INSTANCE = new CacheManager();

    // Check the cache expiration every 10s by default
    private static final long TIMER_TICK_PERIOD = 10;

    private final Map<String, Cache> caches;
    private final Timer timer;

    public static CacheManager instance() {
        return INSTANCE;
    }

    public CacheManager() {
        this.caches = new ConcurrentHashMap<>();
        this.timer = new Timer("Cache-Expirer", true);

        this.scheduleTimer(TIMER_TICK_PERIOD);
    }

    private TimerTask scheduleTimer(float period) {
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                for (Entry<String, Cache> entry : caches().entrySet()) {
                    LOG.debug("Cache '{}' expiration tick", entry.getKey());
                    entry.getValue().tick();
                }
            }
        };
        // The period in seconds
        this.timer.schedule(task, 0, (long) (period * 1000.0));
        return task;
    }

    public Map<String, Cache> caches() {
        return Collections.unmodifiableMap(this.caches);
    }

    public Cache cache(String name) {
        return this.cache(name, RamCache.DEFAULT_SIZE);
    }

    public Cache cache(String name, int capacity) {
        Cache cache = this.caches.get(name);
        if (cache == null) {
            cache = new RamCache(capacity);
            this.caches.put(name, cache);
        }
        return cache;
    }
}
