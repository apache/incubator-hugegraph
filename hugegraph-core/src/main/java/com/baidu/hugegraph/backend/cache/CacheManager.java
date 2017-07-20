package com.baidu.hugegraph.backend.cache;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheManager {

    private static final Logger logger = LoggerFactory.getLogger(Cache.class);

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
                    logger.debug("Cache '{}' expiration tick", entry.getKey());
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
