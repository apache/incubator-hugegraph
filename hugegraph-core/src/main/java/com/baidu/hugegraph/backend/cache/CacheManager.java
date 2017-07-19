package com.baidu.hugegraph.backend.cache;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CacheManager {

    private static CacheManager INSTANCE = new CacheManager();

    private Map<String, Cache> caches;

    public static CacheManager instance() {
        return INSTANCE;
    }

    public CacheManager() {
        this.caches = new ConcurrentHashMap<>();
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
