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

import java.util.Iterator;
import java.util.function.Function;

import org.slf4j.Logger;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.util.Log;

public abstract class AbstractCache implements Cache {

    public static final int MB = 1024 * 1024;
    public static final int DEFAULT_SIZE = 1 * MB;
    public static final int MAX_INIT_CAP = 100 * MB;

    private static final Logger LOG = Log.logger(Cache.class);

    private volatile long hits = 0L;
    private volatile long miss = 0L;

    // Default expire time(ms)
    private volatile long expire = 0L;

    // NOTE: the count in number of items, not in bytes
    private final int capacity;
    private final int halfCapacity;

    public AbstractCache() {
        this(DEFAULT_SIZE);
    }

    public AbstractCache(int capacity) {
        if (capacity < 0) {
            capacity = 0;
        }
        this.capacity = capacity;
        this.halfCapacity = this.capacity >> 1;

        int initialCapacity = capacity >= MB ? capacity >> 10 : 256;
        if (initialCapacity > MAX_INIT_CAP) {
            initialCapacity = MAX_INIT_CAP;
        }
    }

    @Watched(prefix = "cache")
    @Override
    public Object get(Id id) {
        if (id == null) {
            return null;
        }
        Object value = null;
        if (this.size() <= this.halfCapacity || this.containsKey(id)) {
            // Maybe the id removed by other threads and returned null value
            value = this.access(id);
        }

        if (value == null) {
            ++this.miss;
            if (LOG.isDebugEnabled()) {
                LOG.debug("RamCache missed '{}' (miss={}, hits={})",
                          id, this.miss, this.hits);
            }
        } else {
            ++this.hits;
            if (LOG.isDebugEnabled()) {
                LOG.debug("RamCache cached '{}' (hits={}, miss={})",
                          id, this.hits, this.miss);
            }
        }
        return value;
    }

    @Watched(prefix = "cache")
    @Override
    public Object getOrFetch(Id id, Function<Id, Object> fetcher) {
        if (id == null) {
            return null;
        }
        Object value = null;
        if (this.size() <= this.halfCapacity || this.containsKey(id)) {
            // Maybe the id removed by other threads and returned null value
            value = this.access(id);
        }

        if (value == null) {
            ++this.miss;
            if (LOG.isDebugEnabled()) {
                LOG.debug("RamCache missed '{}' (miss={}, hits={})",
                          id, this.miss, this.hits);
            }
            // Do fetch and update the cache
            value = fetcher.apply(id);
            this.update(id, value);
        } else {
            ++this.hits;
            if (LOG.isDebugEnabled()) {
                LOG.debug("RamCache cached '{}' (hits={}, miss={})",
                          id, this.hits, this.miss);
            }
        }
        return value;
    }

    @Watched(prefix = "cache")
    @Override
    public void update(Id id, Object value) {
        if (id == null || value == null || this.capacity <= 0) {
            return;
        }
        this.write(id, value);
    }

    @Watched(prefix = "cache")
    @Override
    public void updateIfAbsent(Id id, Object value) {
        if (id == null || value == null ||
            this.capacity <= 0 || this.containsKey(id)) {
            return;
        }
        this.write(id, value);
    }

    @Watched(prefix = "cache")
    @Override
    public void updateIfPresent(Id id, Object value) {
        if (id == null || value == null ||
            this.capacity <= 0 || !this.containsKey(id)) {
            return;
        }
        this.write(id, value);
    }

    @Watched(prefix = "cache")
    @Override
    public void invalidate(Id id) {
        if (id == null || !this.containsKey(id)) {
            return;
        }
        this.remove(id);
    }

    @Override
    public void expire(long seconds) {
        // Convert the unit from seconds to milliseconds
        this.expire = seconds * 1000;
    }

    @Override
    public long expire() {
        return this.expire;
    }

    @Override
    public long tick() {
        long expireTime = this.expire;
        if (expireTime <= 0) {
            return 0L;
        }

        int expireItems = 0;
        long current = now();
        for (Iterator<CacheNode<Id, Object>> it = this.nodes(); it.hasNext();) {
            CacheNode<Id, Object> node = it.next();
            if (current - node.time() > expireTime) {
                // Remove item while iterating map (it must be ConcurrentMap)
                this.remove(node.key());
                expireItems++;
            }
        }

        if (expireItems > 0) {
            LOG.debug("Cache expired {} items cost {}ms (size {}, expire {}ms)",
                      expireItems, now() - current, this.size(), expireTime);
        }
        return expireItems;
    }

    @Override
    public long capacity() {
        return this.capacity;
    }

    @Override
    public long hits() {
        return this.hits;
    }

    @Override
    public long miss() {
        return this.miss;
    }

    protected abstract Object access(Id id);

    protected abstract void write(Id id, Object value);

    protected abstract void remove(Id id);

    protected abstract boolean containsKey(Id id);

    protected abstract <K, V> Iterator<CacheNode<K, V>> nodes();

    protected static final long now() {
        return System.currentTimeMillis();
    }

    protected static class CacheNode<K, V> {

        private final K key;
        private final V value;
        private long time;

        public CacheNode(K key, V value) {
            assert key != null;
            this.time = now();
            this.key = key;
            this.value = value;
        }

        public final K key() {
            return this.key;
        }

        public final V value() {
            return this.value;
        }

        public long time() {
            return this.time;
        }

        @Override
        public String toString() {
            return this.key.toString();
        }

        @Override
        public int hashCode() {
            return this.key.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof CacheNode)) {
                return false;
            }
            @SuppressWarnings("unchecked")
            CacheNode<K, V> other = (CacheNode<K, V>) obj;
            return this.key.equals(other.key());
        }
    }
}
