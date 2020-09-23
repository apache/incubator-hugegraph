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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.slf4j.Logger;

import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.util.Log;

public abstract class AbstractCache<K, V> implements Cache<K, V> {

    public static final int MB = 1024 * 1024;
    public static final int DEFAULT_SIZE = 1 * MB;
    public static final int MAX_INIT_CAP = 100 * MB;

    protected static final Logger LOG = Log.logger(Cache.class);

    private volatile long hits = 0L;
    private volatile long miss = 0L;

    // Default expire time(ms)
    private volatile long expire = 0L;

    // NOTE: the count in number of items, not in bytes
    private final long capacity;
    private final long halfCapacity;

    // For user attachment
    private final AtomicReference<Object> attachment;

    public AbstractCache() {
        this(DEFAULT_SIZE);
    }

    public AbstractCache(long capacity) {
        if (capacity < 0L) {
            capacity = 0L;
        }
        this.capacity = capacity;
        this.halfCapacity = this.capacity >> 1;
        this.attachment = new AtomicReference<>();
    }

    @Watched(prefix = "cache")
    @Override
    public V get(K id) {
        if (id == null || this.capacity <= 0L) {
            return null;
        }
        V value = null;
        if (this.size() <= this.halfCapacity || this.containsKey(id)) {
            // Maybe the id removed by other threads and returned null value
            value = this.access(id);
        }

        if (value == null) {
            ++this.miss;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Cache missed '{}' (miss={}, hits={})",
                          id, this.miss, this.hits);
            }
        } else {
            ++this.hits;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Cache cached '{}' (hits={}, miss={})",
                          id, this.hits, this.miss);
            }
        }
        return value;
    }

    @Watched(prefix = "cache")
    @Override
    public V getOrFetch(K id, Function<K, V> fetcher) {
        if (id == null || this.capacity <= 0L) {
            return null;
        }
        V value = null;
        if (this.size() <= this.halfCapacity || this.containsKey(id)) {
            // Maybe the id removed by other threads and returned null value
            value = this.access(id);
        }

        if (value == null) {
            ++this.miss;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Cache missed '{}' (miss={}, hits={})",
                          id, this.miss, this.hits);
            }
            // Do fetch and update the cache
            value = fetcher.apply(id);
            this.update(id, value);
        } else {
            ++this.hits;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Cache cached '{}' (hits={}, miss={})",
                          id, this.hits, this.miss);
            }
        }
        return value;
    }

    @Watched(prefix = "cache")
    @Override
    public boolean update(K id, V value) {
        if (id == null || value == null || this.capacity <= 0L) {
            return false;
        }
        return this.write(id, value);
    }

    @Watched(prefix = "cache")
    @Override
    public boolean updateIfAbsent(K id, V value) {
        if (id == null || value == null ||
            this.capacity <= 0L || this.containsKey(id)) {
            return false;
        }
        return this.write(id, value);
    }

    @Watched(prefix = "cache")
    @Override
    public boolean updateIfPresent(K id, V value) {
        if (id == null || value == null ||
            this.capacity <= 0L || !this.containsKey(id)) {
            return false;
        }
        return this.write(id, value);
    }

    @Watched(prefix = "cache")
    @Override
    public void invalidate(K id) {
        if (id == null || this.capacity <= 0L || !this.containsKey(id)) {
            return;
        }
        this.remove(id);
    }

    @Override
    public void expire(long ms) {
        this.expire = ms;
    }

    @Override
    public final long expire() {
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
        for (Iterator<CacheNode<K, V>> it = this.nodes(); it.hasNext();) {
            CacheNode<K, V> node = it.next();
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
    public final long hits() {
        return this.hits;
    }

    @Override
    public final long miss() {
        return this.miss;
    }

    @Override
    public final long capacity() {
        return this.capacity;
    }

    @Override
    public <T> T attachment(T object) {
        this.attachment.compareAndSet(null, object);
        return this.attachment();
    }

    @Override
    public <T> T attachment() {
        @SuppressWarnings("unchecked")
        T attachment = (T) this.attachment.get();
        return attachment;
    }

    protected final long halfCapacity() {
        return this.halfCapacity;
    }

    protected abstract V access(K id);

    protected abstract boolean write(K id, V value);

    protected abstract void remove(K id);

    protected abstract Iterator<CacheNode<K, V>> nodes();

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
        public boolean equals(Object object) {
            if (!(object instanceof CacheNode)) {
                return false;
            }
            @SuppressWarnings("unchecked")
            CacheNode<K, V> other = (CacheNode<K, V>) object;
            return this.key.equals(other.key());
        }
    }
}
