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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import com.baidu.hugegraph.iterator.MapperIterator;
import com.baidu.hugegraph.util.E;

public class ThreadLocalCache<K, V> extends AbstractCache<K, V> {

    private final List<Cache<K, V>> allCaches;
    private final ThreadLocal<Cache<K ,V>> localCache;

    public ThreadLocalCache(long capacity) {
        super(capacity);
        this.allCaches = new CopyOnWriteArrayList<>();
        this.localCache = new ThreadLocal<>();
    }

    @Override
    public boolean containsKey(K id) {
        return this.getOrNewCache().containsKey(id);
    }

    @Override
    public void traverse(Consumer<V> consumer) {
        E.checkNotNull(consumer, "consumer");
        this.getOrNewCache().values().forEach(consumer);
    }

    @Override
    public void clear() {
        this.allCaches.forEach(HashMap::clear);
    }

    @Override
    protected V access(K id) {
        return this.getOrNewCache().get(id);
    }

    @Override
    protected boolean write(K id, V value) {
        this.getOrNewCache();
        this.allCaches.forEach(cache -> cache.put(id, value));
        return true;
    }

    @Override
    protected void remove(K id) {
        this.allCaches.forEach(cache -> cache.remove(id));
    }

    @Override
    protected Iterator<CacheNode<K, V>> nodes() {
        Iterator<Map.Entry<K, V>> iter = this.getOrNewCache().entrySet().iterator();
        return new MapperIterator<>(iter, kvEntry -> {
            return new CacheNode<>(kvEntry.getKey(), kvEntry.getValue());
        });
    }

    @Override
    public long size() {
        return this.getOrNewCache().size();
    }

    private Cache<K, V> getOrNewCache() {
        Cache<K, V> cache = this.localCache.get();
        if (cache == null) {
            cache = new Cache<>();
            this.localCache.set(cache);
            this.allCaches.add(cache);
        }
        return cache;
    }

    private static class Cache<K, V> extends HashMap<K, V> {

        private static final long serialVersionUID = -3426955734239657957L;
    }
}
