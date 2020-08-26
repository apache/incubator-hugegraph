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
import java.util.Map;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;

import com.baidu.hugegraph.iterator.MapperIterator;
import com.baidu.hugegraph.util.E;

public class OptimisticCache<K, V> extends AbstractCache<K, V> {

    private final StampedLock stampedLock;
    private final Map<K, V> cache;

    public OptimisticCache(long capacity) {
        super(capacity);
        this.stampedLock = new StampedLock();
        this.cache = new HashMap<>();
    }

    @Override
    protected V access(K id) {
        final StampedLock stampedLock = this.stampedLock;
        long stamp = stampedLock.tryOptimisticRead();
        V value = this.cache.get(id);
        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                value = this.cache.get(id);
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return value;
    }

    @Override
    protected boolean write(K id, V value) {
        final StampedLock stampedLock = this.stampedLock;
        final long stamp = stampedLock.writeLock();
        try {
            this.cache.put(id, value);
        } finally {
            stampedLock.unlockWrite(stamp);
        }
        return true;
    }

    @Override
    protected void remove(K id) {
        final StampedLock stampedLock = this.stampedLock;
        final long stamp = stampedLock.writeLock();
        try {
            this.cache.remove(id);
        } finally {
            stampedLock.unlockWrite(stamp);
        }
    }

    @Override
    protected Iterator<CacheNode<K, V>> nodes() {
        final StampedLock stampedLock = this.stampedLock;
        final long stamp = stampedLock.readLock();
        try {
            Iterator<Map.Entry<K, V>> iter = this.cache.entrySet().iterator();
            return new MapperIterator<>(iter, kvEntry -> {
                return new CacheNode<>(kvEntry.getKey(), kvEntry.getValue());
            });
        } finally {
            stampedLock.unlockRead(stamp);
        }
    }

    @Override
    public boolean containsKey(K id) {
        final StampedLock stampedLock = this.stampedLock;
        long stamp = stampedLock.tryOptimisticRead();
        boolean existed = this.cache.containsKey(id);
        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                existed = this.cache.containsKey(id);
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return existed;
    }

    @Override
    public void traverse(Consumer<V> consumer) {
        E.checkNotNull(consumer, "consumer");
        final StampedLock stampedLock = this.stampedLock;
        final long stamp = stampedLock.readLock();
        try {
            this.cache.values().forEach(consumer);
        } finally {
            stampedLock.unlockRead(stamp);
        }
    }

    @Override
    public void clear() {
        final StampedLock stampedLock = this.stampedLock;
        final long stamp = stampedLock.writeLock();
        try {
            this.cache.clear();
        } finally {
            stampedLock.unlockWrite(stamp);
        }
    }

    @Override
    public long size() {
        final StampedLock stampedLock = this.stampedLock;
        long stamp = stampedLock.tryOptimisticRead();
        int size = this.cache.size();
        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                size = this.cache.size();
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return size;
    }
}
