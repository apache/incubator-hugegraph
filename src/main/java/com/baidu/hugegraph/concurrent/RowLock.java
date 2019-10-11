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

package com.baidu.hugegraph.concurrent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.baidu.hugegraph.util.E;

public class RowLock<K extends Comparable<K>> {

    private final Map<K, Lock> locks = new ConcurrentHashMap<>();
    private final ThreadLocal<Map<K, LocalLock>> localLocks =
                  ThreadLocal.withInitial(HashMap::new);

    public void lock(K key) {
        if (key == null) {
            return;
        }
        LocalLock localLock = this.localLocks.get().get(key);
        if (localLock != null) {
            localLock.lockCount++;
        } else {
            Lock current = new ReentrantLock();
            Lock previous = this.locks.putIfAbsent(key, current);
            if (previous != null) {
                current = previous;
            }
            current.lock();
            this.localLocks.get().put(key, new LocalLock(current));
        }
    }

    public void unlock(K key) {
        if (key == null) {
            return;
        }
        LocalLock localLock = this.localLocks.get().get(key);
        if (localLock == null) {
            return;
        }
        if (--localLock.lockCount == 0) {
            this.locks.remove(key, localLock.current);
            this.localLocks.get().remove(key);
            localLock.current.unlock();
        }
        E.checkState(localLock.lockCount >= 0,
                     "The lock count must be >= 0, but got %s",
                     localLock.lockCount);
    }

    public void lockAll(Set<K> keys) {
        if (keys == null) {
            return;
        }
        List<K> list = new ArrayList<>(keys);
        Collections.sort(list);
        for (K key : list) {
            this.lock(key);
        }
    }

    public void unlockAll(Set<K> keys) {
        if (keys == null) {
            return;
        }
        for (K key : keys) {
            this.unlock(key);
        }
    }

    private static class LocalLock {

        private final Lock current;
        private int lockCount;

        private LocalLock(Lock current) {
            this.current = current;
            this.lockCount = 1;
        }
    }
}
