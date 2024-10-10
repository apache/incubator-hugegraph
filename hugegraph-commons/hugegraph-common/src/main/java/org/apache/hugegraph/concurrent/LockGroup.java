/*
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

package org.apache.hugegraph.concurrent;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockGroup {

    private final String name;
    private final Map<String, Object> locksMap;

    public LockGroup(String lockGroup) {
        this.name = lockGroup;
        this.locksMap = new ConcurrentHashMap<>();
    }

    public Lock lock(String lockName) {
        if (!this.locksMap.containsKey(lockName)) {
            this.locksMap.putIfAbsent(lockName, new ReentrantLock());
        }
        return (Lock) this.locksMap.get(lockName);
    }

    public AtomicLock atomicLock(String lockName) {
        if (!this.locksMap.containsKey(lockName)) {
            this.locksMap.putIfAbsent(lockName, new AtomicLock(lockName));
        }
        return (AtomicLock) this.locksMap.get(lockName);
    }

    public ReadWriteLock readWriteLock(String lockName) {
        if (!this.locksMap.containsKey(lockName)) {
            this.locksMap.putIfAbsent(lockName, new ReentrantReadWriteLock());
        }
        return (ReadWriteLock) this.locksMap.get(lockName);
    }

    public KeyLock keyLock(String lockName) {
        if (!this.locksMap.containsKey(lockName)) {
            this.locksMap.putIfAbsent(lockName, new KeyLock());
        }
        return (KeyLock) this.locksMap.get(lockName);
    }

    public KeyLock keyLock(String lockName, int size) {
        if (!this.locksMap.containsKey(lockName)) {
            this.locksMap.putIfAbsent(lockName, new KeyLock(size));
        }
        return (KeyLock) this.locksMap.get(lockName);
    }

    public <K extends Comparable<K>> RowLock<K> rowLock(String lockName) {
        if (!this.locksMap.containsKey(lockName)) {
            this.locksMap.putIfAbsent(lockName, new RowLock<>());
        }
        Object value = this.locksMap.get(lockName);
        @SuppressWarnings("unchecked")
        RowLock<K> lock = (RowLock<K>) value;
        return lock;
    }

    public String name() {
        return this.name;
    }
}
