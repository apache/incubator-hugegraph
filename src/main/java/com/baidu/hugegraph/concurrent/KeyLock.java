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
import java.util.List;
import java.util.concurrent.locks.Lock;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Striped;

/**
 * KeyLock provide an interface of segment lock
 */
public class KeyLock {

    private Striped<Lock> locks;

    public KeyLock() {
        // The default size is availableProcessors() * 4
        this(Runtime.getRuntime().availableProcessors() << 2);
    }

    public KeyLock(int size) {
        this.locks = Striped.lock(size);
    }

    private int indexOf(Lock lock) {
        for (int i = 0; i < this.locks.size(); i++) {
            if (this.locks.getAt(i) == lock) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Lock an object
     * @param key The object to lock
     * @return The lock(locked) of passed key
     */
    public final Lock lock(Object key) {
        Lock lock = this.locks.get(key);
        lock.lock();
        return lock;
    }

    /**
     * Unlock an object
     * @param key The object to unlock
     */
    public final void unlock(Object key) {
        this.locks.get(key).unlock();
    }

    /**
     * Lock a list of object with sorted order
     * @param keys The objects to lock
     * @return The locks(locked) of keys
     */
    public final List<Lock> lockAll(Object... keys) {
        List<Lock> locks = new ArrayList<>(keys.length);
        for (Object key : keys) {
            Lock lock = this.locks.get(key);
            locks.add(lock);
        }
        Collections.sort(locks, (a, b) -> {
            int diff = a.hashCode() - b.hashCode();
            if (diff == 0 && a != b) {
                diff = this.indexOf(a) - this.indexOf(b);
                assert diff != 0;
            }
            return diff;
        });
        for (int i = 0; i < locks.size(); i++) {
            locks.get(i).lock();
        }
        return Collections.unmodifiableList(locks);
    }

    /**
     * Lock two objects with sorted order
     * NOTE: This is to optimize the performance of lockAll(keys)
     * @param key1  The first object
     * @param key2  The second object
     * @return      locks for the two objects
     */
    public List<Lock> lockAll(Object key1, Object key2) {
        Lock lock1 = this.locks.get(key1);
        Lock lock2 = this.locks.get(key2);

        int diff = lock1.hashCode() - lock2.hashCode();
        if (diff == 0 && lock1 != lock2) {
            diff = this.indexOf(lock1) - this.indexOf(lock2);
            assert diff != 0;
        }

        List<Lock> locks = diff > 0 ?
                           ImmutableList.of(lock2, lock1) :
                           ImmutableList.of(lock1, lock2);

        for (int i = 0; i < locks.size(); i++) {
            locks.get(i).lock();
        }

        return locks;
    }

    /**
     * Unlock a list of object
     * @param locks The locks to unlock
     */
    public final void unlockAll(List<Lock> locks) {
        for (int i = locks.size(); i > 0; i--) {
            assert this.indexOf(locks.get(i - 1)) != -1;
            locks.get(i - 1).unlock();
        }
    }
}
