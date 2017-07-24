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
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.baidu.hugegraph.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockGroup {

    private static final Logger logger =
            LoggerFactory.getLogger(Lock.class);

    private String name;
    private Map<String, Lock> locksMap;

    public LockGroup(String lockGroup) {
        this.name = lockGroup;
        this.locksMap = new ConcurrentHashMap();
    }

    public boolean lock(String lockName) {
        if (!this.locksMap.containsKey(lockName)) {
            this.locksMap.putIfAbsent(lockName, new Lock(lockName));
        }
        return this.locksMap.get(lockName).lock();
    }

    public boolean lock(String lockName, int retries) {
        // The interval between retries is exponential growth, most wait
        // interval is 2^(retries-1)s. If retries=0, don't retry.
        if (retries < 0 || retries > 10) {
            throw new IllegalArgumentException(String.format(
                "Locking retry times should be between 0 and 10, but got %d",
                retries));
        }

        if (!this.locksMap.containsKey(lockName)) {
            this.locksMap.putIfAbsent(lockName, new Lock(lockName));
        }

        boolean isLocked = false;
        try {
            for (int i = 0; !(isLocked = this.locksMap.get(lockName).lock()) &&
                            i < retries; i++) {
                Thread.sleep(1000 * (1L << i));
            }
        } catch (InterruptedException ignored) {
            logger.info("Thread sleep is interrupted.");
        }
        return isLocked;
    }

    public void unlock(String lockName) {
        if (!this.locksMap.containsKey(lockName)) {
            throw new RuntimeException(String.format(
                      "There is no lock '%s' found in LockGroup '%s'",
                      lockName, this.name));
        }
        this.locksMap.get(lockName).unlock();
    }

    public String name() {
        return this.name;
    }

    public void name(String name) {
        this.name = name;
    }
}
