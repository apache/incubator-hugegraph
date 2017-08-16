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

import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;

import com.baidu.hugegraph.util.Log;

public class AtomicLock {

    private static final Logger LOG = Log.logger(LockManager.class);

    private String name;
    private AtomicReference<Thread> sign;

    public AtomicLock(String name) {
        this.name = name;
        this.sign = new AtomicReference<>();
    }

    public boolean tryLock() {
        Thread current = Thread.currentThread();
        return this.sign.compareAndSet(null, current);
    }

    public void unlock() {
        Thread current = Thread.currentThread();
        if (!this.sign.compareAndSet(current, null)) {
            throw new RuntimeException(String.format(
                      "Thread '%s' trying to unlock '%s' " +
                      "which is held by other threads now.",
                      current.getName(), this.name));
        }
    }

    public boolean lock(int retries) {
        // The interval between retries is exponential growth, most wait
        // interval is 2^(retries-1)s. If retries=0, don't retry.
        if (retries < 0 || retries > 10) {
            throw new IllegalArgumentException(String.format(
                      "Locking retry times should be in [0, 10], but got %d",
                      retries));
        }

        boolean isLocked = false;
        try {
            for (int i = 0; !(isLocked = this.tryLock()) && i < retries; i++) {
                Thread.sleep(1000 * (1L << i));
            }
        } catch (InterruptedException ignored) {
            LOG.info("Thread sleep is interrupted.");
        }
        return isLocked;
    }

    public String name() {
        return this.name;
    }

    public void name(String name) {
        this.name = name;
    }
}
