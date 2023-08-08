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

package org.apache.hugegraph.store.client.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 2022/1/29
 */
public class MetricX {
    public static AtomicLong iteratorSum = new AtomicLong();
    public static AtomicLong iteratorCount = new AtomicLong();
    public static AtomicLong iteratorMax = new AtomicLong();

    public AtomicLong failureCount = new AtomicLong();

    private long start;
    private long end;

    private MetricX(long start) {
        this.start = start;
    }

    public static MetricX ofStart() {
        return new MetricX(System.currentTimeMillis());
    }

    public static void plusIteratorWait(long nanoSeconds) {
        iteratorSum.addAndGet(nanoSeconds);
        iteratorCount.getAndIncrement();
        if (iteratorMax.get() < nanoSeconds) {
            iteratorMax.set(nanoSeconds);
        }
    }

    /**
     * amount of waiting
     *
     * @return millisecond
     */
    public static long getIteratorWait() {
        return iteratorSum.get() / 1_000_000;
    }

    /**
     * average of waiting
     *
     * @return millisecond
     */
    public static long getIteratorWaitAvg() {
        if (iteratorCount.get() == 0) {
            return -1;
        }
        return getIteratorWait() / iteratorCount.get();
    }

    /**
     * maximum of waiting
     *
     * @return millisecond
     */
    public static long getIteratorWaitMax() {
        return iteratorMax.get() / 1_000_000;
    }

    public static long getIteratorCount() {
        return iteratorCount.get();
    }

    public long start() {
        return this.start = System.currentTimeMillis();
    }

    public long end() {
        return this.end = System.currentTimeMillis();
    }

    public long past() {
        return this.end - this.start;
    }

    public void countFail() {
        this.failureCount.getAndIncrement();
    }

    public long getFailureCount() {
        return this.failureCount.get();
    }

}
