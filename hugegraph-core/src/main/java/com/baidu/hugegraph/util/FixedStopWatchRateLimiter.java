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

package com.baidu.hugegraph.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import com.google.common.base.Stopwatch;

/**
 * This class is used for fixed watch-window to rate limit request
 * Now just simplify for performance, don't need lock stop watch
 *
 * Note: This class is not thread safe
 * */
public class FixedStopWatchRateLimiter implements RateLimiter {

    private LongAdder count = new LongAdder();
    private Stopwatch watch;
    private static int limit;

    public FixedStopWatchRateLimiter(int limitPerSecond) {
        this.limit = limitPerSecond;
        this.watch = Stopwatch.createStarted();
        LOG.info("Audit log rate limit is '{}/s'", limitPerSecond);
    }

    @Override
    public boolean tryAcquire() {
        if (count.intValue() < limit) {
            count.increment();
            return true;
        }

        // Reset only if 1000ms elapsed
        if (watch.elapsed(TimeUnit.MILLISECONDS) >= ONE_SECOND) {
            count.reset();
            watch.reset();
            count.increment();
            return true;
        }
        return false;
    }
}
