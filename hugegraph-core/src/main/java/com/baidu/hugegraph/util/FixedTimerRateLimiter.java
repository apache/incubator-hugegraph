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

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.LongAdder;

/**
 * This class is used for fixed window to limit request per second
 * The different with stopwatch is to use timer for reducing count times
 *
 * TODO: Move to common module
 */
public class FixedTimerRateLimiter implements RateLimiter {

    private final Timer timer;
    private final LongAdder count;
    private final int limit;

    public FixedTimerRateLimiter(int limitPerSecond) {
        this.timer = new Timer("RateAuditLog", true);
        this.count = new LongAdder();
        this.limit = limitPerSecond;
        // Count will be reset if exceeds limit (run once per 1000ms)
        this.timer.schedule(new TimerTask() {
            @Override
            public void run() {
                if (count.intValue() > limit) {
                    count.reset();
                }
            }
        }, 300, ONE_SECOND);
    }

    @Override
    public boolean tryAcquire() {
        if (count.intValue() > limit) {
            return false;
        }

        count.increment();
        return true;
    }
}
