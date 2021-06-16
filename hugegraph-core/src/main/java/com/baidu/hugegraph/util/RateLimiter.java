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

import org.slf4j.Logger;

// TODO: Move to common module
public interface RateLimiter {

    public final Logger LOG = Log.logger(RateLimiter.class);

    public final long RESET_PERIOD = 1000L;

    /**
     * Acquires one permit from RateLimiter if it can be acquired immediately
     * without delay.
     */
    public boolean tryAcquire();

    /**
     * Create a RateLimiter with specified rate, to keep compatible with
     * Guava's RateLimiter (use double now)
     *
     * @param ratePerSecond the rate of the returned RateLimiter, measured in
     *                      how many permits become available per second
     */
    public static RateLimiter create(double ratePerSecond) {
        return new FixedTimerRateLimiter((int) ratePerSecond);
    }
}
