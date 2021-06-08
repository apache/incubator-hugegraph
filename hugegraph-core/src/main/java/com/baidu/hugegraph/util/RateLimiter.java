package com.baidu.hugegraph.util;

import org.slf4j.Logger;

public interface RateLimiter {

    public final Logger LOG = Log.logger(RateLimiter.class);
    public final long ONE_SECOND = 1000L;

    /**
     * Acquires one permits from RateLimiter if it can be acquired immediately
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
