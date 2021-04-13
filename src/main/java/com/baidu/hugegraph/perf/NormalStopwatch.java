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

package com.baidu.hugegraph.perf;

import java.util.List;
import java.util.function.BiFunction;

import org.slf4j.Logger;

import com.baidu.hugegraph.perf.PerfUtil.FastMap;
import com.baidu.hugegraph.perf.PerfUtil.LocalStack;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.util.Log;

public final class NormalStopwatch implements Stopwatch {

    private static final Logger LOG = Log.logger(Stopwatch.class);

    private static final String MULTI_THREAD_ACCESS_ERROR =
                         "There may be multi-threaded access, ensure " +
                         "not call PerfUtil.profileSingleThread(true) when " +
                         "multithreading.";

    private long lastStartTime = -1L;

    private long times = 0L;
    private long totalCost = 0L;
    private long minCost = Long.MAX_VALUE;
    private long maxCost = 0L;
    private long totalSelfWasted = 0L;
    private long totalChildrenWasted = -1L;
    private long totalChildrenTimes = -1L;

    private final String name;
    private final Path parent;
    private final Path id;
    private final FastMap<String, Stopwatch> children;

    public NormalStopwatch(String name, Stopwatch parent) {
        this(name, parent.id());
        parent.child(name, this);
    }

    public NormalStopwatch(String name, Path parent) {
        this.name = name;
        this.parent = parent;
        this.id = Stopwatch.id(parent, name);
        this.children = new FastMap<>();
    }

    @Override
    public Path id() {
        return this.id;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public Path parent() {
        return this.parent;
    }

    @Override
    public void lastStartTime(long startTime) {
        this.lastStartTime = startTime;
    }

    @Override
    public void startTime(long startTime) {
        assert this.lastStartTime == -1L : MULTI_THREAD_ACCESS_ERROR;

        this.times++;
        this.lastStartTime = startTime;

        long endTime = PerfUtil.now();
        long wastedTime = endTime - startTime;
        if (wastedTime <= 0L) {
            wastedTime += eachStartWastedLost;
        }

        this.totalSelfWasted += wastedTime;
    }

    @Override
    public void endTime(long startTime) {
        assert startTime >= this.lastStartTime && this.lastStartTime != -1L :
               MULTI_THREAD_ACCESS_ERROR;

        long endTime = PerfUtil.now();
        // The following code cost about 3ns~4ns
        long wastedTime = endTime - startTime;
        if (wastedTime <= 0L) {
            wastedTime += eachEndWastedLost;
        }

        long cost = endTime - this.lastStartTime;

        if (this.minCost > cost) {
            this.minCost = cost;
        }
        if (this.maxCost < cost) {
            this.maxCost = cost;
        }

        this.totalCost += cost;
        this.totalSelfWasted += wastedTime;
        this.lastStartTime = -1L;
    }

    @Override
    public long times() {
        return this.times;
    }

    @Override
    public long totalCost() {
        return this.totalCost;
    }

    @Override
    public void totalCost(long totalCost) {
        this.totalCost = totalCost;
    }

    @Override
    public long minCost() {
        return this.minCost;
    }

    @Override
    public long maxCost() {
        return this.maxCost;
    }

    @Override
    public long totalTimes() {
        if (this.totalChildrenTimes > 0L) {
            return this.times + this.totalChildrenTimes;
        }
        return this.times;
    }

    @Override
    public long totalChildrenTimes() {
        return this.totalChildrenTimes;
    }

    @Override
    public long totalWasted() {
        if (this.totalChildrenWasted > 0L) {
            return this.totalSelfWasted + this.totalChildrenWasted;
        }
        return this.totalSelfWasted;
    }

    @Override
    public long totalSelfWasted() {
        return this.totalSelfWasted;
    }

    @Override
    public long totalChildrenWasted() {
        return this.totalChildrenWasted;
    }

    @Override
    public void fillChildrenTotal(List<Stopwatch> children) {
        // Fill total wasted cost of children
        this.totalChildrenWasted = children.stream().mapToLong(
                                   c -> c.totalWasted()).sum();
        // Fill total times of children
        this.totalChildrenTimes = children.stream().mapToLong(
                                  c -> c.totalTimes()).sum();
    }

    @Override
    public Stopwatch copy() {
        try {
            return (Stopwatch) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Stopwatch child(String name) {
        return this.children.get(name);
    }

    @Override
    public Stopwatch child(String name, Stopwatch watch) {
        if (watch == null) {
            return this.children.remove(name);
        }
        return this.children.put(name, watch);
    }

    @Override
    public boolean empty() {
        return this.children.size() == 0;
    }

    @Override
    public void clear() {
        this.lastStartTime = -1L;

        this.times = 0L;
        this.totalCost = 0L;

        this.minCost = Long.MAX_VALUE;
        this.maxCost = 0L;
        this.totalSelfWasted = 0L;
        this.totalChildrenWasted = -1L;
        this.totalChildrenTimes = -1L;

        this.children.clear();
    }

    @Override
    public String toString() {
        return String.format("{parent:%s,name:%s," +
                             "times:%s,totalChildrenTimes:%s," +
                             "totalCost:%s,minCost:%s,maxCost:%s," +
                             "totalSelfWasted:%s,totalChildrenWasted:%s}",
                             this.parent, this.name,
                             this.times, this.totalChildrenTimes,
                             this.totalCost, this.minCost, this.maxCost,
                             this.totalSelfWasted, this.totalChildrenWasted);
    }

    private static long eachStartWastedLost = 0L;
    private static long eachEndWastedLost = 0L;

    protected static void initEachWastedLost() {
        int times = 100000000;

        LocalStack<Stopwatch> callStack = Whitebox.getInternalState(
                                          PerfUtil.instance(), "callStack");

        long baseStart = PerfUtil.now();
        for (int i = 0; i < times; i++) {
            PerfUtil.instance();
        }
        long baseCost = PerfUtil.now() - baseStart;

        BiFunction<String, Runnable, Long> testEachCost = (name, test) -> {
            long start = PerfUtil.now();
            test.run();
            long end = PerfUtil.now();
            long cost = end - start - baseCost;
            assert cost > 0;
            long eachCost = cost / times;

            LOG.info("Wasted time test: cost={}ms, base_cost={}ms, {}={}ns",
                     cost / 1000000.0, baseCost / 1000000.0, name, eachCost);
            return eachCost;
        };

        String startName = "each_start_cost";
        eachStartWastedLost = testEachCost.apply(startName, () -> {
            Stopwatch watch = PerfUtil.instance().start(startName);
            PerfUtil.instance().end(startName);
            for (int i = 0; i < times; i++) {
                // Test call start()
                PerfUtil.instance().start(startName);
                // Mock end()
                watch.lastStartTime(-1L);
                callStack.pop();
            }
        });

        String endName = "each_end_cost";
        eachEndWastedLost = testEachCost.apply(endName, () -> {
            Stopwatch watch = PerfUtil.instance().start(endName);
            PerfUtil.instance().end(endName);
            for (int i = 0; i < times; i++) {
                // Mock start()
                callStack.push(watch);
                watch.lastStartTime(0L);
                // Test call start()
                PerfUtil.instance().end(endName);
                watch.totalCost(0L);
            }
        });
    }
}
