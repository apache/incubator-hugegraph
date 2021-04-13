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

import com.baidu.hugegraph.perf.PerfUtil.FastMap;

public final class LightStopwatch implements Stopwatch {

    private long lastStartTime = -1L;

    private long times = 0L;
    private long totalCost = 0L;
    private long totalChildrenTimes = -1L;

    private final String name;
    private final Path parent;
    private final Path id;
    private final FastMap<String, Stopwatch> children;

    public LightStopwatch(String name, Stopwatch parent) {
        this(name, parent.id());
        parent.child(name, this);
    }

    public LightStopwatch(String name, Path parent) {
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
        this.times++;
        this.lastStartTime = startTime;
    }

    @Override
    public void endTime(long startTime) {
        this.totalCost += PerfUtil.now() - this.lastStartTime;
    }

    @Override
    public long times() {
        return this.times;
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
    public long totalCost() {
        return this.totalCost;
    }

    @Override
    public void totalCost(long totalCost) {
        this.totalCost = totalCost;
    }

    @Override
    public long minCost() {
        return -1L;
    }

    @Override
    public long maxCost() {
        return -1L;
    }

    @Override
    public long totalWasted() {
        return 0L;
    }

    @Override
    public long totalSelfWasted() {
        return 0L;
    }

    @Override
    public long totalChildrenWasted() {
        return -1L;
    }

    @Override
    public void fillChildrenTotal(List<Stopwatch> children) {
        // Fill total times of children
        this.totalChildrenTimes = children.stream().mapToLong(
                                  c -> c.totalTimes()).sum();
    }

    @Override
    public LightStopwatch copy() {
        try {
            return (LightStopwatch) super.clone();
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
        this.totalChildrenTimes = -1L;

        this.children.clear();
    }

    @Override
    public String toString() {
        return String.format("{parent:%s,name:%s," +
                             "times:%s,totalChildrenTimes:%s,totalCost:%s}",
                             this.parent, this.name,
                             this.times, this.totalChildrenTimes,
                             this.totalCost);
    }
}
