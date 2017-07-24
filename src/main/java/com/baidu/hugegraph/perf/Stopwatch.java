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

package com.baidu.hugegraph.perf;

public class Stopwatch implements Cloneable {

    private long lastStartTime = -1L;

    private long totalCost = 0L;
    private long minCost = 0L;
    private long maxCost = 0L;

    private long times = 0L;

    private String name;
    private String parent;

    public Stopwatch(String name, String parent) {
        this.name = name;
        this.parent = parent;
    }

    public String id() {
        return Stopwatch.id(this.parent, this.name);
    }

    public static String id(String parent, String name) {
        if (parent == null || parent.isEmpty()) {
            return name;
        }
        return parent + "/" + name;
    }

    public String name() {
        return this.name;
    }

    public String parent() {
        return this.parent;
    }

    public void startTime(long time) {
        assert this.lastStartTime == -1L;

        this.lastStartTime = time;
        this.times++;
    }

    public void endTime(long time) {
        assert time >= this.lastStartTime && this.lastStartTime != -1L;

        long cost = time - this.lastStartTime;
        this.totalCost += cost;
        this.lastStartTime = -1L;
        this.updateMinMax(cost);
    }

    protected void updateMinMax(long cost) {
        if (this.minCost > cost || this.minCost == 0L) {
            this.minCost = cost;
        }
        if (this.maxCost < cost) {
            this.maxCost = cost;
        }
    }

    protected void totalCost(long totalCost) {
        this.totalCost = totalCost;
    }

    public long totalCost() {
        return this.totalCost;
    }

    public long minCost() {
        return this.minCost;
    }

    public long maxCost() {
        return this.maxCost;
    }

    public long times() {
        return this.times;
    }

    public Stopwatch copy() {
        try {
            return (Stopwatch) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return String.format(
                "{totalCost:%sms, minCost:%sns, maxCost:%sns, times:%s}",
                this.totalCost / 1000000.0F,
                this.minCost, this.maxCost,
                this.times);
    }

    public String toJson() {
        return String.format("{\"totalCost\":%s, " +
                "\"minCost\":%s, \"maxCost\":%s, \"times\":%s, " +
                "\"name\":\"%s\", \"parent\":\"%s\"}",
                this.totalCost,
                this.minCost,
                this.maxCost,
                this.times,
                this.name,
                this.parent);
    }
}
