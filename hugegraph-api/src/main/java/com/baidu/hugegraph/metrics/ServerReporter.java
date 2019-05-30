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

package com.baidu.hugegraph.metrics;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import com.baidu.hugegraph.util.E;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableSortedMap;

public class ServerReporter extends ScheduledReporter {

    private static volatile ServerReporter instance = null;

    private SortedMap<String, Gauge<?>> gauges;
    private SortedMap<String, Counter> counters;
    private SortedMap<String, Histogram> histograms;
    private SortedMap<String, Meter> meters;
    private SortedMap<String, Timer> timers;

    public static synchronized ServerReporter instance(
                                              MetricRegistry registry) {
        if (instance == null) {
            synchronized (ServerReporter.class) {
                if (instance == null) {
                    instance = new ServerReporter(registry);
                }
            }
        }
        return instance;
    }

    public static ServerReporter instance() {
        E.checkNotNull(instance, "Must instantiate ServerReporter before get");
        return instance;
    }

    private ServerReporter(MetricRegistry registry) {
        this(registry, SECONDS, MILLISECONDS, MetricFilter.ALL);
    }

    private ServerReporter(MetricRegistry registry, TimeUnit rateUnit,
                           TimeUnit durationUnit, MetricFilter filter) {
        super(registry, "server-reporter", filter, rateUnit, durationUnit);
        this.gauges = ImmutableSortedMap.of();
        this.counters = ImmutableSortedMap.of();
        this.histograms = ImmutableSortedMap.of();
        this.meters = ImmutableSortedMap.of();
        this.timers = ImmutableSortedMap.of();
    }

    public Map<String, Timer> timers() {
        return Collections.unmodifiableMap(this.timers);
    }

    public Map<String, Gauge<?>> gauges() {
        return Collections.unmodifiableMap(this.gauges);
    }

    public Map<String, Counter> counters() {
        return Collections.unmodifiableMap(this.counters);
    }

    public Map<String, Histogram> histograms() {
        return Collections.unmodifiableMap(this.histograms);
    }

    public Map<String, Meter> meters() {
        return Collections.unmodifiableMap(this.meters);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        this.gauges = (SortedMap) gauges;
        this.counters = counters;
        this.histograms = histograms;
        this.meters = meters;
        this.timers = timers;
    }
}
