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

import com.codahale.metrics.Snapshot;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

public class MetricsUtil {

    private static final MetricRegistry registry =
                                        MetricManager.INSTANCE.getRegistry();

    private static final String STR_HELP = "# HELP ";
    private static final String STR_TYPE = "# TYPE ";
    private static final String HISTOGRAM_TYPE = "histogram";
    private static final String UNTYPED = "untyped";
    private static final String GAUGE_TYPE = "gauge";
    private static final String END_LSTR = "\n";
    private static final String SPACE_STR = " ";
    private static final String COUNT_ATTR = "{name=\"count\",} ";
    private static final String MIN_ATTR = "{name=\"min\",} ";
    private static final String MAX_ATTR = "{name=\"max\",} ";
    private static final String MEAN_ATTR = "{name=\"mean\",} ";
    private static final String STDDEV_ATTR = "{name=\"stddev\",} ";
    private static final String P50_ATTR = "{name=\"p50\",} ";
    private static final String P75_ATTR = "{name=\"p75\",} ";
    private static final String P95_ATTR = "{name=\"p95\",} ";
    private static final String P98_ATTR = "{name=\"p98\",} ";
    private static final String P99_ATTR = "{name=\"p99\",} ";
    private static final String P999_ATTR = "{name=\"p999\",} ";
    private static final String MEAN_RATE_ATRR = "{name=\"mean_rate\",} ";
    private static final String ONE_MIN_RATE_ATRR = "{name=\"m1_rate\",} ";
    private static final String FIVE_MIN_RATE_ATRR = "{name=\"m5_rate\",} ";
    private static final String FIFT_MIN_RATE_ATRR = "{name=\"m15_rate\",} ";

    public static <T> Gauge<T> registerGauge(Class<?> clazz, String name,
                                             Gauge<T> gauge) {
        return registry.register(MetricRegistry.name(clazz, name), gauge);
    }

    public static Counter registerCounter(Class<?> clazz, String name) {
        return registry.counter(MetricRegistry.name(clazz, name));
    }

    public static Histogram registerHistogram(Class<?> clazz, String name) {
        return registry.histogram(MetricRegistry.name(clazz, name));
    }

    public static Meter registerMeter(Class<?> clazz, String name) {
        return registry.meter(MetricRegistry.name(clazz, name));
    }

    public static Timer registerTimer(Class<?> clazz, String name) {
        return registry.timer(MetricRegistry.name(clazz, name));
    }

    public static void writePrometheus(StringBuilder promeMetrics,
                                       MetricRegistry registry) {
        // gauges
        registry.getGauges().forEach((key, gauge) -> {
            if (gauge != null) {
                String helpName = replaceDotDashInKey(key);
                promeMetrics.append(STR_HELP)
                        .append(helpName).append(END_LSTR);
                promeMetrics.append(STR_TYPE)
                        .append(helpName).append(SPACE_STR + GAUGE_TYPE + END_LSTR);
                promeMetrics.append(helpName).append(SPACE_STR).append(gauge.getValue()).append(END_LSTR);
            }
        });

        // histograms
        registry.getHistograms().forEach((key, histogram) -> {
            if (histogram != null) {
                String helpName = replaceDotDashInKey(key);
                promeMetrics.append(STR_HELP)
                        .append(helpName).append(END_LSTR);
                promeMetrics.append(STR_TYPE)
                        .append(helpName)
                        .append(SPACE_STR + HISTOGRAM_TYPE + END_LSTR);

                promeMetrics.append(helpName)
                        .append(COUNT_ATTR).append(histogram.getCount()).append(END_LSTR);
                promeMetrics.append(
                        exportSnapshort(helpName, histogram.getSnapshot()));
            }
        });

        // meters
        registry.getMeters().forEach((key, metric) -> {
            if (metric != null) {
                String helpName = replaceDotDashInKey(key);
                promeMetrics.append(STR_HELP)
                        .append(helpName).append(END_LSTR);
                promeMetrics.append(STR_TYPE)
                        .append(helpName)
                        .append(SPACE_STR + HISTOGRAM_TYPE + END_LSTR);

                promeMetrics.append(helpName)
                        .append(COUNT_ATTR).append(metric.getCount()).append(END_LSTR);
                promeMetrics.append(helpName)
                        .append(MEAN_RATE_ATRR).append(metric.getMeanRate()).append(END_LSTR);
                promeMetrics.append(helpName)
                        .append(ONE_MIN_RATE_ATRR).append(metric.getOneMinuteRate()).append(END_LSTR);
                promeMetrics.append(helpName)
                        .append(FIVE_MIN_RATE_ATRR).append(metric.getFiveMinuteRate()).append(END_LSTR);
                promeMetrics.append(helpName)
                        .append(FIFT_MIN_RATE_ATRR).append(metric.getFifteenMinuteRate()).append(END_LSTR);
            }
        });

        // timer
        registry.getTimers().forEach((key, timer) -> {
            if (timer != null) {
                String helpName = replaceDotDashInKey(key);
                promeMetrics.append(STR_HELP)
                        .append(helpName).append(END_LSTR);
                promeMetrics.append(STR_TYPE)
                        .append(helpName)
                        .append(SPACE_STR + HISTOGRAM_TYPE + END_LSTR);

                promeMetrics.append(helpName)
                        .append(COUNT_ATTR).append(timer.getCount()).append(END_LSTR);
                promeMetrics.append(helpName)
                        .append(ONE_MIN_RATE_ATRR).append(timer.getOneMinuteRate()).append(END_LSTR);
                promeMetrics.append(helpName)
                        .append(FIVE_MIN_RATE_ATRR).append(timer.getFiveMinuteRate()).append(END_LSTR);
                promeMetrics.append(helpName)
                        .append(FIFT_MIN_RATE_ATRR).append(timer.getFifteenMinuteRate()).append(END_LSTR);
                promeMetrics.append(
                        exportSnapshort(helpName, timer.getSnapshot()));
            }
        });
    }

    private static String replaceDotDashInKey(String orgKey){
        return orgKey.replace(".", "_").replace("-", "_");
    }

    private static String exportSnapshort(final String helpName, final Snapshot snapshot){
        if ( snapshot != null ) {
            StringBuilder snapMetrics = new StringBuilder();
            snapMetrics.append(helpName)
                    .append(MIN_ATTR).append(snapshot.getMin()).append(END_LSTR);
            snapMetrics.append(helpName)
                    .append(MAX_ATTR).append(snapshot.getMax()).append(END_LSTR);
            snapMetrics.append(helpName)
                    .append(MEAN_ATTR).append(snapshot.getMean()).append(END_LSTR);
            snapMetrics.append(helpName)
                    .append(STDDEV_ATTR).append(snapshot.getStdDev()).append(END_LSTR);
            snapMetrics.append(helpName)
                    .append(P50_ATTR).append(snapshot.getMedian()).append(END_LSTR);
            snapMetrics.append(helpName)
                    .append(P75_ATTR).append(snapshot.get75thPercentile()).append(END_LSTR);
            snapMetrics.append(helpName)
                    .append(P95_ATTR).append(snapshot.get95thPercentile()).append(END_LSTR);
            snapMetrics.append(helpName)
                    .append(P98_ATTR).append(snapshot.get98thPercentile()).append(END_LSTR);
            snapMetrics.append(helpName)
                    .append(P99_ATTR).append(snapshot.get99thPercentile()).append(END_LSTR);
            snapMetrics.append(helpName)
                    .append(P999_ATTR).append(snapshot.get999thPercentile()).append(END_LSTR);
            return  snapMetrics.toString();
        }
        return "";
    }

}
