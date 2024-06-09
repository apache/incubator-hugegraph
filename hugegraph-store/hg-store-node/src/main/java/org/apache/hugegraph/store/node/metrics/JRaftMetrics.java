/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.store.node.metrics;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.node.util.HgRegexUtil;

import com.alipay.sofa.jraft.core.NodeMetrics;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;

/**
 * 2022/1/4
 */
@Slf4j
public class JRaftMetrics {

    public final static String PREFIX = "jraft";
    public static final String LABELS = "quantile";
    public static final String LABEL_50 = "0.5";
    public static final String LABEL_75 = "0.75";
    public static final String LABEL_95 = "0.95";
    public static final String LABEL_98 = "0.98";
    public static final String LABEL_99 = "0.99";
    public static final String LABEL_999 = "0.999";
    private final static HgStoreEngine storeEngine = HgStoreEngine.getInstance();
    private final static AtomicInteger groups = new AtomicInteger(0);
    private final static Tag handleDataTag = Tag.of("handle", "data");
    // private final static Tag handleTxTag = Tag.of("handle", "tx"); //reservation
    private final static Set<String> groupSet = new HashSet<>();
    private final static String REGEX_REFINE_REPLICATOR = "(replicator)(.+?:\\d+)(.*)";
    private static MeterRegistry registry;

    private JRaftMetrics() {
    }

    public synchronized static void init(MeterRegistry meterRegistry) {
        if (registry == null) {
            registry = meterRegistry;
            registerMeters();
        }
    }

    private static void registerMeters() {
        Gauge.builder(PREFIX + ".groups", JRaftMetrics::updateGroups)
             .description("Number of raft-groups, which handled the data of graph.")
             .tags(Collections.singleton(handleDataTag))
             .register(registry);

    }

    private static int updateGroups() {
        int buf = getGroups();
        if (buf != groups.get()) {
            groups.set(buf);
            registerNodeMetrics();
        }
        return buf;
    }

    private static int getGroups() {
        return storeEngine.getRaftGroupCount();
    }

    private static Map<String, NodeMetrics> getRaftGroupMetrics() {
        Map<String, NodeMetrics> map = storeEngine.getNodeMetrics();

        if (map == null) {
            return Collections.emptyMap();
        }

        return map;
    }

    private static void registerNodeMetrics() {
        Map<String, NodeMetrics> map = getRaftGroupMetrics();

        synchronized (groupSet) {
            map.forEach((group, metrics) -> {
                if (!groupSet.add(group)) {
                    return;
                }

                metrics.getMetricRegistry().getGauges()
                       .forEach((k, v) -> registerGauge(group, k, v));
                metrics.getMetricRegistry().getMeters()
                       .forEach((k, v) -> registerMeter(group, k, v));
                metrics.getMetricRegistry().getCounters()
                       .forEach((k, v) -> registerCounter(group, k, v));
                metrics.getMetricRegistry().getTimers()
                       .forEach((k, v) -> registerTimer(group, k, v));
                metrics.getMetricRegistry().getHistograms()
                       .forEach((k, v) -> registerHistogram(group, k, v));
            });
        }

    }

    private static HistogramWrapper toWrapper(com.codahale.metrics.Histogram histogram) {
        return new HistogramWrapper(histogram);
    }

    private static String refineMetrics(String name, List<Tag> tags) {
        if (name == null || name.isEmpty()) {
            return name;
        }

        List<String> buf = HgRegexUtil.toGroupValues(REGEX_REFINE_REPLICATOR, name);
        String res = null;

        /*Extracted name of replicator into a tag.*/

        if (buf != null && buf.size() == 4) {
            res = buf.get(1) + buf.get(3);

            String value = buf.get(2);

            if (value != null && value.startsWith("-")) {
                value = value.substring(1);
            }

            tags.add(Tag.of("replicator", value));
        } else {
            res = name;
        }

        return res;
    }

    private static void registerHistogram(String group, String name,
                                          com.codahale.metrics.Histogram histogram) {
        if (histogram == null) {
            return;
        }

        List<Tag> tags = new LinkedList<>();
        tags.add(handleDataTag);
        tags.add(Tag.of("group", group));

        name = refineMetrics(name, tags);

        String baseName = PREFIX + "." + name.toLowerCase();

        HistogramWrapper wrapper = toWrapper(histogram);

        Gauge.builder(baseName + ".median", wrapper, (d) -> d.getSnapshot().getMedian())
             .tags(tags).register(registry);
        Gauge.builder(baseName + ".min", wrapper, (d) -> d.getSnapshot().getMin())
             .tags(tags).register(registry);
        Gauge.builder(baseName + ".max", wrapper, (d) -> d.getSnapshot().getMax())
             .tags(tags).register(registry);
        Gauge.builder(baseName + ".mean", wrapper, (d) -> d.getSnapshot().getMean())
             .tags(tags).register(registry);

        baseName = baseName + ".summary";
        Gauge.builder(baseName, wrapper, (d) -> d.getSnapshot().getMedian())
             .tags(tags).tag(LABELS, LABEL_50).register(registry);
        Gauge.builder(baseName, wrapper, (d) -> d.getSnapshot().get75thPercentile())
             .tags(tags).tag(LABELS, LABEL_75).register(registry);
        Gauge.builder(baseName, wrapper, (d) -> d.getSnapshot().get95thPercentile())
             .tags(tags).tag(LABELS, LABEL_95).register(registry);
        Gauge.builder(baseName, wrapper, (d) -> d.getSnapshot().get98thPercentile())
             .tags(tags).tag(LABELS, LABEL_98).register(registry);
        Gauge.builder(baseName, wrapper, (d) -> d.getSnapshot().get99thPercentile())
             .tags(tags).tag(LABELS, LABEL_99).register(registry);
        Gauge.builder(baseName, wrapper, (d) -> d.getSnapshot().get999thPercentile())
             .tags(tags).tag(LABELS, LABEL_999).register(registry);

        Gauge.builder(baseName + ".sum", wrapper,
                      (d) -> Arrays.stream(d.getSnapshot().getValues()).sum())
             .tags(tags).register(registry);
        Gauge.builder(baseName + ".count", wrapper, (d) -> d.getSnapshot().size())
             .tags(tags).register(registry);

    }

    private static void registerTimer(String group, String name, com.codahale.metrics.Timer timer) {
        List<Tag> tags = new LinkedList<>();
        tags.add(handleDataTag);
        tags.add(Tag.of("group", group));

        name = refineMetrics(name, tags);

        String baseName = PREFIX + "." + name.toLowerCase();

        Gauge.builder(baseName + ".count", timer, Timer::getCount)
             .tags(tags).register(registry);

        Gauge.builder(baseName + ".timer", timer, Timer::getCount)
             .tags(tags).tag("rate", "1m").register(registry);
        Gauge.builder(baseName + ".timer", timer, Timer::getCount)
             .tags(tags).tag("rate", "5m").register(registry);
        Gauge.builder(baseName + ".timer", timer, Timer::getCount)
             .tags(tags).tag("rate", "15m").register(registry);
        Gauge.builder(baseName + ".timer", timer, Timer::getCount)
             .tags(tags).tag("rate", "mean").register(registry);

    }

    private static void registerMeter(String group, String name, com.codahale.metrics.Meter meter) {
        List<Tag> tags = new LinkedList<>();
        tags.add(handleDataTag);
        tags.add(Tag.of("group", group));

        name = refineMetrics(name, tags);

        String baseName = PREFIX + "." + name.toLowerCase();

        Gauge.builder(baseName + ".count", meter, Meter::getCount)
             .tags(tags)
             .register(registry);

        Gauge.builder(baseName + ".rate", meter, Meter::getCount)
             .tags(tags).tag("rate", "1m")
             .register(registry);
        Gauge.builder(baseName + ".rate", meter, Meter::getCount)
             .tags(tags).tag("rate", "5m")
             .register(registry);
        Gauge.builder(baseName + ".rate", meter, Meter::getCount)
             .tags(tags).tag("rate", "15m")
             .register(registry);
        Gauge.builder(baseName + ".rate", meter, Meter::getCount)
             .tags(tags).tag("rate", "mean")
             .register(registry);

    }

    private static void registerCounter(String group, String name,
                                        com.codahale.metrics.Counter counter) {
        List<Tag> tags = new LinkedList<>();
        tags.add(handleDataTag);
        tags.add(Tag.of("group", group));

        name = refineMetrics(name, tags);

        name = name.toLowerCase();

        //Adapted a counter to be a gauge.
        Gauge.builder(PREFIX + "." + name + ".count", counter, Counter::getCount)
             .tags(tags).register(registry);
    }

    private static void registerGauge(String group, String name,
                                      com.codahale.metrics.Gauge<?> gauge) {
        List<Tag> tags = new LinkedList<>();
        tags.add(handleDataTag);
        tags.add(Tag.of("group", group));

        name = refineMetrics(name, tags);

        name = name.toLowerCase();

        if (gauge.getValue() instanceof Number) {
            Gauge.builder(PREFIX + "." + name, gauge, (g) -> ((Number) g.getValue()).doubleValue())
                 .tags(tags).register(registry);
        } else {
            Gauge.builder(PREFIX + "." + name, () -> 1.0)
                 .tags(tags)
                 .tag("str.gauge", String.valueOf(gauge.getValue())).register(registry);
        }

    }

    private static class HistogramWrapper {

        private final com.codahale.metrics.Histogram histogram;

        private Snapshot snapshot;
        private long ts = System.currentTimeMillis();

        HistogramWrapper(com.codahale.metrics.Histogram histogram) {
            this.histogram = histogram;
            this.snapshot = this.histogram.getSnapshot();
        }

        Snapshot getSnapshot() {
            if (System.currentTimeMillis() - this.ts > 30_000) {
                this.snapshot = this.histogram.getSnapshot();
                this.ts = System.currentTimeMillis();
            }
            return this.snapshot;
        }
    }

}
