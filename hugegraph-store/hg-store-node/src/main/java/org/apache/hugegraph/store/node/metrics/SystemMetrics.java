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

import java.lang.management.ClassLoadingMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadMXBean;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.util.Bytes;

@Deprecated
public class SystemMetrics {

    private static final long MB = Bytes.MB;

    private static long totalNonHeapMemory() {
        try {
            return ManagementFactory.getMemoryMXBean()
                                    .getNonHeapMemoryUsage()
                                    .getCommitted();
        } catch (Throwable ignored) {
            return 0;
        }
    }

    private static String formatName(String name) {
        return StringUtils.replace(name, " ", "_").toLowerCase();
    }

    public Map<String, Map<String, Object>> metrics() {
        Map<String, Map<String, Object>> metrics = new LinkedHashMap<>();
        metrics.put("basic", this.getBasicMetrics());
        metrics.put("heap", this.getHeapMetrics());
        metrics.put("nonheap", this.getNonHeapMetrics());
        metrics.put("thread", this.getThreadMetrics());
        metrics.put("class_loading", this.getClassLoadingMetrics());
        metrics.put("garbage_collector", this.getGarbageCollectionMetrics());

        return metrics;
    }

    private Map<String, Object> getBasicMetrics() {
        Map<String, Object> metrics = new LinkedHashMap<>();
        Runtime runtime = Runtime.getRuntime();
        // Heap allocated memory (measured in bytes)
        long total = runtime.totalMemory();
        // Heap free memory
        long free = runtime.freeMemory();
        long used = total - free;

        metrics.put("mem", (total + totalNonHeapMemory()) / MB);
        metrics.put("mem_total", total / MB);
        metrics.put("mem_used", used / MB);
        metrics.put("mem_free", free / MB);
        metrics.put("mem_unit", "MB");
        metrics.put("processors", runtime.availableProcessors());
        metrics.put("uptime", ManagementFactory.getRuntimeMXBean().getUptime());
        metrics.put("systemload_average",
                    ManagementFactory.getOperatingSystemMXBean()
                                     .getSystemLoadAverage());
        return metrics;
    }

    private Map<String, Object> getHeapMetrics() {
        Map<String, Object> metrics = new LinkedHashMap<>();
        MemoryUsage memoryUsage = ManagementFactory.getMemoryMXBean()
                                                   .getHeapMemoryUsage();
        metrics.put("committed", memoryUsage.getCommitted() / MB);
        metrics.put("init", memoryUsage.getInit() / MB);
        metrics.put("used", memoryUsage.getUsed() / MB);
        metrics.put("max", memoryUsage.getMax() / MB);
        return metrics;
    }

    private Map<String, Object> getNonHeapMetrics() {
        Map<String, Object> metrics = new LinkedHashMap<>();
        MemoryUsage memoryUsage = ManagementFactory.getMemoryMXBean()
                                                   .getNonHeapMemoryUsage();
        metrics.put("committed", memoryUsage.getCommitted() / MB);
        metrics.put("init", memoryUsage.getInit() / MB);
        metrics.put("used", memoryUsage.getUsed() / MB);
        metrics.put("max", memoryUsage.getMax() / MB);
        return metrics;
    }

    private Map<String, Object> getThreadMetrics() {
        Map<String, Object> metrics = new LinkedHashMap<>();
        ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
        metrics.put("peak", threadMxBean.getPeakThreadCount());
        metrics.put("daemon", threadMxBean.getDaemonThreadCount());
        metrics.put("total_started", threadMxBean.getTotalStartedThreadCount());
        metrics.put("count", threadMxBean.getThreadCount());
        return metrics;
    }

    private Map<String, Object> getClassLoadingMetrics() {
        Map<String, Object> metrics = new LinkedHashMap<>();
        ClassLoadingMXBean classLoadingMxBean = ManagementFactory
                .getClassLoadingMXBean();
        metrics.put("count", classLoadingMxBean.getLoadedClassCount());
        metrics.put("loaded", classLoadingMxBean.getTotalLoadedClassCount());
        metrics.put("unloaded", classLoadingMxBean.getUnloadedClassCount());
        return metrics;
    }

    private Map<String, Object> getGarbageCollectionMetrics() {
        Map<String, Object> metrics = new LinkedHashMap<>();
        List<GarbageCollectorMXBean> gcMxBeans = ManagementFactory
                .getGarbageCollectorMXBeans();
        for (GarbageCollectorMXBean gcMxBean : gcMxBeans) {
            String name = formatName(gcMxBean.getName());
            metrics.put(name + "_count", gcMxBean.getCollectionCount());
            metrics.put(name + "_time", gcMxBean.getCollectionTime());
        }
        metrics.put("time_unit", "ms");
        return metrics;
    }

}
