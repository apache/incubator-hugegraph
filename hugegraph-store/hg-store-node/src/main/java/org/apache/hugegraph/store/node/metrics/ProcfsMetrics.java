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

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * 2022/3/1
 *
 * @version 0.1.0
 */
public class ProcfsMetrics {

    public final static String PREFIX = "process_memory";
    private final static SystemMemoryStats smaps = new SystemMemoryStats();
    private static MeterRegistry registry;

    private ProcfsMetrics() {

    }

    public synchronized static void init(MeterRegistry meterRegistry) {
        if (registry == null) {
            registry = meterRegistry;
            registerMeters();
        }
    }

    private static void registerMeters() {
        registerProcessGauge();
    }

    private static void registerProcessGauge() {
        Gauge.builder(PREFIX + ".rss.bytes", () -> smaps.getMetric(SystemMemoryStats.KEY.RSS))
             .register(registry);

        Gauge.builder(PREFIX + ".pss.bytes", () -> smaps.getMetric(SystemMemoryStats.KEY.PSS))
             .register(registry);

        Gauge.builder(PREFIX + ".vss.bytes", () -> smaps.getMetric(SystemMemoryStats.KEY.VSS))
             .register(registry);

        Gauge.builder(PREFIX + ".swap.bytes", () -> smaps.getMetric(SystemMemoryStats.KEY.SWAP))
             .register(registry);

        Gauge.builder(PREFIX + ".swappss.bytes", () -> smaps.getMetric(SystemMemoryStats.KEY.SWAPPSS))
             .register(registry);
    }

}
