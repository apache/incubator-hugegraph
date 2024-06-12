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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.meta.Partition;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * 2021/12/28
 */
public final class StoreMetrics {

    public final static String PREFIX = "hg";
    private final static HgStoreEngine storeEngine = HgStoreEngine.getInstance();
    private final static AtomicInteger graphs = new AtomicInteger(0);
    private static MeterRegistry registry;

    private StoreMetrics() {
    }

    public synchronized static void init(MeterRegistry meterRegistry) {
        if (registry == null) {
            registry = meterRegistry;
            registerMeters();
        }
    }

    private static void registerMeters() {
        Gauge.builder(PREFIX + ".up", () -> 1).register(registry);
        Gauge.builder(PREFIX + ".graphs", StoreMetrics::updateGraphs)
             .description("Number of graphs stored in this node")
             .register(registry);

    }

    private static int getGraphs() {
        return getGraphPartitions().size();
    }

    private static int updateGraphs() {
        int buf = getGraphs();
        if (buf != graphs.get()) {
            graphs.set(buf);
            registerPartitionGauge();
        }
        return buf;
    }

    private static void registerPartitionGauge() {
        Map<String, Map<Integer, Partition>> map = getGraphPartitions();

        map.forEach((k, v) -> Gauge.builder(PREFIX + ".partitions", new PartitionsGetter(k))
                                   .description("Number of partitions stored in the node")
                                   .tag("graph", k)
                                   .register(registry));

    }

    private static int getPartitions(String graph) {
        Map<Integer, Partition> map = getGraphPartitions().get(graph);
        if (map == null) {
            return 0;
        } else {
            return map.size();
        }
    }

    private static Map<String, Map<Integer, Partition>> getGraphPartitions() {
        Map<String, Map<Integer, Partition>> map =
                storeEngine.getPartitionManager().getPartitions();
        if (map == null) {
            return Collections.emptyMap();
        }
        return map;
    }

    private static class PartitionsGetter implements Supplier<Number> {

        private final String graph;

        PartitionsGetter(String graph) {
            this.graph = graph;
        }

        @Override
        public Number get() {
            return getPartitions(this.graph);
        }
    }
}
