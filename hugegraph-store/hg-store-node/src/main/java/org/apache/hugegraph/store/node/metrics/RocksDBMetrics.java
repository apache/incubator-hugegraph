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

import static org.apache.hugegraph.store.node.metrics.RocksDBMetricsConst.HISTOGRAMS;
import static org.apache.hugegraph.store.node.metrics.RocksDBMetricsConst.LABELS;
import static org.apache.hugegraph.store.node.metrics.RocksDBMetricsConst.LABEL_50;
import static org.apache.hugegraph.store.node.metrics.RocksDBMetricsConst.LABEL_95;
import static org.apache.hugegraph.store.node.metrics.RocksDBMetricsConst.LABEL_99;
import static org.apache.hugegraph.store.node.metrics.RocksDBMetricsConst.PREFIX;
import static org.apache.hugegraph.store.node.metrics.RocksDBMetricsConst.TICKERS;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.hugegraph.rocksdb.access.RocksDBFactory;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.node.util.HgAssert;
import org.rocksdb.HistogramData;
import org.rocksdb.HistogramType;
import org.rocksdb.MemoryUsageType;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

/**
 * 2021/12/30
 *
 * @version 1.2.0 on 2022/03/22 added auto meter removing when graph has been closed.
 */
@Slf4j
public class RocksDBMetrics {

    private final static RocksDBFactory rocksDBFactory = RocksDBFactory.getInstance();
    private final static AtomicInteger rocks = new AtomicInteger(0);
    private final static Set<String> graphSet = new HashSet<>();
    private final static HgStoreEngine storeEngine = HgStoreEngine.getInstance();
    private final static MemoryUseWrapper memoryUseWrapper = new MemoryUseWrapper();
    private final static Map<String, StatisticsWrapper> statisticsHolder = new HashMap<>();
    private final static Map<HistogramDataWrapper, HistogramType> histogramHolder = new HashMap<>();
    private final static Map<String, Set<Meter>> graphMeterMap = new ConcurrentHashMap<>();
    private static MeterRegistry registry;

    private RocksDBMetrics() {
    }

    public static void init(final MeterRegistry meterRegistry) {
        HgAssert.isArgumentNotNull(meterRegistry, "meterRegistry");

        if (registry != null) {
            return;

        }

        registry = meterRegistry;

        Gauge.builder("rocks.num", RocksDBMetrics::updateRocks)
             .description("Number of instance of RocksDB running in this node")
             .register(registry);

        registerMemoryUse();
    }

    private static int updateRocks() {
        int buf = getRocks();

        if (buf != rocks.get()) {
            rocks.set(buf);
            registerMeter();
        }

        return buf;
    }

    private static int getRocks() {
        return rocksDBFactory.getSessionSize();
    }

    private static Set<String> getGraphs() {
        return rocksDBFactory.getGraphNames();
    }

    private static RocksDBSession getRocksDBSession(String graph) {
        return rocksDBFactory.queryGraphDB(graph);
    }

    private static synchronized void registerMeter() {
        Set<String> graphs = getGraphs();

        if (graphs == null) {
            log.error(
                    "Failed to fetch the collection of names of graph, when invoking to register " +
                    "RocksDB gauge.");
            return;
        }

        graphs.forEach(g -> {
            if (!graphSet.add(g)) {
                return;
            }

            StatisticsWrapper stats = new StatisticsWrapper(g);
            statisticsHolder.put(g, stats);

            for (final TickerType ticker : TICKERS) {
                String gaugeName = PREFIX + "." + ticker.name().toLowerCase();

                saveGraphMeter(g,
                               Gauge.builder(gaugeName, () -> stats.getTickerCount(ticker))
                                    .description("RocksDB reported statistics for " + ticker.name())
                                    .tag("graph", g)
                                    .register(registry)
                );
            }

            for (final HistogramType histogram : HISTOGRAMS) {
                registerHistogram(g, registry, histogram, stats);
            }

            registrySessionRefNum(g);

        });

        graphSet.removeAll(graphSet.stream().filter(g -> !graphs.contains(g))
                                   .peek(g -> removeMeters(g))
                                   .collect(Collectors.toList())
        );

    }

    private static void saveGraphMeter(String g, Meter meter) {
        graphMeterMap.computeIfAbsent(g, k -> new HashSet<>()).add(meter);
    }

    private static void removeMeters(String g) {
        graphMeterMap.getOrDefault(g, Collections.emptySet()).forEach(e -> registry.remove(e));
    }

    private static void registerHistogram(String graph, MeterRegistry registry, HistogramType
            histogramType, StatisticsWrapper stats) {

        HistogramDataWrapper histogram = new HistogramDataWrapper(histogramType,
                                                                  () -> stats.getHistogramData(
                                                                          histogramType));
        histogramHolder.put(histogram, histogramType);

        String baseName = PREFIX + "." + histogramType.name().toLowerCase();
        saveGraphMeter(graph,
                       Gauge.builder(baseName + ".max", histogram, HistogramDataWrapper::getMax)
                            .tag("graph", graph).register(registry));
        saveGraphMeter(graph, Gauge.builder(baseName + ".mean", histogram,
                                            HistogramDataWrapper::getAverage).tag("graph", graph)
                                   .register(registry));
        saveGraphMeter(graph,
                       Gauge.builder(baseName + ".min", histogram, HistogramDataWrapper::getMin)
                            .tag("graph", graph).register(registry));

        baseName = baseName + ".summary";
        saveGraphMeter(graph, Gauge.builder(baseName, histogram, HistogramDataWrapper::getMedian)
                                   .tags("graph", graph, LABELS, LABEL_50).register(registry));
        saveGraphMeter(graph,
                       Gauge.builder(baseName, histogram, HistogramDataWrapper::getPercentile95)
                            .tags("graph", graph, LABELS, LABEL_95).register(registry));
        saveGraphMeter(graph,
                       Gauge.builder(baseName, histogram, HistogramDataWrapper::getPercentile99)
                            .tags("graph", graph, LABELS, LABEL_99).register(registry));
        saveGraphMeter(graph,
                       Gauge.builder(baseName + ".sum", histogram, HistogramDataWrapper::getSum)
                            .tags("graph", graph).register(registry));
        saveGraphMeter(graph,
                       Gauge.builder(baseName + ".count", histogram, HistogramDataWrapper::getCount)
                            .tags("graph", graph).register(registry));

    }

    private static void registerMemoryUse() {
        Gauge.builder(PREFIX + ".table.reader.total", memoryUseWrapper,
                      (e) -> e.getTableReaderTotal())
             .description("The current number of threads in the pool.")
             .register(registry);
        Gauge.builder(PREFIX + ".mem.table.total", memoryUseWrapper, (e) -> e.getMemTableTotal())
             .description("The current number of threads in the pool.")
             .register(registry);
        Gauge.builder(PREFIX + ".mem.table.unFlushed", memoryUseWrapper,
                      (e) -> e.getMemTableUnFlushed())
             .description("The current number of threads in the pool.")
             .register(registry);
        Gauge.builder(PREFIX + ".cache.total", memoryUseWrapper, (e) -> e.getCacheTotal())
             .description("The current number of threads in the pool.")
             .register(registry);
        Gauge.builder(PREFIX + ".block.cache.pinned-usage", memoryUseWrapper,
                      (e) -> e.getProperty("rocksdb.block-cache-pinned-usage"))
             .description("The current number of threads in the pool.")
             .register(registry);

    }

    private static void registrySessionRefNum(String graph) {

        SessionWrapper sessionWrapper = new SessionWrapper(graph);
        saveGraphMeter(graph,
                       Gauge.builder(PREFIX + ".session.ref.count", sessionWrapper,
                                     (e) -> e.getRefCount() - 1)
                            .description("The current amount of reference of session")
                            .tag("ref", "self").tag("graph", graph)
                            .strongReference(true)
                            .register(registry)
        );

    }

    private static <S, T> T getValue(S stat, Function<S, T> fun, T defaultValue) {
        if (stat == null) {
            return defaultValue;
        }
        return fun.apply(stat);
    }

    private static class SessionWrapper {

        private final String graphName;

        SessionWrapper(String graph) {

            this.graphName = graph;
        }

        public int getRefCount() {
            try (RocksDBSession session = getRocksDBSession(graphName)) {
                if (session != null) {
                    return getValue(session, e -> e.getRefCount(), -1);
                }
                return 0;
            }
        }
    }

    private static class MemoryUseWrapper {

        Map<MemoryUsageType, Long> mems = null;
        long lastTime = 0;

        private void loadData() {
            if (mems == null || System.currentTimeMillis() - lastTime > 30000) {
                mems = storeEngine.getBusinessHandler().getApproximateMemoryUsageByType(null);
                lastTime = System.currentTimeMillis();
            }
        }

        public Long getTableReaderTotal() {
            loadData();
            return mems.get(MemoryUsageType.kTableReadersTotal);
        }

        public Long getMemTableTotal() {
            loadData();
            return mems.get(MemoryUsageType.kMemTableTotal);
        }

        public Long getCacheTotal() {
            loadData();
            return mems.get(MemoryUsageType.kCacheTotal);
        }

        public Long getMemTableUnFlushed() {
            loadData();
            return mems.get(MemoryUsageType.kMemTableUnFlushed);
        }

        public Long getProperty(String property) {
            Set<String> graphs = rocksDBFactory.getGraphNames();
            if (graphs.size() > 0) {
                try (RocksDBSession session = getRocksDBSession((String) graphs.toArray()[0])) {
                    if (session != null) {
                        return Long.parseLong(session.getProperty(property));
                    }
                }
            }
            return null;
        }
    }

    private static class StatisticsWrapper {

        private final String graphName;
        private final Map<TickerType, Long> tickerCounteMap = new ConcurrentHashMap<>();
        private final Map<HistogramType, HistogramData> histogramDataMap =
                new ConcurrentHashMap<>();
        long lastTime = 0;

        StatisticsWrapper(String graph) {

            this.graphName = graph;
            loadData();

        }

        private void loadData() {
            if (System.currentTimeMillis() - lastTime < 30000) {
                return;
            }
            lastTime = System.currentTimeMillis();
            try (RocksDBSession session = getRocksDBSession(graphName)) {
                if (session == null) {
                    //   log.error("Failed to fetch the RocksDBSession with graph's name: [ " +
                    //   graph + " ]");
                    return;
                }

                Statistics statistics = session.getRocksDbStats();
                for (final TickerType ticker : TICKERS) {
                    tickerCounteMap.put(ticker, statistics.getTickerCount(ticker));
                }

                for (final HistogramType histogram : HISTOGRAMS) {
                    histogramDataMap.put(histogram, statistics.getHistogramData(histogram));
                }
            }
        }

        public long getTickerCount(TickerType tickerType) {
            this.loadData();
            return tickerCounteMap.containsKey(tickerType) ? tickerCounteMap.get(tickerType) : 0;
        }

        public HistogramData getHistogramData(HistogramType histogramType) {
            this.loadData();
            return histogramDataMap.get(histogramType);
        }

    }

    private static class HistogramDataWrapper {

        private final Supplier<HistogramData> supplier;
        private final HistogramType histogramType;
        private HistogramData data = new HistogramData(0d, 0d, 0d, 0d, 0d);
        private long ts = System.currentTimeMillis() - 30_000;

        HistogramDataWrapper(HistogramType histogramType, Supplier<HistogramData> supplier) {
            this.supplier = supplier;
            this.histogramType = histogramType;
        }

        private HistogramData getData() {
            if (System.currentTimeMillis() - this.ts > 30_000) {
                HistogramData buf = this.supplier.get();
                if (buf != null) {
                    this.data = buf;
                    this.ts = System.currentTimeMillis();
                }
            }
            return this.data;
        }

        public double getMedian() {
            return getData().getMedian();
        }

        public double getPercentile95() {
            return getData().getPercentile95();
        }

        public double getPercentile99() {
            return getData().getPercentile99();
        }

        public double getAverage() {
            return getData().getAverage();
        }

        public double getStandardDeviation() {
            return getData().getStandardDeviation();
        }

        public double getMax() {
            return getData().getMax();
        }

        public long getCount() {
            return getData().getCount();
        }

        public long getSum() {
            return getData().getSum();
        }

        public double getMin() {
            return getData().getMin();
        }

    }
}
