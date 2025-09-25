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

//@Deprecated
//public class SystemMemoryStats extends ProcfsRecord {
//
//    private static final int KB = 1024;
//    private final Map<MetricKey, AtomicLong> metrics = new HashMap<>();
//
//    public SystemMemoryStats() {
//        super(ProcFileHandler.getInstance("smaps"));
//    }
//
//    /* default */ SystemMemoryStats(ProcFileHandler reader) {
//        super(reader);
//    }
//
//    private static long parseKilobytes(String line) {
//        Objects.requireNonNull(line);
//        return Long.parseLong(line.split("\\s+")[1]);
//    }
//
//    @Override
//    protected void clear() {
//        EnumSet.allOf(MetricKey.class).forEach(key -> metrics.put(key, new AtomicLong(-1)));
//    }
//
//    @Override
//    protected void process(Collection<String> lines) {
//        Objects.requireNonNull(lines);
//
//        for (final String line : lines) {
//            if (line.startsWith("Size:")) {
//                increment(MetricKey.VSS, parseKilobytes(line) * KB);
//            } else if (line.startsWith("Rss:")) {
//                increment(MetricKey.RSS, parseKilobytes(line) * KB);
//            } else if (line.startsWith("Pss:")) {
//                increment(MetricKey.PSS, parseKilobytes(line) * KB);
//            } else if (line.startsWith("Swap:")) {
//                increment(MetricKey.SWAP, parseKilobytes(line) * KB);
//            } else if (line.startsWith("SwapPss:")) {
//                increment(MetricKey.SWAPPSS, parseKilobytes(line) * KB);
//            }
//        }
//    }
//
//    public Long getMetric(MetricKey key) {
//        Objects.requireNonNull(key);
//        clear();
//        return metrics.get(key).longValue();
//    }
//
//    private void increment(MetricKey key, long increment) {
//        Objects.requireNonNull(key);
//        metrics.get(key).getAndUpdate(currentValue -> currentValue + increment +
//                                                      (currentValue == -1 ? 1 : 0));
//    }
//
//    public enum MetricKey {
//        /**
//         * Virtual set size
//         */
//        VSS,
//        /**
//         * Resident set size
//         */
//        RSS,
//        /**
//         * Proportional set size
//         */
//        PSS,
//        /**
//         * Paged out memory
//         */
//        SWAP,
//        /**
//         * Paged out memory accounting shared pages. Since Linux 4.3.
//         */
//        SWAPPSS
//    }
//}
