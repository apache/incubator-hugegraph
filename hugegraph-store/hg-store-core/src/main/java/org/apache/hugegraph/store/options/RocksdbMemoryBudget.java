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

package org.apache.hugegraph.store.options;

import java.lang.management.ManagementFactory;

import com.sun.management.HotSpotDiagnosticMXBean;
import com.sun.management.OperatingSystemMXBean;

import lombok.extern.slf4j.Slf4j;

/**
 * Resolves rocksdb.total_memory_size, the budget RaftRocksdbOptions splits into the
 * write buffer manager's cache and the block cache. Both caches are off-heap, so an
 * unset or "0" budget is sized from the memory left after the heap, direct memory and
 * the fixed raft log cache, instead of a fixed number that can exceed a container limit.
 */
@Slf4j
public final class RocksdbMemoryBudget {

    public static final String KEY = "total_memory_size";

    static final long MIB = 1024L * 1024L;
    // The 1 GiB raft log block cache (RaftRocksdbOptions) plus thread stacks, metaspace
    // and other native allocations that sit outside the budget
    static final long RESERVED = 1024 * MIB + 512 * MIB;
    static final long MIN_AUTO = 256 * MIB;
    // RaftRocksdbOptions gives the write buffer manager 2/3 of the budget, and a 0-byte
    // write buffer manager means no memtable limit at all
    static final long MIN_EXPLICIT = MIB;
    // The value shipped before auto-sizing; a large host never gets more than it had
    static final long MAX_AUTO = 32_000_000_000L;

    private RocksdbMemoryBudget() {
    }

    /**
     * Returns the configured budget in bytes, or an automatic one when the value is
     * missing, blank or 0.
     */
    public static long resolve(String configured) {
        long value = isAuto(configured) ? 0L : parse(configured);
        long heap = Runtime.getRuntime().maxMemory();
        long physical = physicalMemory();
        if (value > 0) {
            if (value < MIN_EXPLICIT) {
                throw new IllegalArgumentException(String.format(
                        "rocksdb.total_memory_size must be 0 (auto) or at least %s bytes, " +
                        "got %s", MIN_EXPLICIT, value));
            }
            if (value < MIN_AUTO) {
                log.warn("rocksdb.total_memory_size={} is below the {} bytes auto-sizing " +
                         "never goes under", value, MIN_AUTO);
            }
            long direct = maxDirectMemory(heap);
            long left = physical - heap - direct - RESERVED;
            if (physical > 0 && value > left) {
                log.warn("rocksdb.total_memory_size={} exceeds the {} bytes left after the " +
                         "heap, direct memory and the {} bytes reserve (physical={}, heap={}, " +
                         "direct={}); the process may be OOM-killed",
                         value, left, RESERVED, physical, heap, direct);
            }
            return value;
        }
        long direct = maxDirectMemory(heap);
        long budget = auto(physical, heap, direct);
        log.info("rocksdb.total_memory_size is unset or 0, auto-sizing to {} bytes " +
                 "(physical={}, heap={}, direct={})", budget, physical, heap, direct);
        if (physical > 0 && physical - heap - direct - RESERVED < 2 * MIN_AUTO) {
            log.warn("Heap, direct memory and the {} bytes reserve leave too little of the " +
                     "{} bytes physical memory, rocksdb gets the {} bytes floor; lower -Xmx " +
                     "or -XX:MaxDirectMemorySize", RESERVED, physical, MIN_AUTO);
        }
        return budget;
    }

    static boolean isAuto(String configured) {
        return configured == null || configured.trim().isEmpty();
    }

    static long parse(String configured) {
        try {
            long value = Long.parseLong(configured.trim());
            if (value < 0) {
                throw new NumberFormatException("negative");
            }
            return value;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(String.format(
                    "rocksdb.total_memory_size must be a number of bytes, got '%s'",
                    configured), e);
        }
    }

    /**
     * Half of what is left after the heap, direct memory and the reserve, clamped to
     * [MIN_AUTO, MAX_AUTO]. Falls back to the heap size, the previous default, when
     * physical memory is unknown.
     */
    static long auto(long physical, long heap, long direct) {
        if (physical <= 0) {
            return Math.min(Math.max(heap, MIN_AUTO), MAX_AUTO);
        }
        long available = physical - heap - direct - RESERVED;
        return Math.min(Math.max(available / 2, MIN_AUTO), MAX_AUTO);
    }

    /**
     * The raft disruptor buffer size AppConfig derives from the budget when
     * raft.disruptorBufferSize is 0: the budget in GB, at least 1, rounded to a power
     * of two, times 32.
     */
    public static int disruptorBufferSize(long totalMemory) {
        // A budget under 1 GB would otherwise give log(0) and a zero-sized buffer
        int size = (int) Math.max(1, totalMemory / 1000 / 1000 / 1000);
        return (int) Math.pow(2, Math.round(Math.log(size) / Math.log(2))) * 32;
    }

    private static long physicalMemory() {
        try {
            // Reports the cgroup v1/v2 memory limit inside a container; older JDK 17
            // updates may not see a limit set only on a parent cgroup
            OperatingSystemMXBean os = ManagementFactory.getPlatformMXBean(
                    OperatingSystemMXBean.class);
            return os == null ? -1L : os.getTotalMemorySize();
        } catch (RuntimeException | LinkageError e) {
            log.warn("Failed to read physical memory size", e);
            return -1L;
        }
    }

    private static long maxDirectMemory(long heap) {
        try {
            HotSpotDiagnosticMXBean hotspot = ManagementFactory.getPlatformMXBean(
                    HotSpotDiagnosticMXBean.class);
            long direct = Long.parseLong(
                    hotspot.getVMOption("MaxDirectMemorySize").getValue());
            // 0 means unset, and the JVM then allows as much direct memory as heap
            return direct > 0 ? direct : heap;
        } catch (RuntimeException | LinkageError e) {
            log.warn("Failed to read MaxDirectMemorySize, assuming the heap size", e);
            return heap;
        }
    }
}
