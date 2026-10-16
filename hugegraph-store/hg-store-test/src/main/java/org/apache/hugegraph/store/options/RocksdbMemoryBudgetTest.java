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

import static org.apache.hugegraph.store.options.RocksdbMemoryBudget.MAX_AUTO;
import static org.apache.hugegraph.store.options.RocksdbMemoryBudget.MIB;
import static org.apache.hugegraph.store.options.RocksdbMemoryBudget.MIN_AUTO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import org.apache.hugegraph.config.ConfigException;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.rocksdb.access.RocksDBOptions;
import org.junit.Test;

public class RocksdbMemoryBudgetTest {

    private static final long GIB = 1024 * MIB;

    @Test
    public void testExplicitValueIsKept() {
        assertEquals(32_000_000_000L, RocksdbMemoryBudget.resolve("32000000000"));
        assertEquals(4_000_000_000L, RocksdbMemoryBudget.resolve(" 4000000000 "));
        assertEquals(MIB, RocksdbMemoryBudget.resolve(String.valueOf(MIB)));
    }

    @Test
    public void testExplicitValueBelowOneMibIsRefused() {
        for (String value : new String[]{"1", "2", String.valueOf(MIB - 1)}) {
            try {
                RocksdbMemoryBudget.resolve(value);
                fail("accepted " + value);
            } catch (IllegalArgumentException e) {
                assertTrue(e.getMessage().contains("at least"));
            }
        }
    }

    @Test
    public void testUnsetZeroOrBlankIsAuto() {
        for (String value : new String[]{null, "", "  ", "0", " 0 ", "00"}) {
            long budget = RocksdbMemoryBudget.resolve(value);
            assertTrue(String.valueOf(value), budget >= MIN_AUTO && budget <= MAX_AUTO);
        }
    }

    @Test
    public void testNonNumericValueIsRefused() {
        for (String value : new String[]{"4GB", "4g", "-1", "1.5e9", "99999999999999999999"}) {
            try {
                RocksdbMemoryBudget.resolve(value);
                fail("accepted " + value);
            } catch (IllegalArgumentException e) {
                assertTrue(e.getMessage().contains("number of bytes"));
            }
        }
    }

    @Test
    public void testAutoFitsContainerLimit() {
        // The setup from apache/hugegraph#3254: 4 GiB limit, 1 GiB heap, 512 MiB direct
        assertEquals(512 * MIB, RocksdbMemoryBudget.auto(4 * GIB, GIB, 512 * MIB));
        assertEquals(2560 * MIB, RocksdbMemoryBudget.auto(8 * GIB, GIB, 512 * MIB));
    }

    @Test
    public void testUnsetDirectMemoryIsCappedAtOneGib() {
        assertEquals(512 * MIB, RocksdbMemoryBudget.directMemory(512 * MIB, 8 * GIB));
        assertEquals(4 * GIB, RocksdbMemoryBudget.directMemory(4 * GIB, 8 * GIB));
        assertEquals(GIB, RocksdbMemoryBudget.directMemory(0L, 8 * GIB));
        assertEquals(512 * MIB, RocksdbMemoryBudget.directMemory(0L, 512 * MIB));
    }

    @Test
    public void testLargeHeapWithoutDirectCapKeepsAUsableBudget() {
        // Bare-metal Stores with a raised heap and no -XX:MaxDirectMemorySize; counting
        // the unset cap as a second heap gave all of these the 256 MiB floor
        assertEquals(2816 * MIB, RocksdbMemoryBudget.auto(
                16 * GIB, 8 * GIB, RocksdbMemoryBudget.directMemory(0L, 8 * GIB)));
        assertEquals(13056 * MIB, RocksdbMemoryBudget.auto(
                64 * GIB, 36 * GIB, RocksdbMemoryBudget.directMemory(0L, 36 * GIB)));
        // The default launcher caps the heap at 2 GiB
        assertEquals(5888 * MIB, RocksdbMemoryBudget.auto(
                16 * GIB, 2 * GIB, RocksdbMemoryBudget.directMemory(0L, 2 * GIB)));
    }

    @Test
    public void testAncestorCgroupV2LimitIsUsed() throws IOException {
        Path root = Files.createTempDirectory("cgroup-v2");
        try {
            // A 4 GiB limit on the pod, none on the container or above the pod
            write(root, "kubepods/memory.max", "max");
            write(root, "kubepods/pod1/memory.max", String.valueOf(4 * GIB));
            write(root, "kubepods/pod1/ctr/memory.max", "max\n");
            Path proc = write(root, "proc-self-cgroup", "0::/kubepods/pod1/ctr\n");
            long limit = RocksdbMemoryBudget.cgroupMemoryLimit(proc, root);
            assertEquals(4 * GIB, limit);

            // The case from review: a 64 GiB host reported by the JVM, -Xmx1g and
            // -XX:MaxDirectMemorySize=512m get the 4 GiB limit's budget, not host RAM's
            long physical = RocksdbMemoryBudget.effectivePhysical(64 * GIB, limit);
            assertEquals(512 * MIB, RocksdbMemoryBudget.auto(physical, GIB, 512 * MIB));

            // The container's own lower limit wins over the pod's
            write(root, "kubepods/pod1/ctr/memory.max", String.valueOf(2 * GIB));
            assertEquals(2 * GIB, RocksdbMemoryBudget.cgroupMemoryLimit(proc, root));
        } finally {
            FileUtils.deleteQuietly(root.toFile());
        }
    }

    @Test
    public void testCgroupV2NamespaceRootAndNoLimit() throws IOException {
        Path root = Files.createTempDirectory("cgroup-v2");
        try {
            // Inside a cgroup namespace the process sees "/" and the mount is its cgroup
            Path proc = write(root, "proc-self-cgroup", "0::/\n");
            write(root, "memory.max", "max");
            assertEquals(-1L, RocksdbMemoryBudget.cgroupMemoryLimit(proc, root));
            write(root, "memory.max", String.valueOf(3 * GIB));
            assertEquals(3 * GIB, RocksdbMemoryBudget.cgroupMemoryLimit(proc, root));

            // A path the mount doesn't show falls back to the mount root
            Path hidden = write(root, "proc-hidden", "0::/system.slice/hidden.service\n");
            assertEquals(3 * GIB, RocksdbMemoryBudget.cgroupMemoryLimit(hidden, root));
        } finally {
            FileUtils.deleteQuietly(root.toFile());
        }
    }

    @Test
    public void testCgroupV1HierarchicalLimitIsUsed() throws IOException {
        Path root = Files.createTempDirectory("cgroup-v1");
        try {
            Path proc = write(root, "proc-self-cgroup",
                              "12:cpu,cpuacct:/docker/abc\n4:memory:/docker/abc\n");
            // Docker mounts the container's own memory cgroup, so /docker/abc isn't there
            write(root, "memory/memory.limit_in_bytes", "9223372036854771712");
            write(root, "memory/memory.stat",
                  "cache 0\nhierarchical_memory_limit " + (4 * GIB) + "\n");
            assertEquals(4 * GIB, RocksdbMemoryBudget.cgroupMemoryLimit(proc, root));

            write(root, "memory/memory.stat", "hierarchical_memory_limit 9223372036854771712\n");
            assertEquals(-1L, RocksdbMemoryBudget.cgroupMemoryLimit(proc, root));
        } finally {
            FileUtils.deleteQuietly(root.toFile());
        }
    }

    @Test
    public void testUnreadableCgroupKeepsJvmMemory() {
        Path missing = Path.of("/nonexistent/proc/self/cgroup");
        assertEquals(-1L, RocksdbMemoryBudget.cgroupMemoryLimit(missing, Path.of("/nonexistent")));
        assertEquals(16 * GIB, RocksdbMemoryBudget.effectivePhysical(16 * GIB, -1L));
        assertEquals(4 * GIB, RocksdbMemoryBudget.effectivePhysical(-1L, 4 * GIB));
        assertEquals(4 * GIB, RocksdbMemoryBudget.effectivePhysical(4 * GIB, 8 * GIB));
    }

    private static Path write(Path root, String relative, String content) throws IOException {
        Path file = root.resolve(relative);
        Files.createDirectories(file.getParent());
        Files.write(file, content.getBytes(StandardCharsets.UTF_8));
        return file;
    }

    @Test
    public void testAutoIsClamped() {
        assertEquals(MIN_AUTO, RocksdbMemoryBudget.auto(2 * GIB, GIB, GIB));
        assertEquals(MAX_AUTO, RocksdbMemoryBudget.auto(1024 * GIB, 8 * GIB, 8 * GIB));
    }

    @Test
    public void testDisruptorBufferSizeIsNeverZero() {
        assertEquals(32, RocksdbMemoryBudget.disruptorBufferSize(512 * MIB));
        assertEquals(32, RocksdbMemoryBudget.disruptorBufferSize(1_000_000_000L));
        assertEquals(1024, RocksdbMemoryBudget.disruptorBufferSize(32_000_000_000L));
    }

    @Test
    public void testDerivedWriteBufferSizePassesRocksdbValidation() {
        // The 4 GiB container from apache/hugegraph#3254 auto-sizes to 512 MiB, whose
        // thousandth is under the 1 MiB rocksdb.write_buffer_size minimum
        long budget = RocksdbMemoryBudget.auto(4 * GIB, GIB, 512 * MIB);
        assertEquals(MIB, RocksdbMemoryBudget.writeBufferSize(budget));
        assertEquals(MIB, RocksdbMemoryBudget.writeBufferSize(MIB));
        assertEquals(32_000_000L, RocksdbMemoryBudget.writeBufferSize(32_000_000_000L));

        // The same validation BusinessHandlerImpl.initRocksdb() runs at Store startup
        OptionSpace.register("rocksdb", RocksDBOptions.class.getName());
        RocksDBOptions.instance();
        HugeConfig config = new HugeConfig(rocksdbConfig(budget,
                                                         RocksdbMemoryBudget.writeBufferSize(budget)));
        assertEquals(MIB, (long) config.get(RocksDBOptions.MEMTABLE_SIZE));
        try {
            new HugeConfig(rocksdbConfig(budget, budget / 1000));
            fail("accepted a write buffer under 1 MiB");
        } catch (ConfigException e) {
            assertTrue(e.getMessage().contains("write_buffer_size"));
        }
    }

    private static Map<String, Object> rocksdbConfig(long totalMemory, long writeBufferSize) {
        Map<String, Object> config = new HashMap<>();
        config.put("rocksdb.total_memory_size", String.valueOf(totalMemory));
        config.put("rocksdb.write_buffer_size", String.valueOf(writeBufferSize));
        return config;
    }

    @Test
    public void testUnknownPhysicalMemoryFallsBackToHeap() {
        assertEquals(2 * GIB, RocksdbMemoryBudget.auto(-1L, 2 * GIB, 2 * GIB));
        assertEquals(MIN_AUTO, RocksdbMemoryBudget.auto(0L, 64 * MIB, 64 * MIB));
    }
}
