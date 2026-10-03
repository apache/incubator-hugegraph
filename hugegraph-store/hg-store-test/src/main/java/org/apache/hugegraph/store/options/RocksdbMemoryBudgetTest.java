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
    public void testUnknownPhysicalMemoryFallsBackToHeap() {
        assertEquals(2 * GIB, RocksdbMemoryBudget.auto(-1L, 2 * GIB, 2 * GIB));
        assertEquals(MIN_AUTO, RocksdbMemoryBudget.auto(0L, 64 * MIB, 64 * MIB));
    }
}
