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

package com.baidu.hugegraph.unit.util;

import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.UnitUtil;

public class UnitUtilTest {

    @Test
    public void testBytesToMB() {
        double value = UnitUtil.bytesToMB(0L);
        Assert.assertEquals(0d, value, 0d);

        // KB
        value = UnitUtil.bytesToMB(Bytes.KB * 1);
        Assert.assertEquals(0d, value, 0d);

        value = UnitUtil.bytesToMB(Bytes.KB * 10);
        Assert.assertEquals(0.01d, value, 0d);

        value = UnitUtil.bytesToMB(Bytes.KB * 100);
        Assert.assertEquals(0.1d, value, 0d);

        value = UnitUtil.bytesToMB(Bytes.KB * (long) (134 * 1.024));
        Assert.assertEquals(0.13d, value, 0d);

        value = UnitUtil.bytesToMB(Bytes.KB * (long) (135 * 1.024));
        Assert.assertEquals(0.13d, value, 0d);

        value = UnitUtil.bytesToMB(Bytes.KB * (long) (136 * 1.024));
        Assert.assertEquals(0.14d, value, 0d);

        value = UnitUtil.bytesToMB(Bytes.KB * (long) (144 * 1.024));
        Assert.assertEquals(0.14d, value, 0d);

        value = UnitUtil.bytesToMB(Bytes.KB * (long) (754 * 1.024));
        Assert.assertEquals(0.75d, value, 0d);

        value = UnitUtil.bytesToMB(Bytes.KB * (long) (755 * 1.024));
        Assert.assertEquals(0.75d, value, 0d);

        value = UnitUtil.bytesToMB(Bytes.KB * (long) (756 * 1.024));
        Assert.assertEquals(0.76d, value, 0d);

        // MB
        value = UnitUtil.bytesToMB(Bytes.MB * 1);
        Assert.assertEquals(1d, value, 0d);

        value = UnitUtil.bytesToMB(Bytes.MB * 13);
        Assert.assertEquals(13d, value, 0d);

        value = UnitUtil.bytesToMB(Bytes.MB * 1357);
        Assert.assertEquals(1357d, value, 0d);

        value = UnitUtil.bytesToMB(Bytes.MB * 1357924680);
        Assert.assertEquals(1357924680d, value, 0d);

        value = UnitUtil.bytesToMB(Bytes.MB * 1357924680246L);
        Assert.assertEquals(1357924680246d, value, 0d);
    }

    @Test
    public void testBytesToGB() {
        double value = UnitUtil.bytesToGB(0L);
        Assert.assertEquals(0d, value, 0d);

        // MB
        value = UnitUtil.bytesToGB(Bytes.MB * 1);
        Assert.assertEquals(0d, value, 0d);

        value = UnitUtil.bytesToGB(Bytes.MB * 10);
        Assert.assertEquals(0.01d, value, 0d);

        value = UnitUtil.bytesToGB(Bytes.MB * 100);
        Assert.assertEquals(0.10d, value, 0d);

        value = UnitUtil.bytesToGB(Bytes.MB * (long) (134 * 1.024));
        Assert.assertEquals(0.13d, value, 0d);

        value = UnitUtil.bytesToGB(Bytes.MB * (long) (135 * 1.024));
        Assert.assertEquals(0.13d, value, 0d);

        value = UnitUtil.bytesToGB(Bytes.MB * (long) (136 * 1.024));
        Assert.assertEquals(0.14d, value, 0d);

        value = UnitUtil.bytesToGB(Bytes.MB * (long) (144 * 1.024));
        Assert.assertEquals(0.14d, value, 0d);

        value = UnitUtil.bytesToGB(Bytes.MB * (long) (754 * 1.024));
        Assert.assertEquals(0.75d, value, 0d);

        value = UnitUtil.bytesToGB(Bytes.MB * (long) (755 * 1.024));
        Assert.assertEquals(0.75d, value, 0d);

        value = UnitUtil.bytesToGB(Bytes.MB * (long) (756 * 1.024));
        Assert.assertEquals(0.76d, value, 0d);

        // GB
        value = UnitUtil.bytesToGB(Bytes.GB * 1);
        Assert.assertEquals(1d, value, 0d);

        value = UnitUtil.bytesToGB(Bytes.GB * 13);
        Assert.assertEquals(13d, value, 0d);

        value = UnitUtil.bytesToGB(Bytes.GB * 1357);
        Assert.assertEquals(1357d, value, 0d);

        value = UnitUtil.bytesToGB(Bytes.GB * 1357924680);
        Assert.assertEquals(1357924680d, value, 0d);

        value = UnitUtil.bytesToGB(Bytes.GB * 7357924680L);
        Assert.assertEquals(7357924680d, value, 0d);

        // Bytes overflow long value
        value = UnitUtil.bytesToGB(Bytes.GB * 9357924680L);
        Assert.assertEquals(-7.821944504E9, value, 0d);
    }

    @Test
    public void testBytesToReadableString() {
        String value = UnitUtil.bytesToReadableString(0L);
        Assert.assertEquals("0 B", value);

        // B
        value = UnitUtil.bytesToReadableString(1);
        Assert.assertEquals("1 B", value);

        value = UnitUtil.bytesToReadableString(3);
        Assert.assertEquals("3 B", value);

        value = UnitUtil.bytesToReadableString(10);
        Assert.assertEquals("10 B", value);

        value = UnitUtil.bytesToReadableString(100);
        Assert.assertEquals("100 B", value);

        value = UnitUtil.bytesToReadableString(1000);
        Assert.assertEquals("1000 B", value);

        value = UnitUtil.bytesToReadableString(1023);
        Assert.assertEquals("1023 B", value);

        // KB
        value = UnitUtil.bytesToReadableString(1024);
        Assert.assertEquals("1 KB", value);

        value = UnitUtil.bytesToReadableString(Bytes.KB + 1);
        Assert.assertEquals("1.0 KB", value);

        value = UnitUtil.bytesToReadableString(Bytes.KB + 10);
        Assert.assertEquals("1.01 KB", value);

        value = UnitUtil.bytesToReadableString(Bytes.KB + 20);
        Assert.assertEquals("1.02 KB", value);

        value = UnitUtil.bytesToReadableString(Bytes.KB + 100);
        Assert.assertEquals("1.1 KB", value);

        value = UnitUtil.bytesToReadableString(Bytes.KB + 123);
        Assert.assertEquals("1.12 KB", value);

        value = UnitUtil.bytesToReadableString(Bytes.KB + 129);
        Assert.assertEquals("1.13 KB", value);

        value = UnitUtil.bytesToReadableString(Bytes.KB * 8 +
                                               (long) (755 * 1.024));
        Assert.assertEquals("8.75 KB", value);

        value = UnitUtil.bytesToReadableString(Bytes.KB * 168 +
                                               (long) (756 * 1.024));
        Assert.assertEquals("168.76 KB", value);

        // MB
        value = UnitUtil.bytesToReadableString(Bytes.KB * 1024);
        Assert.assertEquals("1 MB", value);

        value = UnitUtil.bytesToReadableString(Bytes.MB + 1 * Bytes.KB);
        Assert.assertEquals("1.0 MB", value);

        value = UnitUtil.bytesToReadableString(Bytes.MB + 10 * Bytes.KB);
        Assert.assertEquals("1.01 MB", value);

        value = UnitUtil.bytesToReadableString(Bytes.MB + 20 * Bytes.KB);
        Assert.assertEquals("1.02 MB", value);

        value = UnitUtil.bytesToReadableString(Bytes.MB + 100 * Bytes.KB);
        Assert.assertEquals("1.1 MB", value);

        value = UnitUtil.bytesToReadableString(Bytes.MB + 123 * Bytes.KB);
        Assert.assertEquals("1.12 MB", value);

        value = UnitUtil.bytesToReadableString(Bytes.MB + 129 * Bytes.KB);
        Assert.assertEquals("1.13 MB", value);

        value = UnitUtil.bytesToReadableString(Bytes.MB * 8 +
                                               (long) (755 * 1.024) * Bytes.KB);
        Assert.assertEquals("8.75 MB", value);

        value = UnitUtil.bytesToReadableString(Bytes.MB * 168 +
                                               (long) (756 * 1.024) * Bytes.KB);
        Assert.assertEquals("168.76 MB", value);

        // GB
        value = UnitUtil.bytesToReadableString(Bytes.MB * 1024);
        Assert.assertEquals("1 GB", value);

        value = UnitUtil.bytesToReadableString(Bytes.GB + 1 * Bytes.MB);
        Assert.assertEquals("1.0 GB", value);

        value = UnitUtil.bytesToReadableString(Bytes.GB + 10 * Bytes.MB);
        Assert.assertEquals("1.01 GB", value);

        value = UnitUtil.bytesToReadableString(Bytes.GB + 20 * Bytes.MB);
        Assert.assertEquals("1.02 GB", value);

        value = UnitUtil.bytesToReadableString(Bytes.GB + 100 * Bytes.MB);
        Assert.assertEquals("1.1 GB", value);

        value = UnitUtil.bytesToReadableString(Bytes.GB + 123 * Bytes.MB);
        Assert.assertEquals("1.12 GB", value);

        value = UnitUtil.bytesToReadableString(Bytes.GB + 129 * Bytes.MB);
        Assert.assertEquals("1.13 GB", value);

        value = UnitUtil.bytesToReadableString(Bytes.GB * 8 +
                                               (long) (755 * 1.024) * Bytes.MB);
        Assert.assertEquals("8.75 GB", value);

        value = UnitUtil.bytesToReadableString(Bytes.GB * 168 +
                                               (long) (756 * 1.024) * Bytes.MB);
        Assert.assertEquals("168.76 GB", value);

        // TB
        value = UnitUtil.bytesToReadableString(Bytes.GB * 1024);
        Assert.assertEquals("1 TB", value);

        value = UnitUtil.bytesToReadableString(Bytes.TB + 1 * Bytes.GB);
        Assert.assertEquals("1.0 TB", value);

        value = UnitUtil.bytesToReadableString(Bytes.TB + 10 * Bytes.GB);
        Assert.assertEquals("1.01 TB", value);

        value = UnitUtil.bytesToReadableString(Bytes.TB + 20 * Bytes.GB);
        Assert.assertEquals("1.02 TB", value);

        value = UnitUtil.bytesToReadableString(Bytes.TB + 100 * Bytes.GB);
        Assert.assertEquals("1.1 TB", value);

        value = UnitUtil.bytesToReadableString(Bytes.TB + 123 * Bytes.GB);
        Assert.assertEquals("1.12 TB", value);

        value = UnitUtil.bytesToReadableString(Bytes.TB + 129 * Bytes.GB);
        Assert.assertEquals("1.13 TB", value);

        value = UnitUtil.bytesToReadableString(Bytes.TB * 8 +
                                               (long) (755 * 1.024) * Bytes.GB);
        Assert.assertEquals("8.75 TB", value);

        value = UnitUtil.bytesToReadableString(Bytes.TB * 168 +
                                               (long) (756 * 1.024) * Bytes.GB);
        Assert.assertEquals("168.76 TB", value);

        // PB
        value = UnitUtil.bytesToReadableString(Bytes.TB * 1024);
        Assert.assertEquals("1 PB", value);

        value = UnitUtil.bytesToReadableString(Bytes.PB + 1 * Bytes.TB);
        Assert.assertEquals("1.0 PB", value);

        value = UnitUtil.bytesToReadableString(Bytes.PB + 10 * Bytes.TB);
        Assert.assertEquals("1.01 PB", value);

        value = UnitUtil.bytesToReadableString(Bytes.PB + 20 * Bytes.TB);
        Assert.assertEquals("1.02 PB", value);

        value = UnitUtil.bytesToReadableString(Bytes.PB + 100 * Bytes.TB);
        Assert.assertEquals("1.1 PB", value);

        value = UnitUtil.bytesToReadableString(Bytes.PB + 123 * Bytes.TB);
        Assert.assertEquals("1.12 PB", value);

        value = UnitUtil.bytesToReadableString(Bytes.PB + 129 * Bytes.TB);
        Assert.assertEquals("1.13 PB", value);

        value = UnitUtil.bytesToReadableString(Bytes.PB * 8 +
                                               (long) (755 * 1.024) * Bytes.TB);
        Assert.assertEquals("8.75 PB", value);

        value = UnitUtil.bytesToReadableString(Bytes.PB * 168 +
                                               (long) (756 * 1.024) * Bytes.TB);
        Assert.assertEquals("168.76 PB", value);

        // EB
        value = UnitUtil.bytesToReadableString(Bytes.PB * 1024);
        Assert.assertEquals("1 EB", value);

        value = UnitUtil.bytesToReadableString(Bytes.EB + 1 * Bytes.PB);
        Assert.assertEquals("1.0 EB", value);

        value = UnitUtil.bytesToReadableString(Bytes.EB + 10 * Bytes.PB);
        Assert.assertEquals("1.01 EB", value);

        value = UnitUtil.bytesToReadableString(Bytes.EB + 20 * Bytes.PB);
        Assert.assertEquals("1.02 EB", value);

        value = UnitUtil.bytesToReadableString(Bytes.EB + 100 * Bytes.PB);
        Assert.assertEquals("1.1 EB", value);

        value = UnitUtil.bytesToReadableString(Bytes.EB + 123 * Bytes.PB);
        Assert.assertEquals("1.12 EB", value);

        value = UnitUtil.bytesToReadableString(Bytes.EB + 129 * Bytes.PB);
        Assert.assertEquals("1.13 EB", value);

        value = UnitUtil.bytesToReadableString(Bytes.EB * 7 +
                                               (long) (755 * 1.024) * Bytes.PB);
        Assert.assertEquals("7.75 EB", value);

        // Bytes overflow long value
        value = UnitUtil.bytesToReadableString(Bytes.EB * 8);
        Assert.assertEquals("0 B", value);
    }

    @Test
    public void testBytesFromReadableString() {
        // B
        Assert.assertEquals(0L, UnitUtil.bytesFromReadableString("0 B"));
        Assert.assertEquals(1L, UnitUtil.bytesFromReadableString("1 Bytes"));
        Assert.assertEquals(3L, UnitUtil.bytesFromReadableString("3 bytes"));
        Assert.assertEquals(10L, UnitUtil.bytesFromReadableString("10 Byte"));
        Assert.assertEquals(100L, UnitUtil.bytesFromReadableString("100 byte"));
        Assert.assertEquals(1000L, UnitUtil.bytesFromReadableString("1000 b"));
        Assert.assertEquals(1023L, UnitUtil.bytesFromReadableString("1023 B"));

        Assert.assertEquals(1024L, UnitUtil.bytesFromReadableString("1024 B"));
        Assert.assertEquals(10245678L,
                            UnitUtil.bytesFromReadableString("10245678 B"));
        Assert.assertEquals(102456789012L,
                            UnitUtil.bytesFromReadableString("102456789012 B"));

        Assert.assertEquals(1L, UnitUtil.bytesFromReadableString("1  B"));
        Assert.assertEquals(1L, UnitUtil.bytesFromReadableString("1  B "));
        Assert.assertEquals(-2L, UnitUtil.bytesFromReadableString("-2 B"));

        // KB
        Assert.assertEquals(0L, UnitUtil.bytesFromReadableString("0 KB"));
        Assert.assertEquals(Bytes.KB * 1L,
                            UnitUtil.bytesFromReadableString("1 KB"));
        Assert.assertEquals((long) (Bytes.KB * 3.14),
                            UnitUtil.bytesFromReadableString("3.14 KB"));
        Assert.assertEquals(Bytes.KB * 10L,
                            UnitUtil.bytesFromReadableString("10 kB"));
        Assert.assertEquals(Bytes.KB * 100L,
                            UnitUtil.bytesFromReadableString("100 KiB"));
        Assert.assertEquals(Bytes.KB * 1000L,
                            UnitUtil.bytesFromReadableString("1000 kb"));
        Assert.assertEquals(Bytes.KB * 1023L,
                            UnitUtil.bytesFromReadableString("1023 kib"));
        Assert.assertEquals(Bytes.KB * 1234567890L,
                            UnitUtil.bytesFromReadableString("1234567890 Kib"));
        // MB
        Assert.assertEquals(0L, UnitUtil.bytesFromReadableString("0 MB"));
        Assert.assertEquals(Bytes.MB * 1L,
                            UnitUtil.bytesFromReadableString("1 MB"));
        Assert.assertEquals((long) (Bytes.MB * 3.14),
                            UnitUtil.bytesFromReadableString("3.14 MB"));
        Assert.assertEquals(Bytes.MB * 10L,
                            UnitUtil.bytesFromReadableString("10 mB"));
        Assert.assertEquals(Bytes.MB * 100L,
                            UnitUtil.bytesFromReadableString("100 MiB"));
        Assert.assertEquals(Bytes.MB * 1000L,
                            UnitUtil.bytesFromReadableString("1000 mib"));
        Assert.assertEquals(Bytes.MB * 1023L,
                            UnitUtil.bytesFromReadableString("1023 MIB"));
        Assert.assertEquals(Bytes.MB * 1234567890L,
                            UnitUtil.bytesFromReadableString("1234567890 Mb"));

        // GB
        Assert.assertEquals(0L, UnitUtil.bytesFromReadableString("0 GB"));
        Assert.assertEquals(Bytes.GB * 1L,
                            UnitUtil.bytesFromReadableString("1 GB"));
        Assert.assertEquals((long) (Bytes.GB * 3.14),
                            UnitUtil.bytesFromReadableString("3.14 GB"));
        Assert.assertEquals(Bytes.GB * 10L,
                            UnitUtil.bytesFromReadableString("10 gB"));
        Assert.assertEquals(Bytes.GB * 100L,
                            UnitUtil.bytesFromReadableString("100 GiB"));
        Assert.assertEquals(Bytes.GB * 1000L,
                            UnitUtil.bytesFromReadableString("1000 gib"));
        Assert.assertEquals(Bytes.GB * 1023L,
                            UnitUtil.bytesFromReadableString("1023 GIB"));
        Assert.assertEquals(Bytes.GB * 1234567890L,
                            UnitUtil.bytesFromReadableString("1234567890 Gb"));

        // TB
        Assert.assertEquals(0L, UnitUtil.bytesFromReadableString("0 TB"));
        Assert.assertEquals(Bytes.TB * 1L,
                            UnitUtil.bytesFromReadableString("1 TB"));
        Assert.assertEquals((long) (Bytes.TB * 3.14),
                            UnitUtil.bytesFromReadableString("3.14 TB"));
        Assert.assertEquals(Bytes.TB * 10L,
                            UnitUtil.bytesFromReadableString("10 tB"));
        Assert.assertEquals(Bytes.TB * 100L,
                            UnitUtil.bytesFromReadableString("100 TiB"));
        Assert.assertEquals(Bytes.TB * 1000L,
                            UnitUtil.bytesFromReadableString("1000 tib"));
        Assert.assertEquals(Bytes.TB * 1023L,
                            UnitUtil.bytesFromReadableString("1023 TIB"));
        Assert.assertEquals(Bytes.TB * 123456L,
                            UnitUtil.bytesFromReadableString("123456 Tb"));

        // PB
        Assert.assertEquals(0L, UnitUtil.bytesFromReadableString("0 PB"));
        Assert.assertEquals(Bytes.PB * 1L,
                            UnitUtil.bytesFromReadableString("1 PB"));
        Assert.assertEquals((long) (Bytes.PB * 3.14),
                            UnitUtil.bytesFromReadableString("3.14 PB"));
        Assert.assertEquals(Bytes.PB * 10L,
                            UnitUtil.bytesFromReadableString("10 pB"));
        Assert.assertEquals(Bytes.PB * 100L,
                            UnitUtil.bytesFromReadableString("100 PiB"));
        Assert.assertEquals(Bytes.PB * 1000L,
                            UnitUtil.bytesFromReadableString("1000 pib"));
        Assert.assertEquals(Bytes.PB * 1023L,
                            UnitUtil.bytesFromReadableString("1023 PIB"));
        Assert.assertEquals(Bytes.PB * 8024L,
                            UnitUtil.bytesFromReadableString("8024 PIB"));

        // EB
        Assert.assertEquals(0L, UnitUtil.bytesFromReadableString("0 EB"));
        Assert.assertEquals(Bytes.EB * 1L,
                            UnitUtil.bytesFromReadableString("1 EB"));
        Assert.assertEquals((long) (Bytes.EB * 3.14),
                            UnitUtil.bytesFromReadableString("3.14 EB"));
        Assert.assertEquals((long) (Bytes.EB * 5.01),
                            UnitUtil.bytesFromReadableString("5.01 eB"));
        Assert.assertEquals((long) (Bytes.EB * 6.28),
                            UnitUtil.bytesFromReadableString("6.28 EiB"));
        Assert.assertEquals((long) (Bytes.EB * 7.9876),
                            UnitUtil.bytesFromReadableString("7.9876 eib"));
        Assert.assertEquals((long) (Bytes.EB * 8.0),
                            UnitUtil.bytesFromReadableString("8.0 EIB")); // max
    }

    @Test
    public void testBytesFromReadableStringWithInvalidFormat() {
        // No space
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            UnitUtil.bytesFromReadableString("1kb");
        }, e -> {
            Assert.assertContains("Invalid readable bytes '1kb'",
                                  e.getMessage());
        });

        // Invalid unit
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            UnitUtil.bytesFromReadableString("1 aBc");
        }, e -> {
            Assert.assertContains("Unrecognized unit aBc", e.getMessage());
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            UnitUtil.bytesFromReadableString("1 k");
        }, e -> {
            Assert.assertContains("Unrecognized unit k", e.getMessage());
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            UnitUtil.bytesFromReadableString("1 m");
        }, e -> {
            Assert.assertContains("Unrecognized unit m", e.getMessage());
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            UnitUtil.bytesFromReadableString("1 2 MB");
        }, e -> {
            Assert.assertContains("Unrecognized unit 2 MB", e.getMessage());
        });

        // Invalid number
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            UnitUtil.bytesFromReadableString("2b kb");
        }, e -> {
            Assert.assertContains("Invalid parameter(not number): '2b kb'",
                                  e.getMessage());
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            UnitUtil.bytesFromReadableString("2.3.4 kb");
        }, e -> {
            Assert.assertContains("Invalid parameter(not number): '2.3.4 kb'",
                                  e.getMessage());
        });

        // Bytes overflow long value
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            UnitUtil.bytesFromReadableString("8.1 EIB");
        }, e -> {
            Assert.assertContains("The value 9.33866418731546E18 from " +
                                  "parameter '8.1 EIB' is out of range",
                                  e.getMessage());
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            UnitUtil.bytesFromReadableString("9024 Pb");
        }, e -> {
            Assert.assertContains("is out of range", e.getMessage());
        });
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            UnitUtil.bytesFromReadableString("12345678 Tb");
        }, e -> {
            Assert.assertContains("is out of range", e.getMessage());
        });
    }

    @Test
    public void testTimestampToReadableString() {
        Assert.assertEquals("0ms",
                            UnitUtil.timestampToReadableString(0L));
        Assert.assertEquals("1ms",
                            UnitUtil.timestampToReadableString(1L));
        Assert.assertEquals("100ms",
                            UnitUtil.timestampToReadableString(100L));
        Assert.assertEquals("999ms",
                            UnitUtil.timestampToReadableString(999L));

        Assert.assertEquals("1s",
                            UnitUtil.timestampToReadableString(1000L));
        Assert.assertEquals("10s",
                            UnitUtil.timestampToReadableString(10000L));
        Assert.assertEquals("1s1ms",
                            UnitUtil.timestampToReadableString(1001L));
        Assert.assertEquals("1s200ms",
                            UnitUtil.timestampToReadableString(1200L));
        Assert.assertEquals("6s789ms",
                            UnitUtil.timestampToReadableString(6789L));
        Assert.assertEquals("59s789ms",
                            UnitUtil.timestampToReadableString(59789L));

        Assert.assertEquals("1m0s",
                            UnitUtil.timestampToReadableString(60789L));
        Assert.assertEquals("1m1s",
                            UnitUtil.timestampToReadableString(61789L));
        Assert.assertEquals("1m2s",
                            UnitUtil.timestampToReadableString(62000L));
        Assert.assertEquals("2m2s",
                            UnitUtil.timestampToReadableString(122000L));
        Assert.assertEquals("2m12s",
                            UnitUtil.timestampToReadableString(132000L));
        Assert.assertEquals("59m59s",
                            UnitUtil.timestampToReadableString(3599000L));

        Assert.assertEquals("1h0m0s",
                            UnitUtil.timestampToReadableString(3600000L));
        Assert.assertEquals("1h0m23s",
                            UnitUtil.timestampToReadableString(3623000L));
        Assert.assertEquals("1h1m23s",
                            UnitUtil.timestampToReadableString(3683000L));
        Assert.assertEquals("1h24m43s",
                            UnitUtil.timestampToReadableString(5083000L));
        Assert.assertEquals("23h59m59s",
                            UnitUtil.timestampToReadableString(86399000L));

        Assert.assertEquals("1d0h0m0s",
                            UnitUtil.timestampToReadableString(86400000L));
        Assert.assertEquals("1d1h24m43s",
                            UnitUtil.timestampToReadableString(91483000L));
        Assert.assertEquals("1d1h24m43s",
                            UnitUtil.timestampToReadableString(91483000L));

        Assert.assertEquals("30d0h0m0s",
                            UnitUtil.timestampToReadableString(2592000000L));
        Assert.assertEquals("30d23h59m59s",
                            UnitUtil.timestampToReadableString(2678399000L));
        Assert.assertEquals("130d23h59m59s",
                            UnitUtil.timestampToReadableString(11318399000L));
        Assert.assertEquals("1130d23h59m59s",
                            UnitUtil.timestampToReadableString(97718399000L));
    }

    @Test
    public void testTimestampFromReadableString() {
        Assert.assertEquals(0L,
                            UnitUtil.timestampFromReadableString("0ms"));
        Assert.assertEquals(1L,
                            UnitUtil.timestampFromReadableString("1ms"));
        Assert.assertEquals(100L,
                            UnitUtil.timestampFromReadableString("100ms"));
        Assert.assertEquals(999L,
                            UnitUtil.timestampFromReadableString("999ms"));
        Assert.assertEquals(1001L,
                            UnitUtil.timestampFromReadableString("1001ms"));

        Assert.assertEquals(0L,
                            UnitUtil.timestampFromReadableString("0s"));
        Assert.assertEquals(1000L,
                            UnitUtil.timestampFromReadableString("1s"));
        Assert.assertEquals(10000L,
                            UnitUtil.timestampFromReadableString("10s"));
        Assert.assertEquals(1001L,
                            UnitUtil.timestampFromReadableString("1s1ms"));
        Assert.assertEquals(1200L,
                            UnitUtil.timestampFromReadableString("1s200ms"));
        Assert.assertEquals(6789L,
                            UnitUtil.timestampFromReadableString("6s789ms"));
        Assert.assertEquals(59789L,
                            UnitUtil.timestampFromReadableString("59s789ms"));
        Assert.assertEquals(59789L,
                            UnitUtil.timestampFromReadableString("59s2789ms"));

        Assert.assertEquals(60000L,
                            UnitUtil.timestampFromReadableString("1m0s"));
        Assert.assertEquals(61000L,
                            UnitUtil.timestampFromReadableString("1m1s"));
        Assert.assertEquals(62000L,
                            UnitUtil.timestampFromReadableString("1m2s"));
        Assert.assertEquals(122000L,
                            UnitUtil.timestampFromReadableString("2m2s"));
        Assert.assertEquals(132000L,
                            UnitUtil.timestampFromReadableString("2m12s"));
        Assert.assertEquals(3599000L,
                            UnitUtil.timestampFromReadableString("59m59s"));

        Assert.assertEquals(3600000L,
                            UnitUtil.timestampFromReadableString("1h0m0s"));
        Assert.assertEquals(3623000L,
                            UnitUtil.timestampFromReadableString("1h0m23s"));
        Assert.assertEquals(3683000L,
                            UnitUtil.timestampFromReadableString("1h1m23s"));
        Assert.assertEquals(5083000L,
                            UnitUtil.timestampFromReadableString("1h24m43s"));
        Assert.assertEquals(86399000L,
                            UnitUtil.timestampFromReadableString("23h59m59s"));

        Assert.assertEquals(86400000L,
                            UnitUtil.timestampFromReadableString("1d0h0m0s"));
        Assert.assertEquals(91483000L,
                            UnitUtil.timestampFromReadableString("1d1h24m43s"));
        Assert.assertEquals(91483000L,
                            UnitUtil.timestampFromReadableString("1d1h24m43s"));

        Assert.assertEquals(2592000000L, UnitUtil.timestampFromReadableString(
                                         "30d0h0m0s"));
        Assert.assertEquals(2678399000L, UnitUtil.timestampFromReadableString(
                                         "30d23h59m59s"));
        Assert.assertEquals(11318399000L, UnitUtil.timestampFromReadableString(
                                          "130d23h59m59s"));
        Assert.assertEquals(97718399000L, UnitUtil.timestampFromReadableString(
                                          "1130d23h59m59s"));
    }
}
