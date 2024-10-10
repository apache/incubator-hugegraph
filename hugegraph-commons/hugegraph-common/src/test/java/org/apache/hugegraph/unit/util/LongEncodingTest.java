/*
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

package org.apache.hugegraph.unit.util;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.TimeZone;

import org.junit.Test;

import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.util.Bytes;
import org.apache.hugegraph.util.LongEncoding;
import org.apache.hugegraph.util.NumericUtil;
import org.apache.hugegraph.testutil.Assert;

public class LongEncodingTest extends BaseUnitTest {

    @Test
    public void testValidB64Char() {
        Assert.assertTrue(LongEncoding.validB64Char('0'));
        Assert.assertTrue(LongEncoding.validB64Char('1'));
        Assert.assertTrue(LongEncoding.validB64Char('9'));
        Assert.assertTrue(LongEncoding.validB64Char('A'));
        Assert.assertTrue(LongEncoding.validB64Char('Z'));
        Assert.assertTrue(LongEncoding.validB64Char('_'));
        Assert.assertTrue(LongEncoding.validB64Char('a'));
        Assert.assertTrue(LongEncoding.validB64Char('z'));
        Assert.assertTrue(LongEncoding.validB64Char('~'));

        Assert.assertFalse(LongEncoding.validB64Char('`'));
        Assert.assertFalse(LongEncoding.validB64Char('!'));
        Assert.assertFalse(LongEncoding.validB64Char('@'));
        Assert.assertFalse(LongEncoding.validB64Char('#'));
        Assert.assertFalse(LongEncoding.validB64Char('$'));
        Assert.assertFalse(LongEncoding.validB64Char('%'));
        Assert.assertFalse(LongEncoding.validB64Char('^'));
        Assert.assertFalse(LongEncoding.validB64Char('&'));
        Assert.assertFalse(LongEncoding.validB64Char('*'));
        Assert.assertFalse(LongEncoding.validB64Char('('));
        Assert.assertFalse(LongEncoding.validB64Char(')'));
        Assert.assertFalse(LongEncoding.validB64Char('-'));
        Assert.assertFalse(LongEncoding.validB64Char('+'));
        Assert.assertFalse(LongEncoding.validB64Char('='));
        Assert.assertFalse(LongEncoding.validB64Char('['));
        Assert.assertFalse(LongEncoding.validB64Char(']'));
        Assert.assertFalse(LongEncoding.validB64Char('{'));
        Assert.assertFalse(LongEncoding.validB64Char('}'));
        Assert.assertFalse(LongEncoding.validB64Char('|'));
        Assert.assertFalse(LongEncoding.validB64Char('\\'));
        Assert.assertFalse(LongEncoding.validB64Char(';'));
        Assert.assertFalse(LongEncoding.validB64Char(':'));
        Assert.assertFalse(LongEncoding.validB64Char('\''));
        Assert.assertFalse(LongEncoding.validB64Char('\"'));
        Assert.assertFalse(LongEncoding.validB64Char('<'));
        Assert.assertFalse(LongEncoding.validB64Char(','));
        Assert.assertFalse(LongEncoding.validB64Char('>'));
        Assert.assertFalse(LongEncoding.validB64Char('.'));
        Assert.assertFalse(LongEncoding.validB64Char('?'));
        Assert.assertFalse(LongEncoding.validB64Char('/'));
        Assert.assertFalse(LongEncoding.validB64Char('\t'));
        Assert.assertFalse(LongEncoding.validB64Char('\b'));
    }

    @Test
    public void testEncode() {
        String val0 = LongEncoding.encodeB64(0);
        Assert.assertEquals("0", val0);

        String val1 = LongEncoding.encodeB64(1);
        Assert.assertEquals("1", val1);

        String val9 = LongEncoding.encodeB64(9);
        Assert.assertEquals("9", val9);

        String val10 = LongEncoding.encodeB64(10);
        Assert.assertEquals("A", val10);

        String val35 = LongEncoding.encodeB64(35);
        Assert.assertEquals("Z", val35);

        String val36 = LongEncoding.encodeB64(36);
        Assert.assertEquals("_", val36);

        String val37 = LongEncoding.encodeB64(37);
        Assert.assertEquals("a", val37);

        String val62 = LongEncoding.encodeB64(62);
        Assert.assertEquals("z", val62);

        String val63 = LongEncoding.encodeB64(63);
        Assert.assertEquals("~", val63);
    }

    @Test
    public void testEncodeWithMultiString() {
        Assert.assertEquals("0", LongEncoding.encode(0L, "0123456789"));
        Assert.assertEquals("1", LongEncoding.encode(1L, "0123456789"));
        Assert.assertEquals("123", LongEncoding.encode(123L, "0123456789"));
        Assert.assertEquals("13579", LongEncoding.encode(13579L, "0123456789"));
        Assert.assertEquals("24680", LongEncoding.encode(24680L, "0123456789"));

        String val64 = LongEncoding.encodeB64(64);
        Assert.assertEquals("10", val64);

        String val65 = LongEncoding.encodeB64(65);
        Assert.assertEquals("11", val65);

        String val99 = LongEncoding.encodeB64(99);
        Assert.assertEquals("1Z", val99);

        String val100 = LongEncoding.encodeB64(100);
        Assert.assertEquals("1_", val100);

        String val126 = LongEncoding.encodeB64(126);
        Assert.assertEquals("1z", val126);

        String val127 = LongEncoding.encodeB64(127);
        Assert.assertEquals("1~", val127);

        String val128 = LongEncoding.encodeB64(128);
        Assert.assertEquals("20", val128);

        String val200 = LongEncoding.encodeB64(200);
        Assert.assertEquals("38", val200);

        String val1000 = LongEncoding.encodeB64(1000);
        Assert.assertEquals("Fd", val1000);

        String val1234 = LongEncoding.encodeB64(1234);
        Assert.assertEquals("JI", val1234);

        String val10000 = LongEncoding.encodeB64(10000);
        Assert.assertEquals("2SG", val10000);

        String val12345 = LongEncoding.encodeB64(12345);
        Assert.assertEquals("30u", val12345);

        String val22345 = LongEncoding.encodeB64(22345);
        Assert.assertEquals("5T9", val22345);

        String val92345 = LongEncoding.encodeB64(92345);
        Assert.assertEquals("MYu", val92345);

        String val12345678 = LongEncoding.encodeB64(12345678);
        Assert.assertEquals("k65E", val12345678);

        String val112345678 = LongEncoding.encodeB64(112345678);
        Assert.assertEquals("6h_9E", val112345678);

        String valIntMax = LongEncoding.encodeB64(Integer.MAX_VALUE);
        Assert.assertEquals("1~~~~~", valIntMax);

        String valLongMax = LongEncoding.encodeB64(Long.MAX_VALUE);
        Assert.assertEquals("7~~~~~~~~~~", valLongMax);
    }

    @Test
    public void testEncodeWithError() {
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            LongEncoding.encode(1, "");
        }, e -> {
            Assert.assertEquals("The symbols parameter can't be empty",
                                e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            LongEncoding.encode(-1, "");
        }, e -> {
            Assert.assertContains("Expected non-negative number",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            LongEncoding.encodeB64(-1);
        }, e -> {
            Assert.assertContains("Expected non-negative number",
                                  e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            LongEncoding.encodeB64(Long.MIN_VALUE);
        }, e -> {
            Assert.assertContains("Expected non-negative number",
                                  e.getMessage());
        });
    }

    @Test
    public void testDecode() {
        long valEmpty = LongEncoding.decodeB64("");
        Assert.assertEquals(0, valEmpty);

        long val0 = LongEncoding.decodeB64("0");
        Assert.assertEquals(0, val0);

        long val1 = LongEncoding.decodeB64("1");
        Assert.assertEquals(1, val1);

        long val9 = LongEncoding.decodeB64("9");
        Assert.assertEquals(9, val9);

        long val10 = LongEncoding.decodeB64("A");
        Assert.assertEquals(10, val10);

        long val35 = LongEncoding.decodeB64("Z");
        Assert.assertEquals(35, val35);

        long val36 = LongEncoding.decodeB64("_");
        Assert.assertEquals(36, val36);

        long val37 = LongEncoding.decodeB64("a");
        Assert.assertEquals(37, val37);

        long val62 = LongEncoding.decodeB64("z");
        Assert.assertEquals(62, val62);

        long val63 = LongEncoding.decodeB64("~");
        Assert.assertEquals(63, val63);
    }

    @Test
    public void testDecodeWithMultiString() {
        Assert.assertEquals(0L, LongEncoding.decode("0", "0123456789"));
        Assert.assertEquals(1L, LongEncoding.decode("1", "0123456789"));
        Assert.assertEquals(123L, LongEncoding.decode("123", "0123456789"));
        Assert.assertEquals(13579L, LongEncoding.decode("13579", "0123456789"));
        Assert.assertEquals(24680L, LongEncoding.decode("24680", "0123456789"));

        long val64 = LongEncoding.decodeB64("10");
        Assert.assertEquals(64, val64);

        long val65 = LongEncoding.decodeB64("11");
        Assert.assertEquals(65, val65);

        long val99 = LongEncoding.decodeB64("1Z");
        Assert.assertEquals(99, val99);

        long val100 = LongEncoding.decodeB64("1_");
        Assert.assertEquals(100, val100);

        long val126 = LongEncoding.decodeB64("1z");
        Assert.assertEquals(126, val126);

        long val127 = LongEncoding.decodeB64("1~");
        Assert.assertEquals(127, val127);

        long val128 = LongEncoding.decodeB64("20");
        Assert.assertEquals(128, val128);

        long val200 = LongEncoding.decodeB64("38");
        Assert.assertEquals(200, val200);

        long val1000 = LongEncoding.decodeB64("Fd");
        Assert.assertEquals(1000, val1000);

        long val1234 = LongEncoding.decodeB64("JI");
        Assert.assertEquals(1234, val1234);

        long val10000 = LongEncoding.decodeB64("2SG");
        Assert.assertEquals(10000, val10000);

        long val12345 = LongEncoding.decodeB64("30u");
        Assert.assertEquals(12345, val12345);

        long val22345 = LongEncoding.decodeB64("5T9");
        Assert.assertEquals(22345, val22345);

        long val92345 = LongEncoding.decodeB64("MYu");
        Assert.assertEquals(92345, val92345);

        long val12345678 = LongEncoding.decodeB64("k65E");
        Assert.assertEquals(12345678, val12345678);

        long val112345678 = LongEncoding.decodeB64("6h_9E");
        Assert.assertEquals(112345678, val112345678);

        long valIntMax = LongEncoding.decodeB64("1~~~~~");
        Assert.assertEquals(Integer.MAX_VALUE, valIntMax);

        long valLongMax = LongEncoding.decodeB64("7~~~~~~~~~~");
        Assert.assertEquals(Long.MAX_VALUE, valLongMax);
    }

    @Test
    public void testDecodeWithError() {
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            LongEncoding.decode("1", "");
        }, e -> {
            Assert.assertEquals("The symbols parameter can't be empty",
                                e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            LongEncoding.decode("1a", "0123456789");
        }, e -> {
            Assert.assertEquals("Can't decode symbol 'a' in string '1a'",
                                e.getMessage());
        });
    }

    @Test
    public void testEncodeSignedB64() {
        String val1234 = LongEncoding.encodeSignedB64(1234);
        Assert.assertEquals("JI", val1234);

        String val23 = LongEncoding.encodeSignedB64(23);
        Assert.assertEquals("N", val23);

        String valIntMax = LongEncoding.encodeSignedB64(Integer.MAX_VALUE);
        Assert.assertEquals("1~~~~~", valIntMax);

        String valLongMax = LongEncoding.encodeSignedB64(Long.MAX_VALUE);
        Assert.assertEquals("7~~~~~~~~~~", valLongMax);

        String val0 = LongEncoding.encodeSignedB64(0);
        Assert.assertEquals("0", val0);

        String valNeg1 = LongEncoding.encodeSignedB64(-1);
        Assert.assertEquals("-1", valNeg1);

        String valIntMinP1 = LongEncoding.encodeSignedB64(Integer.MIN_VALUE +
                                                          1L);
        Assert.assertEquals("-1~~~~~", valIntMinP1);

        String valIntMin = LongEncoding.encodeSignedB64(Integer.MIN_VALUE);
        Assert.assertEquals("-200000", valIntMin);

        String valLongMinPlus1 = LongEncoding.encodeSignedB64(Long.MIN_VALUE +
                                                              1L);
        Assert.assertEquals("-7~~~~~~~~~~", valLongMinPlus1);

        String valLongMin = LongEncoding.encodeSignedB64(Long.MIN_VALUE);
        Assert.assertEquals("-80000000000", valLongMin);
    }

    @Test
    public void testDecodeSignedB64() {
        long val1234 = LongEncoding.decodeSignedB64("JI");
        Assert.assertEquals(1234, val1234);

        long val23 = LongEncoding.decodeSignedB64("N");
        Assert.assertEquals(23, val23);

        long valIntMax = LongEncoding.decodeSignedB64("1~~~~~");
        Assert.assertEquals(Integer.MAX_VALUE, valIntMax);

        long valLongMax = LongEncoding.decodeSignedB64("7~~~~~~~~~~");
        Assert.assertEquals(Long.MAX_VALUE, valLongMax);

        long val0 = LongEncoding.decodeSignedB64("0");
        Assert.assertEquals(0, val0);

        long valn1 = LongEncoding.decodeSignedB64("-1");
        Assert.assertEquals(-1, valn1);

        long valIntMinPlus1 = LongEncoding.decodeSignedB64("-1~~~~~");
        Assert.assertEquals(Integer.MIN_VALUE + 1L, valIntMinPlus1);

        long valIntMin = LongEncoding.decodeSignedB64("-200000");
        Assert.assertEquals(Integer.MIN_VALUE, valIntMin);

        long valLongMinPlus1 = LongEncoding.decodeSignedB64("-7~~~~~~~~~~");
        Assert.assertEquals(Long.MIN_VALUE + 1L, valLongMinPlus1);

        long valLongMin = LongEncoding.decodeSignedB64("-80000000000");
        Assert.assertEquals(Long.MIN_VALUE, valLongMin);
    }

    @Test
    public void testDecodeSignedB64Overflow() {
        long valOverflow = LongEncoding.decodeSignedB64("80000000000");
        Assert.assertEquals(Long.MIN_VALUE, valOverflow);

        long valOverflow2 = LongEncoding.decodeSignedB64("80000000001");
        Assert.assertEquals(Long.MIN_VALUE + 1L, valOverflow2);

        long valOverflow3 = LongEncoding.decodeSignedB64("800000000001");
        Assert.assertEquals(1L, valOverflow3);

        long valOverflow4 = LongEncoding.decodeSignedB64("80000000000JI");
        Assert.assertEquals(1234L, valOverflow4);

        long valOverflow5 = LongEncoding.decodeSignedB64("80000000000N");
        Assert.assertEquals(23L, valOverflow5);

        long valOverflow6 = LongEncoding.decodeSignedB64("80000000000" +
                                                         "7~~~~~~~~~~");
        Assert.assertEquals(Long.MAX_VALUE, valOverflow6);
    }

    @Test
    public void testEncodeSortable() {
        String val1234 = LongEncoding.encodeSortable(1234);
        Assert.assertEquals("2JI", val1234);

        String val23 = LongEncoding.encodeSortable(23);
        Assert.assertEquals("1N", val23);

        String valIntMax = LongEncoding.encodeSortable(Integer.MAX_VALUE);
        Assert.assertEquals("61~~~~~", valIntMax);

        String valLongMax = LongEncoding.encodeSortable(Long.MAX_VALUE);
        Assert.assertEquals("B7~~~~~~~~~~", valLongMax);

        String val0 = LongEncoding.encodeSortable(0);
        Assert.assertEquals("10", val0);

        String valNeg1 = LongEncoding.encodeSortable(-1);
        Assert.assertEquals("0B7~~~~~~~~~~", valNeg1);

        String valIntMin = LongEncoding.encodeSortable(Integer.MIN_VALUE);
        Assert.assertEquals("0B7~~~~z00000", valIntMin);

        String valLongMin = LongEncoding.encodeSortable(Long.MIN_VALUE);
        Assert.assertEquals("010", valLongMin);
    }

    @Test
    public void testDecodeSortable() {
        long val1234 = LongEncoding.decodeSortable("2JI");
        Assert.assertEquals(1234, val1234);

        long val23 = LongEncoding.decodeSortable("1N");
        Assert.assertEquals(23, val23);

        long valIntMax = LongEncoding.decodeSortable("61~~~~~");
        Assert.assertEquals(Integer.MAX_VALUE, valIntMax);

        long valLongMax = LongEncoding.decodeSortable("B7~~~~~~~~~~");
        Assert.assertEquals(Long.MAX_VALUE, valLongMax);

        long val0 = LongEncoding.decodeSortable("10");
        Assert.assertEquals(0, val0);

        long valn1 = LongEncoding.decodeSortable("0B7~~~~~~~~~~");
        Assert.assertEquals(-1, valn1);

        long valIntMin = LongEncoding.decodeSortable("0B7~~~~z00000");
        Assert.assertEquals(Integer.MIN_VALUE, valIntMin);

        long valLongMin = LongEncoding.decodeSortable("010");
        Assert.assertEquals(Long.MIN_VALUE, valLongMin);
    }

    @Test
    public void testDecodeIllegalSortable() {
        // Length is 1, actual length is 0
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            LongEncoding.decodeSortable("1");
        });

        // Length is 1, actual length is 2
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            LongEncoding.decodeSortable("123");
        });

        // Length is 1, actual length is 0
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            LongEncoding.decodeSortable("01");
        });

        // Length is 1, actual length is 2
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            LongEncoding.decodeSortable("0123");
        });
    }

    @Test
    public void testEncodeNumber() throws ParseException {
        String l1234 = LongEncoding.encodeNumber(1234);
        Assert.assertEquals("2JI", l1234);

        String d1234 = LongEncoding.encodeNumber(1.234);
        Assert.assertEquals("B3~okcR8i3aO", d1234);

        String d21 = LongEncoding.encodeNumber(2.1);
        Assert.assertEquals("B400oCoCoCoD", d21);

        String dpi = LongEncoding.encodeNumber(3.1415926);
        Assert.assertEquals("B4098ViD4iXA", dpi);

        String fpi = LongEncoding.encodeNumber(3.1415926f);
        Assert.assertEquals("610IG~Q", fpi);

        String fn1 = LongEncoding.encodeNumber(-1.0f);
        Assert.assertEquals("0B7~~~~~0V~~~", fn1);

        String bMax = LongEncoding.encodeNumber(Byte.MAX_VALUE);
        Assert.assertEquals("21~", bMax);

        String bMin = LongEncoding.encodeNumber(Byte.MIN_VALUE);
        Assert.assertEquals("0B7~~~~~~~~z0", bMin);

        String sMax = LongEncoding.encodeNumber(Short.MAX_VALUE);
        Assert.assertEquals("37~~", sMax);

        String sMin = LongEncoding.encodeNumber(Short.MIN_VALUE);
        Assert.assertEquals("0B7~~~~~~~t00", sMin);

        String iMax = LongEncoding.encodeNumber(Integer.MAX_VALUE);
        Assert.assertEquals("61~~~~~", iMax);

        String iMin = LongEncoding.encodeNumber(Integer.MIN_VALUE);
        Assert.assertEquals("0B7~~~~z00000", iMin);

        String lMax = LongEncoding.encodeNumber(Long.MAX_VALUE);
        Assert.assertEquals("B7~~~~~~~~~~", lMax);

        String lMin = LongEncoding.encodeNumber(Long.MIN_VALUE);
        Assert.assertEquals("010", lMin);

        String fMax = LongEncoding.encodeNumber(Float.MAX_VALUE);
        Assert.assertEquals("61~V~~~", fMax);

        String fMin = LongEncoding.encodeNumber(Float.MIN_VALUE);
        Assert.assertEquals("11", fMin);

        String dMax = LongEncoding.encodeNumber(Double.MAX_VALUE);
        Assert.assertEquals("B7~k~~~~~~~~", dMax);

        String dMin = LongEncoding.encodeNumber(Double.MIN_VALUE);
        Assert.assertEquals("11", dMin);

        String bdLong = LongEncoding.encodeNumber(new BigDecimal("1234"));
        Assert.assertEquals("2JI", bdLong);

        String bdFLong = LongEncoding.encodeNumber(new BigDecimal("1234.00"));
        Assert.assertEquals("2JI", bdFLong);

        String bdDouble = LongEncoding.encodeNumber(new BigDecimal("1.234"));
        Assert.assertEquals("B3~okcR8i3aO", bdDouble);

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        Date time = dateFormat.parse("2018-12-18");
        String date = LongEncoding.encodeNumber(time);
        Assert.assertEquals("7MUxuK00", date);
    }

    @Test
    public void testDecodeNumber() {
        Number l1234 = LongEncoding.decodeNumber("2JI", Long.class);
        Assert.assertEquals(1234L, l1234);

        Number d1234 = LongEncoding.decodeNumber("B3~okcR8i3aO", Double.class);
        Assert.assertEquals(1.234, d1234);

        Number d21 = LongEncoding.decodeNumber("B400oCoCoCoD", Double.class);
        Assert.assertEquals(2.1, d21);

        Number dpi = LongEncoding.decodeNumber("B4098ViD4iXA", Double.class);
        Assert.assertEquals(3.1415926, dpi);

        Number fpi = LongEncoding.decodeNumber("610IG~Q", Float.class);
        Assert.assertEquals(3.1415926f, fpi);

        Number fn1 = LongEncoding.decodeNumber("0B7~~~~~0V~~~", Float.class);
        Assert.assertEquals(-1.0f, fn1);

        Number bMax = LongEncoding.decodeNumber("21~", Byte.class);
        Assert.assertEquals(Byte.MAX_VALUE, bMax);

        Number bMin = LongEncoding.decodeNumber("0B7~~~~~~~~z0", Byte.class);
        Assert.assertEquals(Byte.MIN_VALUE, bMin);

        Number sMax = LongEncoding.decodeNumber("37~~", Short.class);
        Assert.assertEquals(Short.MAX_VALUE, sMax);

        Number sMin = LongEncoding.decodeNumber("0B7~~~~~~~t00", Short.class);
        Assert.assertEquals(Short.MIN_VALUE, sMin);

        Number iMax = LongEncoding.decodeNumber("61~~~~~", Integer.class);
        Assert.assertEquals(Integer.MAX_VALUE, iMax);

        Number iMin = LongEncoding.decodeNumber("0B7~~~~z00000", Integer.class);
        Assert.assertEquals(Integer.MIN_VALUE, iMin);

        Number lMax = LongEncoding.decodeNumber("B7~~~~~~~~~~", Long.class);
        Assert.assertEquals(Long.MAX_VALUE, lMax);

        Number lMin = LongEncoding.decodeNumber("010", Long.class);
        Assert.assertEquals(Long.MIN_VALUE, lMin);

        Number fMax = LongEncoding.decodeNumber("61~V~~~", Float.class);
        Assert.assertEquals(Float.MAX_VALUE, fMax);

        Number fMin = LongEncoding.decodeNumber("11", Float.class);
        Assert.assertEquals(Float.MIN_VALUE, fMin);

        Number dMax = LongEncoding.decodeNumber("B7~k~~~~~~~~", Double.class);
        Assert.assertEquals(Double.MAX_VALUE, dMax);

        Number dMin = LongEncoding.decodeNumber("11", Double.class);
        Assert.assertEquals(Double.MIN_VALUE, dMin);
    }

    @Test
    public void testEncodeSortableThenCompare() {
        int count = 100000;
        Random random1 = new Random();
        Random random2 = new Random();
        for (int i = 0; i < count; i++) {
            long num1 = random1.nextLong();
            long num2 = random2.nextLong();

            String encoded1 = LongEncoding.encodeSortable(num1);
            String encoded2 = LongEncoding.encodeSortable(num2);
            int cmp = Bytes.compare(encoded1.getBytes(), encoded2.getBytes());

            if (num1 == num2) {
                Assert.assertEquals(0, cmp);
            } else if (num1 > num2) {
                Assert.assertTrue(cmp > 0);
            } else {
                assert num1 < num2;
                Assert.assertTrue(cmp < 0);
            }
        }
    }

    @Test
    public void testEncodeNumberThenCompare() {
        int count = 100000;
        Random random1 = new Random();
        Random random2 = new Random();

        for (int i = 0; i < count; i++) {
            compareEncodedNumber(random1.nextInt(), random2.nextInt());
        }
        for (int i = 0; i < count; i++) {
            compareEncodedNumber(random1.nextLong(), random2.nextLong());
        }

        for (int i = 0; i < count; i++) {
            compareEncodedNumber(random1.nextInt(), random2.nextLong());
        }
        for (int i = 0; i < count; i++) {
            compareEncodedNumber(random1.nextLong(), random2.nextInt());
        }

        for (int i = 0; i < count; i++) {
            compareEncodedNumber(random1.nextFloat(), random2.nextFloat());
        }
        for (int i = 0; i < count; i++) {
            compareEncodedNumber(random1.nextDouble(), random2.nextDouble());
        }
    }

    private static void compareEncodedNumber(Number num1, Number num2) {
        int cmpExpected = NumericUtil.compareNumber(num1, num2);

        String encoded1 = LongEncoding.encodeNumber(num1);
        String encoded2 = LongEncoding.encodeNumber(num2);
        int cmp = Bytes.compare(encoded1.getBytes(), encoded2.getBytes());

        if (cmpExpected == 0) {
            Assert.assertEquals(0, cmp);
        } else if (cmpExpected > 0) {
            Assert.assertTrue(cmp > 0);
        } else {
            assert cmpExpected < 0;
            Assert.assertTrue(cmp < 0);
        }
    }
}
