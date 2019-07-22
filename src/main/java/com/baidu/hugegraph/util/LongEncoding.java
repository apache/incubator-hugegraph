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

package com.baidu.hugegraph.util;

/**
 * Utility class for encoding longs in strings based on:
 * @see <a href="http://stackoverflow.com/questions/2938482/encode-decode-a-long-to-a-string-using-a-fixed-set-of-letters-in-java">encode decode long to string</a>
 */
public final class LongEncoding {

    private static final String BASE_SYMBOLS =
            "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz~";

    private static final String LENGTH_SYMBOLS = "0123456789ABCDEF";
    private static final char NEG = LENGTH_SYMBOLS.charAt(0);

    private static final long FULL_LONG = Long.MIN_VALUE;

    public static String encodeNumber(Object number) {
        Number num = NumericUtil.convertToNumber(number);
        long value = NumericUtil.numberToSortableLong(num);
        return encodeSortable(value);
    }

    public static Number decodeNumber(String str, Class<?> clazz) {
        long value = decodeSortable(str);
        Number number = NumericUtil.sortableLongToNumber(value, clazz);
        return number;
    }

    public static String encodeSortable(long num) {
        boolean negative = false;
        if (num < 0L) {
            negative = true;
            num += FULL_LONG;
        }

        String encoded = encode(num, BASE_SYMBOLS);
        int length = encoded.length();
        E.checkArgument(length <= LENGTH_SYMBOLS.length(),
                        "Length symbols can't represent encoded number '%s'",
                        encoded);
        StringBuilder sb = new StringBuilder(length + 2);
        if (negative) {
            sb.append(NEG);
        }
        char len = LENGTH_SYMBOLS.charAt(length);
        sb.append(len);
        sb.append(encoded);

        return sb.toString();
    }

    public static long decodeSortable(String str) {
        E.checkArgument(str.length() >= 2,
                        "Length of sortable encoded string must be >=2");
        boolean negative = str.charAt(0) == NEG;
        int lengthPos = 0;
        if (negative) {
            lengthPos = 1;
        }
        int length = BASE_SYMBOLS.indexOf(str.charAt(lengthPos));
        E.checkArgument(length == str.length() - lengthPos - 1,
                        "Can't decode illegal string '%s' with wrong length",
                        str);
        String encoded = str.substring(lengthPos + 1);
        long value = decode(encoded);
        if (negative) {
            value -= FULL_LONG;
        }
        return value;
    }

    public static boolean validSortableChar(char c) {
        return BASE_SYMBOLS.indexOf(c) != -1;
    }

    public static String encode(long num) {
        return encode(num, BASE_SYMBOLS);
    }

    public static long decode(String str) {
        return decode(str, BASE_SYMBOLS);
    }

    public static long decode(String str, String symbols) {
        final int B = symbols.length();
        E.checkArgument(B > 0, "The symbols parameter can't be empty");
        long num = 0;
        for (char ch : str.toCharArray()) {
            num *= B;
            int pos = symbols.indexOf(ch);
            if (pos < 0)
                throw new NumberFormatException(
                          "Symbol set does not match string");
            num += pos;
        }
        return num;
    }

    public static String encode(long num, String symbols) {
        final int B = symbols.length();
        E.checkArgument(num >= 0, "Expected non-negative number: %s", num);
        E.checkArgument(B > 0, "The symbols parameter can't be empty");

        StringBuilder sb = new StringBuilder();
        do {
            sb.append(symbols.charAt((int) (num % B)));
            num /= B;
        } while (num != 0);

        return sb.reverse().toString();
    }
}
