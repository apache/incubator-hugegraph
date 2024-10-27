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

package org.apache.hugegraph.util;

public final class LongEncoding {

    private static final String B64_SYMBOLS =
            "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz~";

    private static final String LENGTH_SYMBOLS = "0123456789ABCDEF";
    private static final char SORTABLE_NEG = LENGTH_SYMBOLS.charAt(0);
    private static final char SIGNED_NEG = '-';

    private static final long FULL_LONG = Long.MIN_VALUE;

    public static String encodeNumber(Object number) {
        Number num = NumericUtil.convertToNumber(number);
        long value = NumericUtil.numberToSortableLong(num);
        return encodeSortable(value);
    }

    public static Number decodeNumber(String str, Class<?> clazz) {
        long value = decodeSortable(str);
        return NumericUtil.sortableLongToNumber(value, clazz);
    }

    public static String encodeSortable(long num) {
        boolean negative = false;
        if (num < 0L) {
            negative = true;
            num += FULL_LONG;
        }

        String encoded = encode(num, B64_SYMBOLS);
        int length = encoded.length();
        E.checkArgument(length <= LENGTH_SYMBOLS.length(),
                        "Length symbols can't represent encoded number '%s'",
                        encoded);
        StringBuilder sb = new StringBuilder(length + 2);
        if (negative) {
            sb.append(SORTABLE_NEG);
        }
        char len = LENGTH_SYMBOLS.charAt(length);
        sb.append(len);
        sb.append(encoded);

        return sb.toString();
    }

    public static long decodeSortable(String str) {
        E.checkArgument(str.length() >= 2,
                        "Length of sortable encoded string must be >=2");
        boolean negative = str.charAt(0) == SORTABLE_NEG;
        int lengthPos = 0;
        if (negative) {
            lengthPos = 1;
        }
        int length = B64_SYMBOLS.indexOf(str.charAt(lengthPos));
        E.checkArgument(length == str.length() - lengthPos - 1,
                        "Can't decode illegal string '%s' with wrong length",
                        str);
        String encoded = str.substring(lengthPos + 1);
        long value = decode(encoded, B64_SYMBOLS);
        if (negative) {
            value -= FULL_LONG;
        }
        return value;
    }

    public static String encodeSignedB64(long value) {
        boolean negative = false;
        if (value < 0L) {
            negative = true;
            if (value == FULL_LONG) {
                return "-80000000000";
            }
            value = -value;
        }
        assert value >= 0L : value;
        String encoded = encodeB64(value);
        return negative ? SIGNED_NEG + encoded : encoded;
    }

    public static long decodeSignedB64(String value) {
        boolean negative = false;
        if (!value.isEmpty() && value.charAt(0) == SIGNED_NEG) {
            negative = true;
            value = value.substring(1);
        }
        long decoded = decodeB64(value);
        return negative ? -decoded : decoded;
    }

    public static boolean validB64Char(char c) {
        return B64_SYMBOLS.indexOf(c) != -1;
    }

    public static String encodeB64(long num) {
        return encode(num, B64_SYMBOLS);
    }

    public static long decodeB64(String str) {
        return decode(str, B64_SYMBOLS);
    }

    public static long decode(String encoded, String symbols) {
        final int B = symbols.length();
        E.checkArgument(B > 0, "The symbols parameter can't be empty");
        long num = 0L;
        for (char ch : encoded.toCharArray()) {
            num *= B;
            int pos = symbols.indexOf(ch);
            if (pos < 0) {
                throw new NumberFormatException(String.format(
                          "Can't decode symbol '%s' in string '%s'",
                          ch, encoded));
            }
            num += pos;
        }
        return num;
    }

    public static String encode(long num, String symbols) {
        final int B = symbols.length();
        E.checkArgument(num >= 0L, "Expected non-negative number: %s", num);
        E.checkArgument(B > 0, "The symbols parameter can't be empty");

        StringBuilder sb = new StringBuilder();
        do {
            sb.append(symbols.charAt((int) (num % B)));
            num /= B;
        } while (num != 0L);

        return sb.reverse().toString();
    }
}
