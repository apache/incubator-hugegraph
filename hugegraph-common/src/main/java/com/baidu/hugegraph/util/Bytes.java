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

package com.baidu.hugegraph.util;

import java.util.Arrays;
import java.util.Comparator;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import com.google.common.primitives.UnsignedBytes;

/**
 * TODO: extends com.google.common.primitives.Bytes
 */
public final class Bytes {

    public static final long BASE = 1024L;
    public static final long KB = BASE;
    public static final long MB = KB * BASE;
    public static final long GB = MB * BASE;
    public static final long TB = GB * KB;
    public static final long PB = GB * MB;
    public static final long EB = GB * GB;

    private static final Comparator<byte[]> CMP =
                         UnsignedBytes.lexicographicalComparator();

    public static int compare(byte[] bytes1, byte[] bytes2) {
        return CMP.compare(bytes1, bytes2);
    }

    public static byte[] concat(byte[] bytes1, byte[] bytes2) {
        byte[] result = new byte[bytes1.length + bytes2.length];
        System.arraycopy(bytes1, 0, result, 0, bytes1.length);
        System.arraycopy(bytes2, 0, result, bytes1.length, bytes2.length);
        return result;
    }

    public static boolean prefixWith(byte[] bytes, byte[] prefix) {
        if (bytes.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (bytes[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    public static boolean contains(byte[] bytes, byte value) {
        for (byte b : bytes) {
            if (b == value) {
                return true;
            }
        }
        return false;
    }

    public static int indexOf(byte[] bytes, byte value) {
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] == value) {
                return i;
            }
        }
        return -1;
    }

    public static boolean equals(byte[] bytes1, byte[] bytes2) {
        return Arrays.equals(bytes1, bytes2);
    }

    public static String toHex(byte b) {
        return toHex(new byte[]{b});
    }

    public static String toHex(byte[] bytes) {
        return new String(Hex.encodeHex(bytes));
    }

    public static byte[] fromHex(String hex) {
        try {
            return Hex.decodeHex(hex.toCharArray());
        } catch (DecoderException e) {
            throw new RuntimeException("Failed to decode hex: " + hex, e);
        }
    }
}
