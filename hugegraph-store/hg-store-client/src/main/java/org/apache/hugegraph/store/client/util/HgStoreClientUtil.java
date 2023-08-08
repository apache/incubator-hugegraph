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

package org.apache.hugegraph.store.client.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.client.type.HgStoreClientException;

import lombok.extern.slf4j.Slf4j;

/**
 * created on 2021/10/14
 */
@Slf4j
public final class HgStoreClientUtil {
    public static HgOwnerKey toOwnerKey(byte[] key) {
        return new HgOwnerKey(HgStoreClientConst.EMPTY_BYTES, key);
    }

    public static HgOwnerKey toOwnerKey(String key) {
        return new HgOwnerKey(HgStoreClientConst.EMPTY_BYTES, toBytes(key));
    }

    public static HgOwnerKey toAllNodeKey(String key) {
        return new HgOwnerKey(HgStoreClientConst.ALL_PARTITION_OWNER, toBytes(key));
    }

    public static HgOwnerKey toOwnerKey(String owner, String key) {
        return new HgOwnerKey(toBytes(owner), toBytes(key));
    }

    public static HgStoreClientException err(String msg) {
        log.error(msg);
        return HgStoreClientException.of(msg);
    }

    public static boolean isValid(HgOwnerKey key) {
        if (key == null) {
            return false;
        }
        if (key.getKey() == null) {
            return false;
        }
        return key.getKey().length != 0;
    }

    public static String toStr(byte[] b) {
        if (b == null) {
            return "";
        }
        if (b.length == 0) {
            return "";
        }
        return new String(b, StandardCharsets.UTF_8);
    }

    public static String toByteStr(byte[] b) {
        if (b == null) {
            return "";
        }
        if (b.length == 0) {
            return "";
        }
        return Arrays.toString(b);
    }

    public static String toStr(HgOwnerKey ownerKey) {
        if (ownerKey == null) {
            return "";
        }
        return "{ " +
               "owner: " + Arrays.toString(ownerKey.getOwner()) +
               ", key: " + toStr(ownerKey.getKey()) +
               " }";
    }

    public static byte[] toBytes(String str) {
        if (str == null) {
            return null;
        }
        return str.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] toBytes(long l) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(l);
        return buffer.array();
    }

    public static byte[] toIntBytes(final int i) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(i);
        return buffer.array();
    }

    public static long toLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip();//need flip
        return buffer.getLong();
    }

    public static int toInt(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.put(bytes);
        buffer.flip();//need flip
        return buffer.getInt();
    }

    public static String getHostAddress() {
        String res = null;

        try {
            res = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            res = "";
        }

        return res;
    }

    public static byte[] combine(byte[] first, byte[] second) {
        if (first == null) {
            first = HgStoreClientConst.EMPTY_BYTES;
        }

        if (second == null) {
            second = HgStoreClientConst.EMPTY_BYTES;
        }

        byte[] result = new byte[first.length + second.length];
        System.arraycopy(first, 0, result, 0, first.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }

    public static void printCallStack(String txt, Throwable ex) {
        StackTraceElement[] stackElements = ex.getStackTrace();
        StringBuilder sb = new StringBuilder();
        sb.append(txt).append(":\n");
        if (stackElements != null) {
            for (int i = 0; i < stackElements.length; i++) {
                sb.append(stackElements[i].getClassName()).append(" : ")
                  .append(stackElements[i].getMethodName()).append(" [ ");
                sb.append(stackElements[i].getLineNumber()).append(" ]\n");

            }
            sb.append(
                    "--------------------------------------------------------------------------------------\n");
        }
        log.error(sb.toString());
    }
}
