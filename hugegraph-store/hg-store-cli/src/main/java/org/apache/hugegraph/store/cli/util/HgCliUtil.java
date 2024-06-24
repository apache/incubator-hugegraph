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

package org.apache.hugegraph.store.cli.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.HgStoreSession;
import org.apache.hugegraph.store.client.util.HgStoreClientConst;

/**
 * 2022/2/14
 */
public class HgCliUtil {

    public final static String TABLE_NAME = "cli-table";

    public static Map<HgOwnerKey, byte[]> batchPut(HgStoreSession session, String keyPrefix) {
        return batchPut(session, keyPrefix, 100);
    }

    public static Map<HgOwnerKey, byte[]> batchPut(HgStoreSession session, String keyPrefix,
                                                   int loop) {
        return batchPut(session, TABLE_NAME, keyPrefix, loop);
    }

    public static Map<HgOwnerKey, byte[]> batchPut(HgStoreSession session, String tableName
            , String keyPrefix, int loop) {
        return batchPut(session, tableName, keyPrefix, loop, 1, key -> toOwnerKey(key));
    }

    public static Map<HgOwnerKey, byte[]> batchPut(HgStoreSession session, String tableName
            , String keyPrefix, int loop, int start) {
        return batchPut(session, tableName, keyPrefix, loop, start, key -> toOwnerKey(key));
    }

    public static Map<HgOwnerKey, byte[]> batchPut(HgStoreSession session, String tableName
            , String keyPrefix, int loop, Function<String, HgOwnerKey> f) {
        return batchPut(session, tableName, keyPrefix, loop, 1, f);
    }

    public static Map<HgOwnerKey, byte[]> batchPut(HgStoreSession session, String tableName
            , String keyPrefix, int loop, int start, Function<String, HgOwnerKey> f) {

        Map<HgOwnerKey, byte[]> res = new LinkedHashMap<>();

        int length = String.valueOf(loop).length();

        session.beginTx();
        for (int i = start; i <= loop; i++) {

            HgOwnerKey key = f.apply(keyPrefix + "-" + padLeftZeros(String.valueOf(i), length));

            byte[] value = toBytes(keyPrefix + "-V-" + i);
            res.put(key, value);
            session.put(tableName, key, value);

            if ((i + 1) % 10000 == 0) {
                println("commit: " + (i + 1));
                session.commit();
                session.beginTx();
            }
        }
        if (session.isTx()) {
            session.commit();
        }

        return res;
    }

    public static void printNum(List<HgKvEntry> list, String title) {
        if (list == null) {
            return;
        }

        println(title + " size: " + list.size());
    }

    public static void println(Iterator<HgKvEntry> iterator) {
        if (iterator == null) {
            return;
        }
        while (iterator.hasNext()) {
            println(iterator.next());
        }

    }

    public static void printOwner(List<HgOwnerKey> list) {
        if (list == null) {
            return;
        }

        for (HgOwnerKey entry : list) {
            println(entry);
        }
    }

    public static void println(List<HgKvEntry> list) {
        if (list == null) {
            return;
        }

        for (HgKvEntry entry : list) {
            println(entry);
        }
    }

    public static void println(List<HgKvEntry> list, int mod) {
        if (list == null) {
            return;
        }

        for (int i = 0; i < list.size(); i++) {
            if (i % mod == 0) {
                println(list.get(i));
            }
        }
    }

    public static void println(HgKvEntry kv) {
        if (kv == null) {
            System.out.println("null");
            return;
        }
        println("[ " + toStr(kv.key()) + " : " + toStr(kv.value()) + " ]");
    }

    public static void println(HgOwnerKey key) {
        if (key == null) {
            System.out.println("null");
            return;
        }
        println("[ " + toInt(key.getOwner()) + " : " + toStr(key.getKey()) + " ]");
    }

    public static void println(String str) {
        System.out.println(str);
    }

    public static HgOwnerKey toOwnerKey(String key) {
        return new HgOwnerKey(getOwner(key), toBytes(key));
    }

    public static HgOwnerKey toOwnerKey(byte[] key) {
        return new HgOwnerKey(getOwner(key), key);
    }

    private static byte[] getOwner(String key) {
        return getOwner(toBytes(key));
    }

    private static byte[] getOwner(byte[] key) {
        return toBytes(Arrays.hashCode(key));
    }

    public static HgOwnerKey toAllPartitionKey(String key) {
        return HgOwnerKey.of(HgStoreClientConst.ALL_PARTITION_OWNER, toBytes(key));
    }

    public static HgOwnerKey toOwnerKey(String owner, String key) {
        return HgOwnerKey.of(toBytes(owner), toBytes(key));
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

    private static byte[] toBytes(final int i) {
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

    public static long toInt(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.put(bytes);
        buffer.flip();//need flip
        return buffer.getInt();
    }

    public static String padLeftZeros(String str, int n) {
        return String.format("%1$" + n + "s", str).replace(' ', '0');
    }

    public static String toSuffix(int num, int length) {
        return "-" + padLeftZeros(String.valueOf(num), length);
    }

    public static int amountOf(List list) {
        if (list == null) {
            return 0;
        }
        return list.size();
    }

    public static int amountOf(Iterator iterator) {
        if (iterator == null) {
            return 0;
        }
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        return count;
    }
}
