package com.baidu.hugegraph.store.node.util;

import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * @author lynn.bond@hotmail.com created on 2021/10/29
 */
@Slf4j
public final class HgStoreNodeUtil {
    public static String toStr(byte[] b) {
        if (b == null) return "";
        if (b.length == 0) return "";
        return new String(b, StandardCharsets.UTF_8);
    }

    public static byte[] toBytes(String str) {
        if (str == null) return null;
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

    public static int toInt(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.put(bytes);
        buffer.flip();//need flip
        return buffer.getInt();
    }

    public static String toUuidStr(byte[] bytes) {
        try {
            return UUID.nameUUIDFromBytes(bytes).toString();
        } catch (Throwable t) {
            log.error("Failed to parse bytes to UUID",t);
        }
        return null;
    }

}