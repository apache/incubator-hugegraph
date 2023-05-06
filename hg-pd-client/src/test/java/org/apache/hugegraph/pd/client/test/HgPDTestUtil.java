package org.apache.hugegraph.pd.client.test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

/**
 * @author lynn.bond@hotmail.com created on 2021/11/8
 */
public class HgPDTestUtil {

    public static void println(Object str) {
        System.out.println(str);
    }

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
        if (iterator == null) return 0;
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        return count;
    }
}
