package com.baidu.hugegraph.store.term;

import java.nio.ByteBuffer;

public class Bits {
    /**
     * 大头字节序写入short
     */
    public static void putShort(byte[] buf, int offSet, int x) {
        buf[offSet] = (byte) (x >> 8);
        buf[offSet + 1] = (byte) (x);
    }

    public static void putInt(byte[] buf, int offSet, int x) {
        buf[offSet] = (byte) (x >> 24);
        buf[offSet + 1] = (byte) (x >> 16);
        buf[offSet + 2] = (byte) (x >> 8);
        buf[offSet + 3] = (byte) (x);
    }
    /**
     * 大头字节序读取short
     */
    public static int getShort(byte[] buf, int offSet) {
        int x = buf[offSet] & 0xff;
        x = (x  << 8) + (buf[offSet + 1] & 0xff);
        return x;
    }

    public static int getInt(byte[] buf, int offSet) {
        int x = (buf[offSet] << 24)
                + ((buf[offSet + 1] & 0xff) << 16)
                + ((buf[offSet + 2] & 0xff) << 8)
                + (buf[offSet + 3] & 0xff);
        return x;
    }
    public static void put(byte[] buf, int offSet, byte[] srcBuf) {
        System.arraycopy(srcBuf, 0, buf, offSet, srcBuf.length);
    }

    public static int toInt(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.put(bytes);
        buffer.flip();//need flip
        return buffer.getInt();
    }
}
