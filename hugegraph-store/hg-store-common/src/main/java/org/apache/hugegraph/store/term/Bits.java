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

package org.apache.hugegraph.store.term;

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
        x = (x << 8) + (buf[offSet + 1] & 0xff);
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
