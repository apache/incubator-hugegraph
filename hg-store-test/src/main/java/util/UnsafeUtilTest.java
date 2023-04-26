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

package util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;

import org.apache.hugegraph.store.util.UnsafeUtf8Util;
import org.apache.hugegraph.store.util.UnsafeUtil;
import org.junit.Test;

import lombok.Data;
import sun.misc.Unsafe;

public class UnsafeUtilTest {


    @Test
    public void testHasUnsafe() {
        assertTrue(UnsafeUtil.hasUnsafe());
    }

    @Test
    public void testPutByte() {
        UnsafeUtil.putByte("content".getBytes(), 0L, (byte) 99);
        assertEquals((byte) 99, UnsafeUtil.getByte("content".getBytes(), 0L));
    }

    @Test
    public void testMoveToString() {
        assertEquals("a", UnsafeUtil.moveToString(new char[]{'a'}));
    }

    @Test
    public void testEncodedLength() {
        assertEquals(10, UnsafeUtf8Util.encodedLength("aa中文aa"));
    }

    @Test
    public void testEncodeUtf8() {
        assertEquals(10, UnsafeUtf8Util.encodeUtf8("aa中文aa", new byte[16], 0, 16));
    }

    @Test
    public void testDecodeUtf8() {
        assertEquals("co", UnsafeUtf8Util.decodeUtf8("content".getBytes(), 0, 2));
    }

    @Test
    public void testUnsafeUtf8Util() {
        String content = "content";
        UnsafeUtf8Util.decodeUtf8(content.getBytes(), 0, content.length());
        byte[] out = new byte[content.length()];
        UnsafeUtf8Util.encodeUtf8(content, out, 0, content.length());
        UnsafeUtf8Util.encodedLength(content);
    }

    @Test
    public void testUnsafeAccessor() {
        Unsafe unsafe = null;
        long offset = 0;
        try {
            Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            unsafe = (Unsafe) unsafeField.get(null);
            UnsafeUtil.UnsafeAccessor acc = new UnsafeUtil.UnsafeAccessor(unsafe);
            acc.getUnsafe();
            TestObject to = new TestObject();
            byte byteValue = 126;
            offset = acc.objectFieldOffset(TestObject.class.getDeclaredField("b"));
            acc.putByte(to, offset, byteValue);
            byte b = acc.getByte(to, offset);
            assertEquals(byteValue, b);
            short shortValue = 1;
            acc.putShort(to, offset, shortValue);
            short shortResult = acc.getShort(to, offset);
            assertEquals(shortValue, shortResult);
            int intValue = 99;
            acc.putInt(to, offset, intValue);
            int i = acc.getInt(to, offset);
            assertEquals(intValue, i);
            long longValue = 11L;
            acc.putLong(to, offset, longValue);
            long l = acc.getLong(to, offset);
            assertEquals(longValue, l);
            acc.putBoolean(to, offset, false);
            assertFalse(acc.getBoolean(to, offset));
            float f = 1;
            acc.putFloat(to, offset, f);
            float v = acc.getFloat(to, offset);
            assertEquals(f, v, 0.0);
            double d = 2;
            acc.putDouble(to, offset, d);
            double v1 = acc.getDouble(to, offset);
            assertEquals(d, v1, 0.0);
            TestObject o1 = new TestObject();
            acc.putObject(to, offset, o1);
            Object o2 = acc.getObject(to, offset);
            assertEquals(o1, o2);
            offset = unsafe.allocateMemory(1024);
            acc.putByte(offset, byteValue);
            byte bResult = acc.getByte(offset);
            assertEquals(byteValue, bResult);
            acc.putShort(offset, shortValue);
            short aShort1 = acc.getShort(offset);
            assertEquals(aShort1, shortValue);
            acc.putInt(offset, intValue);
            assertEquals(acc.getInt(offset), intValue);
            acc.putLong(offset, longValue);
            assertEquals(acc.getLong(offset), longValue);
            unsafe.freeMemory(offset);
            offset = acc.objectFieldOffset(TestObject.class.getDeclaredField("b"));
            acc.putByteVolatile(to, offset, byteValue);
            b = acc.getByteVolatile(to, offset);
            assertEquals(byteValue, b);
            acc.putShortVolatile(to, offset, shortValue);
            shortResult = acc.getShortVolatile(to, offset);
            assertEquals(shortValue, shortResult);
            acc.putIntVolatile(to, offset, intValue);
            i = acc.getIntVolatile(to, offset);
            assertEquals(intValue, i);
            acc.putLongVolatile(to, offset, longValue);
            l = acc.getLongVolatile(to, offset);
            assertEquals(longValue, l);
            acc.putBooleanVolatile(to, offset, false);
            assertFalse(acc.getBooleanVolatile(to, offset));
            acc.putFloatVolatile(to, offset, f);
            v = acc.getFloatVolatile(to, offset);
            assertEquals(f, v, 0.0);
            acc.putDoubleVolatile(to, offset, d);
            v1 = acc.getDoubleVolatile(to, offset);
            assertEquals(d, v1, 0.0);
            acc.putObjectVolatile(to, offset, o1);
            o2 = acc.getObjectVolatile(to, offset);
            assertEquals(o1, o2);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Data
    private class TestObject {
        private Object o;
        private byte b;

        public TestObject() {

        }

        public TestObject(Object o, byte b) {
            this.o = o;
            this.b = b;
        }
    }

}
