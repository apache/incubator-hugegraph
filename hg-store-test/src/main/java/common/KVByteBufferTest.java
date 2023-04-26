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

package common;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.apache.hugegraph.store.buffer.KVByteBuffer;
import org.junit.Test;

public class KVByteBufferTest {

    @Test
    public void testOps() {
        KVByteBuffer buffer1 = new KVByteBuffer(10);
        buffer1.put((byte) 10);
        // just put a byte
        assertEquals(1, buffer1.position());
        // 9 left
        assertTrue(buffer1.hasRemaining());
        buffer1.clear();
        assertEquals(10, buffer1.get());

        buffer1.clear();
        buffer1.putInt(10);
        buffer1.clear();
        assertEquals(10, buffer1.getInt());

        buffer1.flip();
        // just write to a int
        assertEquals(4, buffer1.getBuffer().limit());

        byte[] bytes = new byte[]{10, 20, 30};
        KVByteBuffer buffer2 = new KVByteBuffer(bytes);
        assertArrayEquals(buffer2.array(), bytes);


        ByteBuffer bb = ByteBuffer.allocate(10);
        KVByteBuffer buffer3 = new KVByteBuffer(bb);
        buffer3.put(bytes);
        buffer3.clear();
        assertArrayEquals(buffer3.getBytes(), bytes);

        // int (4) + byte(3)
        assertEquals(7, buffer3.getBuffer().position());

        ByteBuffer bb2 = buffer3.copyBuffer();
        assertEquals(7, bb2.capacity());
    }
}
