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

package raftcore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Test;

import com.google.protobuf.ZeroByteStringHelper;

public class ZeroByteStringHelperTest {

    private static final String STR = "hello word!";

    @Test
    public void testWrap() {
        byte[] b1 = new byte[]{10, 20, 30};
        byte[] b2 = new byte[]{40, 50};

        var h1 = ZeroByteStringHelper.wrap(b1);
        var h2 = ZeroByteStringHelper.wrap(b2, 0, b2.length);

        ByteBuffer buffer = ByteBuffer.allocate(5);
        buffer.put(b1);
        buffer.put(b2);
        var h3 = ZeroByteStringHelper.wrap(buffer);
        assertEquals(h3.isEmpty(), true);
        var h4 = ZeroByteStringHelper.concatenate(h1, h2);
        assertTrue(Arrays.equals(ZeroByteStringHelper.getByteArray(h4), buffer.array()));
    }

    @Test
    public void testConcatenate() {
        byte[] b1 = new byte[]{10, 20, 30};
        byte[] b2 = new byte[]{40, 50};
        ByteBuffer buffer1 = ByteBuffer.allocate(5);
        buffer1.put(b1);

        ByteBuffer buffer2 = ByteBuffer.allocate(5);
        buffer1.put(b2);

        var array = new ArrayList<ByteBuffer>();
        array.add(buffer1);
        array.add(buffer2);

        var bs = ZeroByteStringHelper.concatenate(array);
        assertEquals(bs.toByteArray().length, 5);
    }
}
