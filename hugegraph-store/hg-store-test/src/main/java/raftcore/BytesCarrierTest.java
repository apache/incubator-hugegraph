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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;

import com.google.protobuf.BytesCarrier;

public class BytesCarrierTest {

    @Test
    public void testWrite() throws IOException {
        byte[] bytes = new byte[]{10, 20, 30};
        BytesCarrier carrier = new BytesCarrier();

        // not valid
        carrier.write((byte) 1);
        assertNull(carrier.getValue());
        assertFalse(carrier.isValid());

        // not valid
        ByteBuffer buffer = ByteBuffer.allocate(10);
        carrier.write(buffer);
        assertNull(carrier.getValue());

        // not valid
        carrier.writeLazy(buffer);
        assertNull(carrier.getValue());

        // ok, write done
        carrier.write(bytes, 0, bytes.length);
        assertNotNull(carrier.getValue());
        assertTrue(carrier.isValid());

        // has data
        carrier.writeLazy(bytes, 0, bytes.length);
        assertFalse(carrier.isValid());
    }
}
