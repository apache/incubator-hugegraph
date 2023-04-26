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

package client;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.store.term.Bits;
import com.baidu.hugegraph.store.term.HgPair;
import com.baidu.hugegraph.store.term.HgTriple;

public class HgPairTest {

    private HgPair<String, String> pair;
    private HgTriple<String, String, String> triple;

    @Before
    public void setUp() {
        pair = new HgPair<>("key", "value");
        triple = new HgTriple<>("x", "y", "z");
    }

    @Test
    public void testPair() {
        int hashCode = pair.hashCode();
        pair.toString();
        pair.setKey("key1");
        pair.setValue("value1");
        pair.getKey();
        pair.getValue();
        assertTrue(new HgPair<>("key1", "value1").equals(pair));
        var pair2 = new HgPair<>();
        pair2.setKey("key1");
        pair2.hashCode();
        assertFalse(pair2.equals(pair));
        triple.getZ();
        triple.getX();
        triple.getY();
        triple.toString();
        triple.hashCode();
        triple.hashCode();
        assertTrue(triple.equals(new HgTriple<>("x", "y", "z")));
        assertFalse(pair2.equals(triple));
    }

    @Test
    public void testBits() {
        byte[] buf = new byte[4];
        Bits.putInt(buf, 0, 3);
        int i = Bits.getInt(buf, 0);
        assertTrue(i == 3);
        buf = new byte[2];
        Bits.putShort(buf, 0, 2);
        int s = Bits.getShort(buf, 0);
        assertTrue(s == 2);
        buf = new byte[4];
        Bits.put(buf, 0, new byte[]{0, 0, 0, 66});
        int toInt = Bits.toInt(buf);
        assertTrue(toInt == 66);
    }
}
