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

package org.apache.hugegraph.store.common;

import org.apache.hugegraph.store.term.Bits;
import org.junit.Assert;
// import org.junit.Test;

public class BitsTest {
    // @Test
    public void test() {
        for (int i = 0; i < Integer.MAX_VALUE; i = i + 10) {
            byte[] val = new byte[4];
            Bits.putInt(val, 0, i);
            int n = Bits.getInt(val, 0);
            Assert.assertEquals(i, n);
        }
    }
}
