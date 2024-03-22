/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.pd.common;

import java.nio.charset.StandardCharsets;

import org.junit.Assert;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PartitionUtilsTest {

    @Test
    public void testCalcHashcode() {
        byte[] key = new byte[5];
        long code = PartitionUtils.calcHashcode(key);
        Assert.assertEquals(code, 31912L);
    }

    @Test
    public void testHashCode() {
        int partCount = 10;
        int partSize = PartitionUtils.MAX_VALUE / partCount + 1;
        int[] counter = new int[partCount];
        for (int i = 0; i < 10000; i++) {
            String s = String.format("BATCH-GET-UNIT-%02d", i);
            int c = PartitionUtils.calcHashcode(s.getBytes(StandardCharsets.UTF_8));
            counter[c / partSize]++;
        }
        for (int i = 0; i < counter.length; i++) {
            System.out.println(i + " " + counter[i]);
        }
    }
}
