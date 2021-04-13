/*
 * Copyright 2017 HugeGraph Authors
 *
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

package com.baidu.hugegraph.unit.core;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.util.collection.Int2IntsMap;

public class Int2IntsMapTest {

    @Before
    public void setup() {
        // pass
    }

    @After
    public void teardown() {
        // pass
    }

    @Test
    public void testInt2IntsMap() {
        Int2IntsMap map = new Int2IntsMap();

        int i = 100;
        int j = 200;
        int k = 250;
        int l = 255;
        int m = 270;
        Random random = new Random();
        for (int n = 0; n < 1000; n++) {
            switch (random.nextInt() % 5) {
                case 0:
                    if (i < 200) {
                        map.add(1, i++);
                    }
                    break;
                case 1:
                    if (j < 250) {
                        map.add(2, j++);
                    }
                    break;
                case 2:
                    if (k < 255) {
                        map.add(3, k++);
                    }
                    break;
                case 3:
                    if (l < 270) {
                        map.add(4, l++);
                    }
                    break;
                case 4:
                    if (m < 300) {
                        map.add(5, m++);
                    }
                    break;
            }
        }

        int[][] results = new int[5][];
        results[0] = new int[]{100,101,102,103,104,105,106,107,108,109,
                               110,111,112,113,114,115,116,117,118,119,
                               120,121,122,123,124,125,126,127,128,129,
                               130,131,132,133,134,135,136,137,138,139,
                               140,141,142,143,144,145,146,147,148,149,
                               150,151,152,153,154,155,156,157,158,159,
                               160,161,162,163,164,165,166,167,168,169,
                               170,171,172,173,174,175,176,177,178,179,
                               180,181,182,183,184,185,186,187,188,189,
                               190,191,192,193,194,195,196,197,198,199};
        results[1] = new int[]{200,201,202,203,204,205,206,207,208,209,
                               210,211,212,213,214,215,216,217,218,219,
                               220,221,222,223,224,225,226,227,228,229,
                               230,231,232,233,234,235,236,237,238,239,
                               240,241,242,243,244,245,246,247,248,249};
        results[2] = new int[]{250,251,252,253,254};
        results[3] = new int[]{255,256,257,258,259,260,261,262,
                               263,264,265,266,267,268,269};
        results[4] = new int[]{270,271,272,273,274,275,276,277,278,279,
                               280,281,282,283,284,285,286,287,288,289,
                               290, 291,292,293,294,295,296,297,298,299};

        for (int ii = 0; ii < 5; ii++) {
            int[] result = map.get(ii + 1);
            Assert.assertTrue(Arrays.equals(results[ii], result));
        }
    }

    @Test
    public void testInt2IntsMapRandom() {
        Int2IntsMap map = new Int2IntsMap();

        Random random = new Random();
        for (int i = 0; i < 1000; i++) {
            map.add(Math.abs(random.nextInt()) % 5, i);
        }

        Set<Integer> results = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            int[] result = map.get(i);
            for (int j : result) {
                results.add(j);
            }
        }
        Assert.assertEquals(1000, results.size());
    }
}
