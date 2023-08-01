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

package org.apache.hugegraph.pd.common;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hugegraph.pd.common.HgAssert;
import org.junit.Test;

public class HgAssertTest {

    @Test(expected = IllegalArgumentException.class)
    public void testIsTrue() {
        HgAssert.isTrue(false, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIsTrue2() {
        HgAssert.isTrue(true, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIsFalse() {
        HgAssert.isFalse(true, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIsFalse2() {
        HgAssert.isTrue(false, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void isArgumentValid() {
        HgAssert.isArgumentValid(new byte[0], "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void isArgumentValidStr() {
        HgAssert.isArgumentValid("", "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIsArgumentNotNull() {
        HgAssert.isArgumentNotNull(null, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIstValid() {
        HgAssert.istValid(new byte[0], "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIstValidStr() {
        HgAssert.isValid("", "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIsNotNull() {
        HgAssert.isNotNull(null, "");
    }


    @Test
    public void testIsInvalid() {
        assertFalse(HgAssert.isInvalid("abc", "test"));
        assertTrue(HgAssert.isInvalid("", null));
    }

    @Test
    public void testIsInvalidByte() {
        assertTrue(HgAssert.isInvalid(new byte[0]));
        assertFalse(HgAssert.isInvalid(new byte[1]));
    }

    @Test
    public void testIsInvalidMap() {
        assertTrue(HgAssert.isInvalid(new HashMap<Integer, Integer>()));
        assertFalse(HgAssert.isInvalid(new HashMap<Integer, Integer>() {{
            put(1, 1);
        }}));
    }

    @Test
    public void testIsInvalidCollection() {
        assertTrue(HgAssert.isInvalid(new ArrayList<Integer>()));
        assertFalse(HgAssert.isInvalid(new ArrayList<Integer>() {{
            add(1);
        }}));
    }

    @Test
    public void testIsContains() {
        assertTrue(HgAssert.isContains(new Object[]{Integer.valueOf(1), Long.valueOf(2)},
                                       Long.valueOf(2)));
        assertFalse(HgAssert.isContains(new Object[]{Integer.valueOf(1), Long.valueOf(2)},
                                        Long.valueOf(3)));
    }

    @Test
    public void testIsContainsT() {
        assertTrue(HgAssert.isContains(new ArrayList<>() {{
            add(1);
        }}, 1));
        assertFalse(HgAssert.isContains(new ArrayList<>() {{
            add(1);
        }}, 2));
    }

    @Test
    public void testIsNull() {
        assertTrue(HgAssert.isNull(null));
        assertFalse(HgAssert.isNull("abc", "cdf"));
    }

}
