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

import java.util.List;
import java.util.Map;

import org.apache.hugegraph.store.client.util.HgAssert;
import org.junit.Test;

public class HgAssertTest {

    @Test
    public void testIsTrue1() {
        // Setup
        // Run the test
        try {
            HgAssert.isTrue(false, "message");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testIsTrue2() {
        try {
            HgAssert.isTrue(false, () -> "message");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testIsFalse1() {
        try {
            HgAssert.isFalse(true, "message");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testIsFalse2() {
        try {
            HgAssert.isFalse(true, () -> "message");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testIsArgumentValid1() {
        // Setup
        // Run the test
        HgAssert.isArgumentValid("content".getBytes(), "parameter");

        // Verify the results
    }

    @Test
    public void testIsArgumentValid2() {
        // Setup
        // Run the test
        HgAssert.isArgumentValid("str", "parameter");

        // Verify the results
    }

    @Test
    public void testIsArgumentValid3() {
        // Setup
        // Run the test
        HgAssert.isArgumentValid(List.of("value"), "parameter");

        // Verify the results
    }

    @Test
    public void testIsArgumentNotNull() {
        // Setup
        // Run the test
        HgAssert.isArgumentNotNull("obj", "parameter");

        // Verify the results
    }

    @Test
    public void testIstValid() {
        // Setup
        // Run the test
        HgAssert.istValid("content".getBytes(), "message");

        // Verify the results
    }

    @Test
    public void testIsValid() {
        // Setup
        // Run the test
        HgAssert.isValid("str", "message");

        // Verify the results
    }

    @Test
    public void testIsNotNull() {
        // Setup
        // Run the test
        HgAssert.isNotNull("obj", "message");

        // Verify the results
    }

    @Test
    public void testIsContains1() {
        assertTrue(HgAssert.isContains(new Object[]{"obj"}, "obj"));
    }

    @Test
    public void testIsInvalid1() {
        assertFalse(HgAssert.isInvalid("strs"));
    }

    @Test
    public void testIsInvalid2() {
        assertFalse(HgAssert.isInvalid("content".getBytes()));
    }

    @Test
    public void testIsInvalid3() {
        // Setup
        final Map<?, ?> map = Map.ofEntries(Map.entry("value", "value"));
        // Verify the results
        assertFalse(HgAssert.isInvalid(map));
    }

    @Test
    public void testIsInvalid4() {
        assertFalse(HgAssert.isInvalid(List.of("value")));
    }

    @Test
    public void testIsContains2() {
        assertTrue(HgAssert.isContains(List.of("item"), "item"));
    }

    @Test
    public void testIsNull() {
        assertFalse(HgAssert.isNull("objs"));
    }
}
