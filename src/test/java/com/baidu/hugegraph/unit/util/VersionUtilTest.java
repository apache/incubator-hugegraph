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

package com.baidu.hugegraph.unit.util;

import java.net.MalformedURLException;

import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.unit.BaseUnitTest;
import com.baidu.hugegraph.util.VersionUtil;
import com.baidu.hugegraph.util.VersionUtil.Version;

public class VersionUtilTest extends BaseUnitTest {

    @Test
    public void testVersionWithTwoPart() {
        Version version = Version.of("0.2");

        Assert.assertTrue(VersionUtil.match(version, "0", "1"));
        Assert.assertTrue(VersionUtil.match(version, "0.0", "1.0"));
        Assert.assertTrue(VersionUtil.match(version, "0.1", "0.3"));
        Assert.assertTrue(VersionUtil.match(version, "0.2", "0.3"));
        Assert.assertTrue(VersionUtil.match(version, "0.2", "0.2.1"));
        Assert.assertTrue(VersionUtil.match(version, "0.2", "0.2.01"));
        Assert.assertTrue(VersionUtil.match(version, "0.2", "0.10"));
        Assert.assertTrue(VersionUtil.match(version, "0.2", "1.0"));

        Assert.assertFalse(VersionUtil.match(version, "1", "0.3"));
        Assert.assertFalse(VersionUtil.match(version, "0.3", "0.4"));
        Assert.assertFalse(VersionUtil.match(version, "0.2", "0.1"));
        Assert.assertFalse(VersionUtil.match(version, "0.2", "0.2"));
        Assert.assertFalse(VersionUtil.match(version, "0.2", "0.2.0"));
        Assert.assertFalse(VersionUtil.match(version, "0.3", "0.1"));
    }

    @Test
    public void testVersionWithThreePart() {
        Version version = Version.of("3.2.5");

        Assert.assertTrue(VersionUtil.match(version, "2", "4"));
        Assert.assertTrue(VersionUtil.match(version, "3", "4"));
        Assert.assertTrue(VersionUtil.match(version, "3.1", "3.3"));
        Assert.assertTrue(VersionUtil.match(version, "3.2", "3.3"));
        Assert.assertTrue(VersionUtil.match(version, "3.2.4", "3.3"));
        Assert.assertTrue(VersionUtil.match(version, "3.2.5", "3.3"));
        Assert.assertTrue(VersionUtil.match(version, "3.2.5", "3.2.6"));
        Assert.assertTrue(VersionUtil.match(version, "3.2.5", "3.2.10"));
        Assert.assertTrue(VersionUtil.match(version, "3.2.5", "3.20.0"));

        Assert.assertFalse(VersionUtil.match(version, "2", "3"));
        Assert.assertFalse(VersionUtil.match(version, "3.3", "3.1"));
        Assert.assertFalse(VersionUtil.match(version, "3.3", "3.2"));
        Assert.assertFalse(VersionUtil.match(version, "3.3", "3.2"));
        Assert.assertFalse(VersionUtil.match(version, "3.2.5", "3.2.5"));
        Assert.assertFalse(VersionUtil.match(version, "3.2.6", "3.2.5"));
        Assert.assertFalse(VersionUtil.match(version, "3.2.6", "3.2.4"));
        Assert.assertFalse(VersionUtil.match(version, "3.2.50", "3.2.6"));
        Assert.assertFalse(VersionUtil.match(version, "3.20.0", "3.3"));
    }

    @Test
    public void testVersionGte() {
        String version = "0.2";

        Assert.assertTrue(VersionUtil.gte(version, "0.2"));
        Assert.assertTrue(VersionUtil.gte(version, "0.2.0"));
        Assert.assertTrue(VersionUtil.gte(version, "0.1"));
        Assert.assertTrue(VersionUtil.gte(version, "0.1.9"));
        Assert.assertTrue(VersionUtil.gte(version, "0.0.3"));

        Assert.assertFalse(VersionUtil.gte(version, "0.2.0.1"));
        Assert.assertFalse(VersionUtil.gte(version, "0.2.1"));
        Assert.assertFalse(VersionUtil.gte(version, "0.3"));
        Assert.assertFalse(VersionUtil.gte(version, "0.10"));
    }

    @Test
    public void testVersionCheck() {
        Version version = Version.of("0.6.5");

        VersionUtil.check(version, "0.6", "0.7", "test-component");
        VersionUtil.check(version, "0.6.5", "0.7", "test-component");
        VersionUtil.check(version, "0.6.5", "0.6.6", "test-component");

        Assert.assertThrows(IllegalStateException.class, () -> {
            VersionUtil.check(version, "0.6.5", "0.6.5", "test-component");
        });

        Assert.assertThrows(IllegalStateException.class, () -> {
            VersionUtil.check(version, "0.6.5", "0.6", "test-component");
        });

        Assert.assertThrows(IllegalStateException.class, () -> {
            VersionUtil.check(version, "0.7", "0.6", "test-component");
        });

        Assert.assertThrows(IllegalStateException.class, () -> {
            VersionUtil.check(version, "0.5", "0.6", "test-component");
        });

        Assert.assertThrows(IllegalStateException.class, () -> {
            VersionUtil.check(version, "0.7", "1.0", "test-component");
        });
    }

    @Test
    public void testGetImplementationVersion() throws MalformedURLException {
        // Can't mock Class: https://github.com/mockito/mockito/issues/1734
        //Class<?> clazz = Mockito.mock(Class.class);
        //Mockito.when(clazz.getSimpleName()).thenReturn("fake");
        //Mockito.when(clazz.getResource("fake.class")).thenReturn(manifest);

        String manifestPath = "file:./src/test/resources";
        Assert.assertEquals("1.8.6.0",
                            VersionUtil.getImplementationVersion(manifestPath));

        manifestPath = "file:./src/test/resources2";
        Assert.assertEquals(null,
                            VersionUtil.getImplementationVersion(manifestPath));
    }

    @Test
    public void testVersion() {
        // Test equals
        Version v1 = VersionUtil.Version.of("0.2.1");
        Version v2 = VersionUtil.Version.of("0.2.1");
        Assert.assertEquals(v1, v1);
        Assert.assertEquals(v1, v2);

        Version v3 = VersionUtil.Version.of("0.2.0");
        Version v4 = VersionUtil.Version.of("0.2");
        Assert.assertEquals(v3, v4);

        Version v5 = VersionUtil.Version.of("0.2.3");
        Version v6 = VersionUtil.Version.of("0.3.2");
        Assert.assertNotEquals(v5, v6);
        Assert.assertNotEquals(v5, null);
        Assert.assertNotEquals(v5, "0.2.3");

        // Test hashCode
        Assert.assertEquals(1023, v1.hashCode());
        Assert.assertEquals(1023, v2.hashCode());
        Assert.assertEquals(62, v3.hashCode());
        Assert.assertEquals(62, v4.hashCode());
        Assert.assertEquals(2945, v5.hashCode());
        Assert.assertEquals(2015, v6.hashCode());

        // Test compareTo
        Assert.assertEquals(0, v1.compareTo(v2));
        Assert.assertEquals(1, v1.compareTo(v3));
        Assert.assertEquals(-1, v1.compareTo(v5));
        Assert.assertEquals(1, v1.compareTo(null));
    }
}
