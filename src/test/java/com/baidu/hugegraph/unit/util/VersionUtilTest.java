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
}
