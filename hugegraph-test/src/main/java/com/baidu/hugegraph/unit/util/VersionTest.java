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
import com.baidu.hugegraph.util.VersionUtil;
import com.baidu.hugegraph.version.CoreVersion;

public class VersionTest {

    @Test
    public void testCoreVersionGet() {
        String pomVersion = VersionUtil.getPomVersion();
        Assert.assertNotNull(pomVersion);
        String version = CoreVersion.VERSION.get();
        Assert.assertNotNull(version);
        Assert.assertEquals(pomVersion, version);
    }

    @Test
    public void testCoreVersionCheck() {
        // Expect not throw exception
        CoreVersion.check();
    }
}
