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

package org.apache.hugegraph.unit.util;

import org.junit.Test;
import org.slf4j.Logger;

import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.util.Log;
import org.apache.hugegraph.testutil.Assert;

public class LogTest extends BaseUnitTest {

    @Test
    public void testLog() {
        Logger log1 = Log.logger(LogTest.class);
        Logger log2 = Log.logger("org.apache.hugegraph.unit.util.LogTest");
        Logger log3 = Log.logger("test");

        Assert.assertEquals(log1, log2);
        Assert.assertNotEquals(log1, log3);

        log1.info("Info: testLog({})", LogTest.class);
        log2.info("Info: testLog({})", "org.apache.hugegraph.unit.util.LogTest");
        log3.info("Info: testLog({})", "test");
    }
}
