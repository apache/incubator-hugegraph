/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.hugegraph.unit.hbase;

import org.apache.hugegraph.testutil.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HbaseUnitTest extends BaseHbaseUnitTest {

    private static final String GRAPH_NAME = "test_graph";

    @Before
    public void setUp(){
        this.provider.open(GRAPH_NAME);
        this.provider.init();
        this.store.init();
    }

    @After
    public void teardown(){
        if (this.store != null) {
            this.store.close();
        }
        if (this.provider != null) {
            this.provider.close();
        }
    }

    @Test
    public void testHbaseMetaVersion(){
        String beforeVersion = this.store.storedVersion();
        this.store.truncate();
        String afterInitVersion = this.store.storedVersion();
        Assert.assertNotNull(beforeVersion);
        Assert.assertNotNull(afterInitVersion);
        Assert.assertEquals(beforeVersion, afterInitVersion);
    }
}
