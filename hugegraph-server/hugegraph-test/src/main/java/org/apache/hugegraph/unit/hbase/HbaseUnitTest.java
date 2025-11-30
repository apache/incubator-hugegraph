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

import org.apache.hadoop.hbase.client.Result;
import org.apache.hugegraph.backend.store.BackendEntry.BackendIterator;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.backend.store.hbase.HbaseSessions;
import org.apache.hugegraph.util.StringEncoding;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HbaseUnitTest extends BaseHbaseUnitTest {

    @Before
    public void setUp() throws IOException {
        super.setup();
    }

    @Test
    public void testHbaseMetaVersionAfterTruncate() {
        BackendStore systemStore = this.provider.loadSystemStore(config);

        // Record system version before truncation
        String beforeVersion = systemStore.storedVersion();

        HbaseSessions.Session testsession = this.sessions.session();
        
        // Insert test data
        testsession.put("g_v", "f".getBytes(), "row_trunc_v".getBytes(), StringEncoding.encode("q"), StringEncoding.encode("v"));
        testsession.put("g_oe", "f".getBytes(), "row_trunc_oe".getBytes(), StringEncoding.encode("q"), StringEncoding.encode("v"));
        testsession.put("g_ie", "f".getBytes(), "row_trunc_ie".getBytes(), StringEncoding.encode("q"), StringEncoding.encode("v"));
        testsession.commit();

        // Verify data insertion success
        BackendIterator<Result> vIterator = testsession.get("g_v", "f".getBytes(), "row_trunc_v".getBytes());
        BackendIterator<Result> oeIterator = testsession.get("g_oe", "f".getBytes(), "row_trunc_oe".getBytes());
        BackendIterator<Result> ieIterator = testsession.get("g_ie", "f".getBytes(), "row_trunc_ie".getBytes());
        
        Assert.assertTrue("data should exist", vIterator.hasNext());
        Assert.assertTrue("data should exist", oeIterator.hasNext());
        Assert.assertTrue("data should exist", ieIterator.hasNext());
        
        vIterator.close();
        oeIterator.close();
        ieIterator.close();

        // Execute truncate operation, clears all graph data but preserves system tables
        this.provider.truncate();

        // Verify system version remains unchanged after truncation
        String afterVersion = systemStore.storedVersion();
        Assert.assertNotNull("System metadata version should exist",afterVersion);
        Assert.assertEquals("System metadata version should remain unchanged after truncation", beforeVersion, afterVersion);

        // Verify data has been cleared
        vIterator = testsession.get("g_v", "f".getBytes(), "row_trunc_v".getBytes());
        oeIterator = testsession.get("g_oe", "f".getBytes(), "row_trunc_oe".getBytes());
        ieIterator = testsession.get("g_ie", "f".getBytes(), "row_trunc_ie".getBytes());
        
        Assert.assertFalse("data should not exist", vIterator.hasNext());
        Assert.assertFalse("data should not exist", oeIterator.hasNext());
        Assert.assertFalse("data should not exist", ieIterator.hasNext());

        vIterator.close();
        oeIterator.close();
        ieIterator.close();
    }
}
