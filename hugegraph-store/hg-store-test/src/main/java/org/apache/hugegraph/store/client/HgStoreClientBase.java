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

package org.apache.hugegraph.store.client;

import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.store.HgStoreClient;
import org.apache.hugegraph.store.HgStoreSession;
import org.apache.hugegraph.store.UnitTestBase;
import org.junit.After;
import org.junit.Before;

public class HgStoreClientBase {

    protected static String GRAPH_NAME = "testGraphName";
    protected static String TABLE_NAME = UnitTestBase.DEFAULT_TEST_TABLE;
    private static final String PD_ADDRESS = "127.0.0.1:8686";
    protected HgStoreClient storeClient;
    protected PDClient pdClient;

    @Before
    public void setup() throws Exception {
        storeClient = HgStoreClient.create(PDConfig.of(PD_ADDRESS)
                                                   .setEnableCache(true));
        pdClient = storeClient.getPdClient();

        HgStoreSession session = storeClient.openSession(TABLE_NAME);
        session.dropTable(TABLE_NAME);
        session.truncate();
    }

    @After
    public void teardown() {
        // pass
    }
}
