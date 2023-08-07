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

import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.store.HgStoreClient;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BaseClientTest {
    public static HgStoreClient storeClient;

    public static PDClient pdClient;
    public static String tableName0 = "cli-table0";
    protected static String graphName = "testGraphName";
    protected static String tableName = "testTableName";

    @BeforeClass
    public static void beforeClass() {
        storeClient = HgStoreClient.create(PDConfig.of("127.0.0.1:8686")
                                                   .setEnableCache(true));
        pdClient = storeClient.getPdClient();
    }

    @After
    public void teardown() {
        // pass
    }
}