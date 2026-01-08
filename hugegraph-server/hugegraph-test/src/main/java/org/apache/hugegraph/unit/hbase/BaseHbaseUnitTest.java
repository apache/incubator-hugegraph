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

package org.apache.hugegraph.unit.hbase;

import org.apache.commons.configuration2.Configuration;
import org.apache.hugegraph.backend.store.hbase.HbaseSessions;
import org.apache.hugegraph.backend.store.hbase.HbaseStoreProvider;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.testutil.Utils;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

public class BaseHbaseUnitTest extends BaseUnitTest {

    private static final String GRAPH_NAME = "test_graph";

    protected HugeConfig config;
    protected HbaseStoreProvider provider;
    protected HbaseSessions sessions;

    @Before
    public void setup() throws IOException {
        Configuration conf = Utils.getConf();
        this.config = new HugeConfig(conf);
        this.provider = new HbaseStoreProvider();
        try {
            this.provider.open(GRAPH_NAME);
            this.provider.loadSystemStore(config).open(config);
            this.provider.loadGraphStore(config).open(config);
            this.provider.loadSchemaStore(config).open(config);
            // ensure back is clear
            this.provider.truncate();
            this.provider.init();
            this.sessions = new HbaseSessions(config, GRAPH_NAME, this.provider.loadGraphStore(config).store());
            this.sessions.open();
        } catch (Exception e) {
            tearDown();
            LOG.warn("Failed to init Hbasetest ", e);
        }

    }

    @After
    public void tearDown() {
        if (this.sessions != null) {
            try {
                this.sessions.close();
            } catch (Exception e) {
                LOG.warn("Failed to close sessions ", e);
            }
        }
        if (this.provider != null) {
            // ensure back is clear
            this.provider.truncate();
            try {
                this.provider.close();
            } catch (Exception e) {
                LOG.warn("Failed to close provider ", e);
            }
        }
    }
}
