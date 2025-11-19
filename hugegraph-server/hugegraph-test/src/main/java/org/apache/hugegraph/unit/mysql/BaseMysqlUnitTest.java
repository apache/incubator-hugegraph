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

package org.apache.hugegraph.unit.mysql;

import org.apache.commons.configuration2.Configuration;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.store.hbase.HbaseStoreProvider;
import org.apache.hugegraph.backend.store.mysql.MysqlStoreProvider;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.testutil.Utils;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.apache.hugegraph.unit.FakeObjects;
import org.junit.After;
import org.junit.Before;

public class BaseMysqlUnitTest extends BaseUnitTest {

    private static final String GRAPH_NAME = "test_graph";

    protected HugeConfig config;
    protected MysqlStoreProvider provider;

    public void setup() {
        try {
            Configuration conf = Utils.getConf();
            this.config = new HugeConfig(conf);
        } catch (Exception e) {
            this.config = FakeObjects.newConfig();
        }
        this.provider = new MysqlStoreProvider();
        this.provider.open(GRAPH_NAME);
        this.provider.loadSystemStore(config).open(config);
        this.provider.loadGraphStore(config).open(config);
        this.provider.loadSchemaStore(config).open(config);
        this.provider.init();
    }

    @After
    public void down(){
        if (this.provider != null) {
            try {
                this.provider.close();
            } catch (Exception e) {
                // pass
            }
        }
    }
}
