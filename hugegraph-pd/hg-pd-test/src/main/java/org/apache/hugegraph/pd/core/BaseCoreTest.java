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

package org.apache.hugegraph.pd.core;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.pd.ConfigService;
import org.apache.hugegraph.pd.config.PDConfig;
import org.junit.After;
import org.junit.BeforeClass;


public class BaseCoreTest {

    static org.apache.hugegraph.pd.config.PDConfig pdConfig;

    @BeforeClass
    public static void init() throws Exception {
        String path = "tmp/unitTest";
        deleteDirectory(new File(path));
        pdConfig = new org.apache.hugegraph.pd.config.PDConfig() {{
            this.setClusterId(100);
            this.setInitialStoreList("127.0.0.1:8500,127.0.0.1:8501,127.0.0.1:8502," +
                                     "127.0.0.1:8503,127.0.0.1:8504,127.0.0.1:8505");
        }};

        pdConfig.setStore(new org.apache.hugegraph.pd.config.PDConfig().new Store() {{
            this.setMaxDownTime(3600);
            this.setKeepAliveTimeout(3600);
        }});

        pdConfig.setPartition(new org.apache.hugegraph.pd.config.PDConfig().new Partition() {{
            this.setShardCount(3);
            this.setMaxShardsPerStore(3);
        }});
        pdConfig.setRaft(new org.apache.hugegraph.pd.config.PDConfig().new Raft() {{
            this.setEnable(false);
        }});
        pdConfig.setDiscovery(new PDConfig().new Discovery());
        pdConfig.setDataPath(path);
        ConfigService configService = new ConfigService(pdConfig);
        pdConfig = configService.loadConfig();
    }

    public static void deleteDirectory(File dir) {
        try {
            FileUtils.deleteDirectory(dir);
        } catch (IOException e) {
            System.out.printf("Failed to start ....,%s%n", e.getMessage());
        }
    }

    @After
    public void teardown() throws Exception {
        // pass
    }
}