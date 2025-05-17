<<<<<<<< HEAD:hugegraph-pd/hg-pd-test/src/main/java/org/apache/hugegraph/pd/core/ConfigServiceTest.java
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

package org.apache.hugegraph.pd.core;

import java.util.List;

import org.apache.hugegraph.pd.ConfigService;
import org.apache.hugegraph.pd.IdService;
========
package org.apache.hugegraph.pd.service;

import org.apache.hugegraph.pd.ConfigService;
import org.apache.hugegraph.pd.IdService;
import org.apache.hugegraph.pd.common.PDException;
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-test/src/main/java/org/apache/hugegraph/pd/service/ConfigServiceTest.java
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

<<<<<<<< HEAD:hugegraph-pd/hg-pd-test/src/main/java/org/apache/hugegraph/pd/core/ConfigServiceTest.java
public class ConfigServiceTest extends PDCoreTestBase {
========
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ConfigServiceTest {

    private PDConfig config = BaseServerTest.getConfig();
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-test/src/main/java/org/apache/hugegraph/pd/service/ConfigServiceTest.java

    private ConfigService service;

    @Before
    public void setUp() {
        this.service = new ConfigService(getPdConfig());
    }

    @Test
    public void testGetPDConfig() {
        // Setup
        try {
            final Metapb.PDConfig config = Metapb.PDConfig.newBuilder()
                                                          .setVersion(0L)
                                                          .setShardCount(55)
                                                          .setMaxShardsPerStore(0)
                                                          .setTimestamp(0L).build();
            this.service.setPDConfig(config);

            // Run the test
            Metapb.PDConfig result = this.service.getPDConfig(0L);

            // Verify the results
            Assert.assertEquals(55, result.getShardCount());
            result = this.service.getPDConfig();
            Assert.assertEquals(55, result.getShardCount());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetGraphSpace() throws Exception {
        // Setup
        Metapb.GraphSpace space = Metapb.GraphSpace.newBuilder()
                                                   .setName("gs1")
                                                   .setTimestamp(0L).build();
        this.service.setGraphSpace(space);

        // Run the test
        final List<Metapb.GraphSpace> result = this.service.getGraphSpace("gs1");

        // Verify the results
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(space.getName(), result.get(0).getName());
    }

    @Test
    public void testUpdatePDConfig() {
        try {
            final Metapb.PDConfig mConfig = Metapb.PDConfig.newBuilder()
                                                           .setVersion(0L)
                                                           .setShardCount(0)
                                                           .setMaxShardsPerStore(0)
                                                           .setTimestamp(0L)
                                                           .build();
            final PDConfig expectedResult = new PDConfig();
            expectedResult.setConfigService(new ConfigService(new PDConfig()));
            expectedResult.setIdService(new IdService(new PDConfig()));
            expectedResult.setClusterId(0L);
            expectedResult.setPatrolInterval(0L);
            expectedResult.setDataPath("dataPath");
            expectedResult.setMinStoreCount(0);
            expectedResult.setInitialStoreList("initialStoreList");
            expectedResult.setHost("host");
            expectedResult.setVerifyPath("verifyPath");
            expectedResult.setLicensePath("licensePath");
            this.service.updatePDConfig(mConfig);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testStoreGroup() throws PDException {
        config.setInitialStoreList("192.168.1.1:8500,192.168.1.1:8501,192.168.1.2:8500/1");
        service.loadConfig();

        assertEquals(2, service.getAllStoreGroup().size());
        var group1 = service.getStoreGroup(0);
        assertEquals(24, group1.getPartitionCount());

        var group2 = service.getStoreGroup(1);
        assertEquals(12, group2.getPartitionCount());

        service.updateStoreGroup(0, "DEFAULT");
        service.setPartitionCount(0, 36);

        group1 = service.getStoreGroup(0);
        assertEquals(36, group1.getPartitionCount());
        assertEquals("DEFAULT", group1.getName());

        service.createStoreGroup(2, "group2", 12);
        assertEquals(3, service.getAllStoreGroup().size());
    }
}
