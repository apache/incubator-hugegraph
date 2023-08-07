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

package org.apache.hugegraph.pd.client;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.hugegraph.pd.client.DiscoveryClientImpl;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfo;
import org.apache.hugegraph.pd.grpc.discovery.Query;
import org.junit.Before;
import org.junit.Test;

public class DiscoveryClientTest {

    private DiscoveryClientImpl client;

    @Before
    public void setUp() {
        this.client = getClient("appName", "localhost:8654", new HashMap());
    }

    @Test
    public void testGetRegisterNode() {
        // Setup
        try {
            Consumer result = this.client.getRegisterConsumer();
            final NodeInfo expectedResult = NodeInfo.newBuilder()
                                                    .setAppName("appName")
                                                    .build();

            Thread.sleep(3000);
            Query query = Query.newBuilder().setAppName("appName")
                               .setVersion("0.13.0").build();

            // Run the test
            this.client.getNodeInfos(query);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            this.client.close();
        }

    }

    private DiscoveryClientImpl getClient(String appName, String address,
                                          Map labels) {
        DiscoveryClientImpl discoveryClient = null;
        try {
            discoveryClient = DiscoveryClientImpl.newBuilder().setCenterAddress(
                                                         "localhost:8686").setAddress(address).setAppName(appName)
                                                 .setDelay(2000)
                                                 .setVersion("0.13.0")
                                                 .setId("0").setLabels(labels)
                                                 .build();
            discoveryClient.scheduleTask();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return discoveryClient;
    }
}
