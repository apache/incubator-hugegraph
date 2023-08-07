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

package core;

import java.util.Map;

import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.metric.HgMetricService;
import org.apache.hugegraph.store.metric.SystemMetricService;
import org.junit.Before;
import org.junit.Test;

public class MetricServiceTest {

    private SystemMetricService service;
    private HgMetricService hgService;

    @Before
    public void setUp() {
        service = new SystemMetricService();
        HgStoreEngine instance = HgStoreEngine.getInstance();
        service.setStoreEngine(instance);
        hgService = HgMetricService.getInstance().setHgStoreEngine(instance);
    }

    @Test
    public void testGetStorageEngine() {
        HgStoreEngine result = service.getStorageEngine();
    }

    @Test
    public void testGetSystemMetrics() {
        try {
            Map<String, Long> systemMetrics = service.getSystemMetrics();
            Thread.sleep(1000);
            systemMetrics = service.getSystemMetrics();
        } catch (Exception e) {

        }
    }

    @Test
    public void testGetHgMetrics() {
        // Setup
        Metapb.StoreStats.Builder systemMetrics = hgService.getMetrics();
    }
}
