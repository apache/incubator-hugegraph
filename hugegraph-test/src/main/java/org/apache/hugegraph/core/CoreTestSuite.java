/*
 * Copyright 2017 HugeGraph Authors
 *
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

package org.apache.hugegraph.core;

import org.apache.hugegraph.testutil.Utils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.slf4j.Logger;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.core.PropertyCoreTest.EdgePropertyCoreTest;
import org.apache.hugegraph.core.PropertyCoreTest.VertexPropertyCoreTest;
import org.apache.hugegraph.dist.RegisterUtil;
import org.apache.hugegraph.type.define.NodeRole;
import org.apache.hugegraph.util.Log;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    PropertyKeyCoreTest.class,
    VertexLabelCoreTest.class,
    EdgeLabelCoreTest.class,
    IndexLabelCoreTest.class,
    VertexCoreTest.class,
    EdgeCoreTest.class,
    VertexPropertyCoreTest.class,
    EdgePropertyCoreTest.class,
    RestoreCoreTest.class,
    TaskCoreTest.class,
    AuthTest.class,
    MultiGraphsTest.class,
    RamTableTest.class,
    RoleElectionStateMachineTest.class
})
public class CoreTestSuite {

    private static final Logger LOG = Log.logger(CoreTestSuite.class);

    private static HugeGraph graph = null;

    @BeforeClass
    public static void initEnv() {
        RegisterUtil.registerBackends();
    }

    @BeforeClass
    public static void init() {
        graph = Utils.open();
        graph.clearBackend();
        graph.initBackend();
        graph.serverStarted(IdGenerator.of("server1"), NodeRole.MASTER);
    }

    @AfterClass
    public static void clear() {
        if (graph == null) {
            return;
        }

        try {
            graph.clearBackend();
        } finally {
            try {
                graph.close();
            } catch (Throwable e) {
                LOG.error("Error when close()", e);
            }
            graph = null;
        }
    }

    protected static HugeGraph graph() {
        Assert.assertNotNull(graph);
        //Assert.assertFalse(graph.closed());
        return graph;
    }
}
