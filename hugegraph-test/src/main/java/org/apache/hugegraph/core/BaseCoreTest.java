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

import java.util.Random;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.store.BackendFeatures;
import org.apache.hugegraph.dist.RegisterUtil;
import org.apache.hugegraph.schema.SchemaManager;
import org.apache.hugegraph.testutil.Utils;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.type.define.NodeRole;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;

public class BaseCoreTest {

    protected static final Logger LOG = Log.logger(BaseCoreTest.class);

    protected static final int TX_BATCH = 100;
    private static boolean registered = false;
    private static HugeGraph graph = null;

    public static HugeGraph graph() {
        Assert.assertNotNull(graph);
        //Assert.assertFalse(graph.closed());
        return graph;
    }

    @BeforeClass
    public static void initEnv() {
        if (registered) {
            return;
        }
        RegisterUtil.registerBackends();
        registered = true;
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

    @Before
    public void setup() {
        this.clearData();
        this.clearSchema();
    }

    @After
    public void teardown() throws Exception {
        // pass
    }

    protected void clearData() {
        HugeGraph graph = graph();

        // Clear uncommitted data(maybe none)
        graph.tx().rollback();

        int count = 0;

        // Clear edge
        do {
            count = 0;
            for (Edge e : graph().traversal().E().limit(TX_BATCH).toList()) {
                count++;
                e.remove();
            }
            graph.tx().commit();
        } while (count == TX_BATCH);

        // Clear vertex
        do {
            count = 0;
            for (Vertex v : graph().traversal().V().limit(TX_BATCH).toList()) {
                count++;
                v.remove();
            }
            graph.tx().commit();
        } while (count == TX_BATCH);
    }

    private void clearSchema() {
        SchemaManager schema = graph().schema();

        schema.getIndexLabels().forEach(elem -> {
            schema.indexLabel(elem.name()).remove();
        });

        schema.getEdgeLabels().forEach(elem -> {
            schema.edgeLabel(elem.name()).remove();
        });

        schema.getVertexLabels().forEach(elem -> {
            schema.vertexLabel(elem.name()).remove();
        });

        schema.getPropertyKeys().forEach(elem -> {
            schema.propertyKey(elem.name()).remove();
        });
    }

    protected void mayCommitTx() {
        // Commit tx probabilistically for test
        if (new Random().nextBoolean()) {
            graph().tx().commit();
        }
    }

    protected void commitTx() {
        graph().tx().commit();
    }

    protected BackendFeatures storeFeatures() {
        return graph().backendStoreFeatures();
    }

    protected HugeGraphParams params() {
        return Whitebox.getInternalState(graph(), "params");
    }
}
