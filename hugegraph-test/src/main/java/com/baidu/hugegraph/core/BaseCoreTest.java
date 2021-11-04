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

package com.baidu.hugegraph.core;

import com.baidu.hugegraph.auth.AuthManager;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.store.BackendFeatures;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.util.Log;

public class BaseCoreTest {

    protected static final Logger LOG = Log.logger(BaseCoreTest.class);

    protected static final int TX_BATCH = 100;

    public static final String DEFAULT_GRAPH_SPACE = "DEFAULT";

    public HugeGraph graph() {
        return CoreTestSuite.graph();
    }

    public AuthManager authManager() {
        return CoreTestSuite.authManager();
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

    protected BackendFeatures storeFeatures() {
        return graph().backendStoreFeatures();
    }

    protected HugeGraphParams params() {
        return Whitebox.getInternalState(graph(), "params");
    }
}
