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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import com.baidu.hugegraph.util.Log;

import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;

public class BaseCoreTest {

    protected static final Logger LOG = Log.logger(BaseCoreTest.class);

    public static String CONF_PATH = "hugegraph.properties";

    private static HugeGraph graph = null;

    protected static HugeGraph open() {
        String confFile = BaseCoreTest.class.getClassLoader()
                          .getResource(CONF_PATH).getPath();
        return HugeFactory.open(confFile);
    }

    @BeforeClass
    public static void init() {
        graph = open();

        graph.clearBackend();
        graph.initBackend();
    }

    @AfterClass
    public static void clear() throws Exception {
        graph.close();
    }

    public HugeGraph graph() {
        return graph;
    }

    @Before
    public void setup() {
        this.clearData();
    }

    @After
    public void teardown() throws Exception {
        // pass
    }

    protected void clearData() {
        HugeGraph graph = graph();

        graph.tx().open();
        try {
            // Clear edge
            graph().traversal().E().toStream().forEach(e -> {
                e.remove();
            });

            // Clear vertex
            graph().traversal().V().toStream().forEach(v -> {
                v.remove();
            });

            // Commit before clearing schema
            graph.tx().commit();

            // Clear schema
            SchemaTransaction schema = graph.schemaTransaction();

            schema.getIndexLabels().stream().forEach(elem -> {
                schema.removeIndexLabel(elem.name());
            });

            schema.getEdgeLabels().stream().forEach(elem -> {
                schema.removeEdgeLabel(elem.name());
            });

            schema.getVertexLabels().stream().forEach(elem -> {
                schema.removeVertexLabel(elem.name());
            });

            schema.getPropertyKeys().stream().forEach(elem -> {
                schema.removePropertyKey(elem.name());
            });

            graph.tx().commit();
        } finally {
            graph.tx().close();
        }
    }
}
