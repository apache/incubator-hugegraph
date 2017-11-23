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

import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.store.BackendFeatures;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.util.Log;

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
        }
    }

    public HugeGraph graph() {
        return graph;
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

        // Clear edge
        graph().traversal().E().toStream().forEach(e -> {
            e.remove();
        });

        // Clear vertex
        graph().traversal().V().toStream().forEach(v -> {
            v.remove();
        });

        // Commit changes
        graph.tx().commit();
    }

    private void clearSchema() {
        SchemaManager schema = graph().schema();

        schema.getIndexLabels().stream().forEach(elem -> {
            schema.indexLabel(elem.name()).remove();
        });

        schema.getEdgeLabels().stream().forEach(elem -> {
            schema.edgeLabel(elem.name()).remove();
        });

        schema.getVertexLabels().stream().forEach(elem -> {
            schema.vertexLabel(elem.name()).remove();
        });

        schema.getPropertyKeys().stream().forEach(elem -> {
            schema.propertyKey(elem.name()).remove();
        });
    }

    protected BackendFeatures storeFeatures() {
        return graph().graphTransaction().store().features();
    }
}
