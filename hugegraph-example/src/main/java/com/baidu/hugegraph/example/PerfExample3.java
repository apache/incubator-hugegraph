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

package com.baidu.hugegraph.example;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.baidu.hugegraph.event.EventHub;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.util.Log;

/**
 * Perf test for: insert vertices in order
 */
public class PerfExample3 extends PerfExampleBase {

    private static final Logger LOG = Log.logger(PerfExample3.class);

    public static void main(String[] args) throws InterruptedException {
        PerfExample3 tester = new PerfExample3();
        tester.test(args);

        // Stop event hub before main thread exits
        EventHub.destroy(30);
    }

    @Override
    protected void initSchema(SchemaManager schema) {
        // Schema changes will be commit directly into the back-end
        LOG.info("===============  propertyKey  ================");
        schema.propertyKey("id").asInt().create();
        schema.propertyKey("name").asText().create();
        schema.propertyKey("age").asInt().valueSingle().create();
        schema.propertyKey("city").asText().create();

        LOG.info("===============  vertexLabel  ================");

        schema.vertexLabel("person")
              .properties("name", "age", "city")
              .primaryKeys("name")
              .create();

        LOG.info("===============  vertexLabel & index  ================");

        schema.indexLabel("personByCity")
              .onV("person").secondary().by("city").create();
        schema.indexLabel("personByAge")
              .onV("person").range().by("age").create();

        LOG.info("===============  edgeLabel  ================");

        schema.edgeLabel("knows")
              .sourceLabel("person").targetLabel("person")
              .ifNotExist().create();
    }

    @Override
    protected void testInsert(GraphManager graph, int times, int multiple) {
        final int TIMES = times * multiple;
        final int BATCH = 100;
        long total = 0;
        // Insert in order
        for (int i = 0; i < TIMES; i++) {
            for (int j = 0; j < BATCH; j++) {
                String name = String.format("p-%08d", total++);
                Vertex v = graph.addVertex(T.label, "person", "name", name,
                                           "city", "Hongkong", "age", 3);
                this.vertices.add(v.id());
            }
            graph.tx().commit();
        }
    }

    protected void testAppend(GraphManager graph) {
        Vertex v1 = graph.addVertex(T.label, "person",
                                    "name", String.format("p-%08d", 1),
                                    "city", "Hongkong", "age", 18);
        Vertex v2 = graph.addVertex(T.label, "person",
                                    "name", String.format("p-%08d", 10000002),
                                    "city", "Hongkong", "age", 20);
        v1.addEdge("knows", v2);

        graph.tx().commit();
    }
}
