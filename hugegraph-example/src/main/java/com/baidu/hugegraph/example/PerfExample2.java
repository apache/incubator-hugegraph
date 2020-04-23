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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.schema.SchemaManager;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

/**
 * Perf test for: vertex without properties
 */
public class PerfExample2 extends PerfExampleBase {

    public static void main(String[] args) throws Exception {
        PerfExample2 tester = new PerfExample2();
        tester.test(args);

        // Stop daemon thread
        HugeFactory.shutdown(30L);
    }

    @Override
    protected void initSchema(SchemaManager schema) {
        schema.propertyKey("name").asText().ifNotExist().create();

        schema.vertexLabel("person")
              .useAutomaticId()
              .ifNotExist()
              .create();

        schema.vertexLabel("software")
              .useAutomaticId()
              .ifNotExist()
              .create();

        schema.edgeLabel("knows")
              .sourceLabel("person")
              .targetLabel("person")
              .ifNotExist()
              .create();

        schema.edgeLabel("created")
              .sourceLabel("person")
              .targetLabel("software")
              .ifNotExist()
              .create();
    }

    @Override
    protected void testInsert(GraphManager graph, int times, int multiple) {
        List<Object> personIds = new ArrayList<>(PERSON_NUM * multiple);
        List<Object> softwareIds = new ArrayList<>(SOFTWARE_NUM * multiple);

        for (int time = 0; time < times; time++) {
            LOG.debug("============== random person vertex ===============");
            for (int i = 0; i < PERSON_NUM * multiple; i++) {
                Vertex vetex = graph.addVertex(T.label, "person");
                personIds.add(vetex.id());
                LOG.debug("Add person: {}", vetex);
            }

            LOG.debug("============== random software vertex ============");
            for (int i = 0; i < SOFTWARE_NUM * multiple; i++) {
                Vertex vetex = graph.addVertex(T.label, "software");
                softwareIds.add(vetex.id());
                LOG.debug("Add software: {}", vetex);
            }

            LOG.debug("========== random knows & created edges ==========");
            for (int i = 0; i < EDGE_NUM / 2 * multiple; i++) {
                Random random = new Random();

                // Add edge: person --knows-> person
                Object p1 = personIds.get(random.nextInt(PERSON_NUM));
                Object p2 = personIds.get(random.nextInt(PERSON_NUM));
                graph.getVertex(p1).addEdge("knows", graph.getVertex(p2));

                // Add edge: person --created-> software
                Object p3 = personIds.get(random.nextInt(PERSON_NUM));
                Object s1 = softwareIds.get(random.nextInt(SOFTWARE_NUM));
                graph.getVertex(p3).addEdge("created", graph.getVertex(s1));
            }

            try {
                graph.tx().commit();
            } catch (BackendException e) {
                if (e.getCause() instanceof NoHostAvailableException) {
                    LOG.warn("Failed to commit tx: {}", e.getMessage());
                } else {
                    throw e;
                }
            }

            this.vertices.addAll(personIds);
            this.vertices.addAll(softwareIds);

            personIds.clear();
            softwareIds.clear();
        }
    }
}
