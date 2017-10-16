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
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.cache.Cache;
import com.baidu.hugegraph.backend.cache.CacheManager;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.event.EventHub;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.util.Log;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

public class PerfExample1 {

    public static final int PERSON_NUM = 70;
    public static final int SOFTWARE_NUM = 30;
    public static final int EDGE_NUM = 100;

    private static final Logger LOG = Log.logger(PerfExample1.class);

    public static void main(String[] args) throws InterruptedException {
        if (args.length != 3) {
            System.out.println("Usage: threadCount times multiple");
            return;
        }

        int threadCount = Integer.parseInt(args[0]);
        int times = Integer.parseInt(args[1]);
        int multiple = Integer.parseInt(args[2]);

        // NOTE: this test with HugeGraph is for local, change it into
        // client if test with restful server from remote
        HugeGraph hugegraph = ExampleUtil.loadGraph();
        GraphManager graph = new GraphManager(hugegraph);

        initSchema(hugegraph.schema());
        testInsertPerf(graph, threadCount, times, multiple);

        hugegraph.close();

        // Stop event hub before main thread exits
        EventHub.destroy(30);
    }

    /**
     * Multi-threaded and multi-commits and batch insertion test
     * @param graph
     * @param threadCount
     *        The count of threads that perform the insert operation at the
     *        same time
     * @param times
     *        The transaction commit times for each thread
     * @param multiple
     *        The coefficient to multiple number of vertices(100) and edges(100)
     *        for each transaction commit
     * @throws InterruptedException
     */
    public static void testInsertPerf(GraphManager graph,
                                      int threadCount,
                                      int times,
                                      int multiple)
                                      throws InterruptedException {

        List<Thread> threads = new ArrayList<>(threadCount);
        for (int i = 0; i < threadCount; i++) {
            Thread t = new Thread(() -> {
                graph.tx().open();
                testInsertPerf(graph, times, multiple);
                graph.tx().close();
                graph.close();
            });
            threads.add(t);
        }

        long beginTime = System.currentTimeMillis();

        for (Thread t : threads) {
            t.start();
        }

        for (Thread t : threads) {
            t.join();
        }

        long endTime = System.currentTimeMillis();

        // Total edges
        long edges = EDGE_NUM * threadCount * times * multiple;
        long cost = endTime - beginTime;

        LOG.info("Total edges: {}, cost times: {}", edges, cost);
        LOG.info("Rate with threads: {} edges/s", edges * 1000 / cost);
    }

    public static void initSchema(SchemaManager schema) {
        schema.propertyKey("name").asText().create();
        schema.propertyKey("age").asInt().create();
        schema.propertyKey("lang").asText().create();
        schema.propertyKey("date").asText().create();
        schema.propertyKey("price").asInt().create();

        schema.vertexLabel("person")
                .properties("name", "age")
                .primaryKeys("name")
                .ifNotExist()
                .create();

        schema.vertexLabel("software")
                .properties("name", "lang", "price")
                .primaryKeys("name")
                .ifNotExist()
                .create();

        schema.edgeLabel("knows")
                .sourceLabel("person").targetLabel("person")
                .properties("date")
                .nullableKeys("date")
                .ifNotExist()
                .create();

        schema.edgeLabel("created")
                .sourceLabel("person").targetLabel("software")
                .properties("date")
                .nullableKeys("date")
                .ifNotExist()
                .create();
    }

    public static void testInsertPerf(GraphManager graph, int times, int multiple) {
        List<Object> personIds = new ArrayList<>(PERSON_NUM * multiple);
        List<Object> softwareIds = new ArrayList<>(SOFTWARE_NUM * multiple);

        for (int time = 0; time < times; time++) {
            LOG.debug("============== random person vertex ===============");
            for (int i = 0; i < PERSON_NUM * multiple; i++) {
                Random random = new Random();
                int age = random.nextInt(70);
                String name = "P" + random.nextInt();
                Vertex vetex = graph.addVertex(T.label, "person",
                                               "name", name, "age", age);
                personIds.add(vetex.id());
                LOG.debug("Add person: {}", vetex);
            }

            LOG.debug("============== random software vertex ============");
            for (int i = 0; i < SOFTWARE_NUM * multiple; i++) {
                Random random = new Random();
                int price = random.nextInt(10000) + 1;
                String name = "S" + random.nextInt();
                Vertex vetex = graph.addVertex(T.label, "software",
                                               "name", name, "lang", "java",
                                               "price", price);
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
            personIds.clear();
            softwareIds.clear();
        }
    }

    static class GraphManager {
        private HugeGraph hugegraph;
        private Cache cache = CacheManager.instance().cache("perf-test");

        public GraphManager(HugeGraph hugegraph) {
            this.hugegraph = hugegraph;
        }

        public Transaction tx() {
            return this.hugegraph.tx();
        }

        public void close() {
            this.hugegraph.close();
        }

        public Vertex addVertex(Object... keyValues) {
            HugeVertex v = (HugeVertex) this.hugegraph.addVertex(keyValues);
            this.cache.update(v.id(), v.resetTx());
            return v;
        }

        public Vertex getVertex(Object id) {
            return ((Vertex) this.cache.getOrFetch((Id) id, k -> {
                return this.hugegraph.vertices(k).next();
            }));
        }
    }
}
