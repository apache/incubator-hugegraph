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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.cache.Cache;
import com.baidu.hugegraph.backend.cache.CacheManager;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.perf.PerfUtil;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.Log;

public abstract class PerfExampleBase {

    public static final int PERSON_NUM = 70;
    public static final int SOFTWARE_NUM = 30;
    public static final int EDGE_NUM = 100;

    protected static final Logger LOG = Log.logger(PerfExampleBase.class);

    protected Set<Object> vertices = new HashSet<>();

    public int test(String[] args) throws InterruptedException {
        if (args.length != 3) {
            System.out.println("Usage: threadCount times multiple");
            return -1;
        }

        int threadCount = Integer.parseInt(args[0]);
        int times = Integer.parseInt(args[1]);
        int multiple = Integer.parseInt(args[2]);

        // NOTE: this test with HugeGraph is for local, change it into
        // client if test with restful server from remote
        HugeGraph hugegraph = ExampleUtil.loadGraph(true);
        GraphManager graph = new GraphManager(hugegraph);

        initSchema(hugegraph.schema());

        testInsertPerf(graph, threadCount, times, multiple);

        testQueryVertexPerf(graph, threadCount, times, multiple);
        testQueryEdgePerf(graph, threadCount, times, multiple);

        hugegraph.close();

        return 0;
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
    public void testInsertPerf(GraphManager graph,
                               int threadCount,
                               int times,
                               int multiple)
                               throws InterruptedException {
        // Total edges
        long edges = EDGE_NUM * threadCount * times * multiple;
        long cost = this.execute(i -> {
            this.testInsert(graph, times, multiple);
            graph.close();
        }, threadCount);

        LOG.info("Insert rate with threads: {} edges/s, " +
                 "insert total edges: {}, cost time: {}ms",
                 edges * 1000 / cost, edges, cost);
    }

    public void testQueryVertexPerf(GraphManager graph,
                                    int threadCount,
                                    int times,
                                    int multiple)
                                    throws InterruptedException {
        long cost = this.execute(i -> {
            this.testQueryVertex(graph, threadCount, i, multiple);
            graph.close();
        }, threadCount);

        final int size = (PERSON_NUM + SOFTWARE_NUM) * threadCount * times;
        LOG.info("Query rate with threads: {} vertices/s, " +
                 "query total vertices {}, cost time: {}ms",
                 size * 1000 / cost, size, cost);
    }

    public void testQueryEdgePerf(GraphManager graph,
                                  int threadCount,
                                  int times,
                                  int multiple)
                                  throws InterruptedException {
        long cost = this.execute(i -> {
            this.testQueryEdge(graph, threadCount, i, multiple);
            graph.close();
        }, threadCount);

        final int size = (PERSON_NUM + SOFTWARE_NUM) * threadCount * times;
        LOG.info("Query rate with threads: {} vedges/s, " +
                 "query total vedges {}, cost time: {}ms",
                 size * 1000 / cost, size, cost);
    }

    protected long execute(Consumer<Integer> task, int threadCount)
                           throws InterruptedException {
        List<Thread> threads = new ArrayList<>(threadCount);
        for (int i = 0; i < threadCount; i++) {
            int j = i;
            Thread t = new Thread(() -> {
                task.accept(j);
                LOG.info("option = {}", PerfUtil.instance().toECharts());
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

        return endTime - beginTime;
    }

    protected abstract void initSchema(SchemaManager schema);

    protected abstract void testInsert(GraphManager graph,
                                       int times,
                                       int multiple);

    protected void testQueryVertex(GraphManager graph,
                                   int threads,
                                   int thread,
                                   int multiple) {
        int i = 0;
        int j = 0;
        int total = 0;
        for (Object id : this.vertices) {
            if (i++ % multiple != 0) {
                continue;
            }
            if (j++ % threads != thread) {
                continue;
            }

            LOG.debug("Qeury vertex {}: {}", i, id);
            Vertex vertex = graph.queryVertex(id);
            if (!vertex.id().equals(id)) {
                LOG.warn("Qeury vertex by id {} returned {}", id, vertex);
            }
            total++;
        }
        LOG.debug("Qeury vertices with thread({}): {}", thread, total);
    }

    protected void testQueryEdge(GraphManager graph,
                                 int threads,
                                 int thread,
                                 int multiple) {
        int i = 0;
        int j = 0;
        int totalV = 0;
        int totalE = 0;
        for (Object id : this.vertices) {
            if (i++ % multiple != 0) {
                continue;
            }
            if (j++ % threads != thread) {
                continue;
            }

            LOG.debug("Qeury vertex {}: {}", i, id);
            Iterator<Edge> edges = graph.queryVertexEdge(id, Directions.OUT);
            while (edges.hasNext()) {
                totalE++;
                LOG.debug("Edge of vertex {}: {}", i, edges.next());
            }
            totalV++;
        }
        LOG.debug("Qeury edges of vertices({}) with thread({}): {}",
                  totalV, thread, totalE);
    }

    protected static class GraphManager {
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

        public Vertex queryVertex(Object id) {
            return this.hugegraph.vertices(id).next();
        }

        public Iterator<Edge> queryVertexEdge(Object id, Directions direction) {
            ConditionQuery q = new ConditionQuery(HugeType.EDGE);
            q.eq(HugeKeys.OWNER_VERTEX, id);
            q.eq(HugeKeys.DIRECTION, direction);
            return this.hugegraph.edges(q);
        }
    }
}
