/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.core;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.HugeFactory;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.query.QueryBatch;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.backend.store.rocksdb.RocksDBSessions;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.iterator.MapperIterator;
import org.apache.hugegraph.query.Query;
import org.apache.hugegraph.testutil.Utils;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.traversal.algorithm.OltpTraverser;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.Consumers;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksIterator;

public class OltpTraverserCursorTest {

    @Before
    public void requireRocksDB() {
        Assume.assumeTrue("Native cursor regression requires RocksDB",
                          "rocksdb".equals(Utils.getConf().getString("backend")));
        CoreTestSuite.initEnv();
    }

    @Test
    public void testConsumerFailureClosesNativeCursor() throws Exception {
        this.checkNativeCursor("consumer");
    }

    @Test
    public void testStopExecutionClosesNativeCursor() throws Exception {
        this.checkNativeCursor("stop");
    }

    @Test
    public void testInterruptedConsumerClosesNativeCursor() throws Exception {
        this.checkNativeCursor("interrupt");
    }

    @Test
    public void testPrepareFailureClosesNativeCursor() throws Exception {
        this.checkNativeCursor("prepare");
    }

    @Test
    public void testCloseFailureIsSuppressedAfterNativeCursorCloses() throws Exception {
        this.checkNativeCursor("close");
    }

    private void checkNativeCursor(String outcome) throws Exception {
        Path directory = Files.createTempDirectory("oltp-cursor-");
        HugeConfig config = new HugeConfig(new MapConfiguration(Collections.emptyMap()));
        config.setProperty("backend", "rocksdb");
        config.setProperty("serializer", "binary");
        config.setProperty("store", "cursor_" + UUID.randomUUID().toString().replace("-", ""));
        config.setProperty("oltp.concurrent_threads", 0);
        config.setProperty("rocksdb.data_path", directory.toString());
        config.setProperty("rocksdb.wal_path", directory.toString());
        HugeGraph graph = null;
        RocksDBSessions control = null;
        Iterator<Edge> edges = null;
        RocksIterator nativeCursor = null;
        try {
            graph = HugeFactory.open(config);
            graph.initBackend();
            graph.schema().propertyKey("seq").asInt().create();
            graph.schema().vertexLabel("vertex").useCustomizeNumberId().create();
            graph.schema().edgeLabel("linked").sourceLabel("vertex").targetLabel("vertex")
                 .multiTimes().properties("seq").sortKeys("seq").create();
            Vertex source = graph.addVertex(T.label, "vertex", T.id, 1L);
            Vertex target = graph.addVertex(T.label, "vertex", T.id, 2L);
            // The backend prefetches 500 edges; record 501 keeps its native cursor open.
            for (int i = 0; i < 501; i++) {
                source.addEdge("linked", target, "seq", i);
            }
            graph.tx().commit();
            Id label = graph.schema().getEdgeLabel("linked").id();
            Query query = GraphTransaction.constructEdgesQuery(IdGenerator.of(1L), Directions.OUT,
                                                               new Id[]{label});
            edges = graph.edges(query);
            Assert.assertTrue(edges.hasNext());
            nativeCursor = nativeCursor(edges, Collections.newSetFromMap(new IdentityHashMap<>()));
            Assert.assertNotNull("Expected the real RocksDB scan cursor", nativeCursor);
            Assert.assertTrue(nativeCursor.isOwningHandle());
            Object store = graph.storeProvider().loadGraphStore(config);
            RocksDBSessions sessions = Whitebox.getInternalState(store, "sessions");
            // Keep the native DB alive during handle observation after caller FINISHED cleanup.
            control = sessions.copy(config, "cursor-control", "g");
            control.session();
            Probe traverser = new Probe(graph);
            RuntimeException failure = "stop".equals(outcome) ? new Consumers.StopExecution("limit") :
                                       new IllegalStateException("business failure");
            if ("close".equals(outcome)) {
                edges = new MapperIterator<Edge, Edge>(edges, edge -> edge) {
                    @Override
                    public void close() throws Exception {
                        super.close();
                        throw new IllegalStateException("close failure");
                    }
                };
            }
            Iterator<Edge> input = edges;
            OltpTraverser.EdgesConsumer<Edge, ?> consumer;
            if ("prepare".equals(outcome)) {
                consumer = traverser.new EdgesConsumer<Edge, Id>(id -> Assert.fail(), OltpTraverser.NO_LIMIT) {
                    @Override
                    protected Iterator<Id> prepare(Iterator<Edge> iterator) {
                        throw failure;
                    }
                };
            } else {
                consumer = traverser.new OneStepEdgeIterConsumer(id -> {
                    throw failure;
                }, OltpTraverser.NO_LIMIT);
            }
            if ("interrupt".equals(outcome)) {
                Thread.currentThread().interrupt();
                try {
                    consumer.accept(input);
                    Assert.assertTrue(Thread.currentThread().isInterrupted());
                } finally {
                    Thread.interrupted();
                }
            } else {
                RuntimeException thrown = org.apache.hugegraph.testutil.Assert.assertThrows(
                        RuntimeException.class, () -> consumer.accept(input));
                Assert.assertSame(failure, thrown);
                if ("close".equals(outcome)) {
                    Assert.assertEquals(1, thrown.getSuppressed().length);
                    Assert.assertTrue(thrown.getSuppressed()[0].getCause().getMessage().contains("close failure"));
                }
            }
            HugeFactory.closeCurrentThreadTransactions();
            Assert.assertTrue("Control lease must keep the DB alive", control.databaseOpened());
            Assert.assertFalse("Traversal retained its native cursor after " + outcome,
                               nativeCursor.isOwningHandle());
        } finally {
            // These are teardown only; the native handle assertion above must run first.
            try {
                if (nativeCursor != null && nativeCursor.isOwningHandle()) {
                    nativeCursor.close();
                }
                if (edges != null && !"close".equals(outcome)) {
                    CloseableIterator.closeIterator(edges);
                }
            } finally {
                try {
                    HugeFactory.closeCurrentThreadTransactions();
                    if (control != null) {
                        control.close();
                    }
                    if (graph != null && !graph.closed()) {
                        graph.drop();
                    }
                } finally {
                    if (graph != null) {
                        HugeFactory.remove(graph);
                    }
                    FileUtils.deleteDirectory(directory.toFile());
                }
            }
        }
    }

    private static RocksIterator nativeCursor(Object value, Set<Object> visited) throws Exception {
        if (value instanceof RocksIterator) {
            return (RocksIterator) value;
        }
        if (!(value instanceof Iterator || value instanceof QueryResults || value instanceof QueryBatch) ||
            !visited.add(value)) {
            return null;
        }
        for (Class<?> type = value.getClass(); type != null &&
             type.getName().startsWith("org.apache.hugegraph."); type = type.getSuperclass()) {
            for (Field field : type.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                field.setAccessible(true);
                RocksIterator found = nativeCursor(field.get(value), visited);
                if (found != null) {
                    return found;
                }
            }
        }
        return null;
    }

    private static class Probe extends OltpTraverser {

        Probe(HugeGraph graph) {
            super(graph);
        }
    }
}
