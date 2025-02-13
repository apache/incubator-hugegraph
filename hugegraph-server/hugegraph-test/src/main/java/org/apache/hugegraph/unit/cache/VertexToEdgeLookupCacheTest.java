package org.apache.hugegraph.unit.cache;
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


import org.apache.hugegraph.backend.cache.Cache;
import org.apache.hugegraph.backend.cache.CachedBackendStore;
import org.apache.hugegraph.backend.cache.RamCache;
import org.apache.hugegraph.backend.cache.VertexToEdgeLookupCache;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.tx.GraphTransaction;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.define.Directions;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class VertexToEdgeLookupCacheTest {

    private static final Logger LOG = LoggerFactory.getLogger(VertexToEdgeLookupCacheTest.class);

    private VertexToEdgeLookupCache index;
    private Cache<Id, Object> edgesCache;

    @Before
    public void setup() {
        edgesCache = new RamCache(10_000_000); // 10 million capacity
        index = new VertexToEdgeLookupCache(edgesCache,10_000_000);

    }

    /**
     * Performance test for cleanupInvalidReferences method.
     * Tests with varying vertex counts and query counts per vertex.
     */
    @Test
    public void testCleanupInvalidReferencesPerformance() throws InterruptedException {
        for (int i = 1; i <= 4; i++) {
            int base = 1_000_000; // 1 million
            testCleanupInvalidReferencesPerformance(base * i, 2,
                                                    16, base * (i + 1));
        }
    }

    /**
     * Helper method to test cleanupInvalidReferences performance.
     *
     * @param vertexCount        Number of vertices to create.
     * @param queryCountPerVertex Number of queries per vertex.
     * @param threadCount        Number of threads to use.
     * @param stopVertexCount    Vertex count at which to stop.
     */
    private void testCleanupInvalidReferencesPerformance(int vertexCount, int queryCountPerVertex,
                                                         int threadCount, int stopVertexCount) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AtomicInteger verticesWritten = new AtomicInteger(0);
        AtomicInteger queriesWritten = new AtomicInteger(0);

        // Submit tasks
        for (int i = 0; i < threadCount - 1; i++) {
            executor.submit(() -> prepareTestDataConcurrently(vertexCount, queryCountPerVertex, verticesWritten, queriesWritten));
        }
        executor.submit(() -> prepareTestDataConcurrentlyWithPause(stopVertexCount, queryCountPerVertex, verticesWritten, queriesWritten));

        // Wait until all vertices are written
        while (verticesWritten.get() < vertexCount) {
            Thread.sleep(1000);
        }

        if (verticesWritten.get() >= vertexCount) {
            // Invalidate half of the queries
            invalidateHalfQueries(stopVertexCount, queryCountPerVertex);

            // Measure tick performance
            long startTime = System.currentTimeMillis();
            index.tick();
            long cost = System.currentTimeMillis() - startTime;
            LOG.info("VertexEdgeQueryIndex tick cost {}ms", cost);
            Assert.assertFalse("Tick cost exceeded 2000ms", cost > 2000);
        }

        // Cleanup
        verticesWritten.set(stopVertexCount);
        index.clear();
        edgesCache.clear();

        // Shutdown executor
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    /**
     * Invalidates half of the queries for the given vertex count.
     *
     * @param vertexCount        Number of vertices.
     * @param queryCountPerVertex Number of queries per vertex.
     */
    private void invalidateHalfQueries(int vertexCount, int queryCountPerVertex) {
        new Thread(() -> {
            for (int i = 0; i < vertexCount; i++) {
                for (int j = 0; j < queryCountPerVertex; j++) {
                    if (i % 2 == 0) {
                        ConditionQuery conditionQuery = GraphTransaction.constructEdgesQuery(
                                IdGenerator.of(i * queryCountPerVertex + j), Directions.BOTH, new Id[0]);
                        CachedBackendStore.QueryId queryId = new CachedBackendStore.QueryId(conditionQuery);
                        edgesCache.invalidate(queryId);
                    }
                }
            }
            System.gc(); // Suggest GC to clean up invalid references
            LOG.info("invalidateHalfQueries completed.");
        }).start();
    }

    /**
     * Prepares test data concurrently.
     */
    private void prepareTestDataConcurrently(int vertexCount, int queryCountPerVertex,
                                             AtomicInteger verticesWritten, AtomicInteger queriesWritten) {
        while (verticesWritten.get() < vertexCount) {
            int vertexId = verticesWritten.getAndIncrement();
            if (vertexId >= vertexCount) {
                break; // Ensure we don't exceed the vertex count
            }

            Id vertex = IdGenerator.of(vertexId);
            for (int j = 0; j < queryCountPerVertex; j++) {
                ConditionQuery conditionQuery = GraphTransaction.constructEdgesQuery(
                        IdGenerator.of(vertexId * queryCountPerVertex + j), Directions.BOTH, new Id[0]);
                CachedBackendStore.QueryId queryId = new CachedBackendStore.QueryId(conditionQuery);
                edgesCache.update(queryId, new Object());
                index.update(vertex, queryId);
                queriesWritten.incrementAndGet();
            }
        }
    }

    /**
     * Prepares test data concurrently with a delay.
     */
    private void prepareTestDataConcurrentlyWithPause(int vertexCount, int queryCountPerVertex,
                                                      AtomicInteger verticesWritten, AtomicInteger queriesWritten) {
        while (verticesWritten.get() < vertexCount) {
            int vertexId = verticesWritten.getAndIncrement();
            if (vertexId >= vertexCount) {
                break; // Ensure we don't exceed the vertex count
            }

            Id vertex = IdGenerator.of(vertexId);
            for (int j = 0; j < queryCountPerVertex; j++) {
                ConditionQuery conditionQuery = GraphTransaction.constructEdgesQuery(
                        IdGenerator.of(vertexId * queryCountPerVertex + j), Directions.BOTH, new Id[0]);
                CachedBackendStore.QueryId queryId = new CachedBackendStore.QueryId(conditionQuery);
                edgesCache.update(queryId, new Object());
                index.update(vertex, queryId);
                queriesWritten.incrementAndGet();
            }

            try {
                TimeUnit.MILLISECONDS.sleep(500); // Simulate write delay
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Thread interrupted during sleep", e);
            }
        }
    }

    /**
     * Tests invalidate and verify logic.
     */
    @Test
    public void testInvalidateAndVerify() throws InterruptedException {
        int threadCount = 10;
        int vertexCount = 5000;
        int queryCountPerVertex = 2;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AtomicInteger verticesWritten = new AtomicInteger(0);
        AtomicInteger queriesWritten = new AtomicInteger(0);

        // Submit tasks
        for (int i = 0; i < threadCount - 1; i++) {
            executor.submit(() -> prepareTestDataConcurrentlyWithPause(vertexCount, queryCountPerVertex, verticesWritten, queriesWritten));
        }

        // Schedule verifyIndexAccuracy task
        ScheduledExecutorService verifyScheduler = Executors.newScheduledThreadPool(1);
        verifyScheduler.scheduleAtFixedRate(() -> {
            try {
                for (int i = 0; i < 10; i++) {
                    List<Id> edgeKeys = new ArrayList<>();
                    edgesCache.traverseKeys(edgeKeys::add);
                    if (!edgeKeys.isEmpty()) {
                        Id randomEdgeId = edgeKeys.get(ThreadLocalRandom.current().nextInt(edgeKeys.size()));
                        edgesCache.invalidate(randomEdgeId);
                    }

                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

                verifyIndexAccuracy();
                System.gc();
            } catch (Exception e) {
                LOG.error("Error during verifyIndexAccuracy", e);
            }
        }, 10, 10, TimeUnit.SECONDS);

        // Shutdown executor
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
    }

    /**
     * Verifies the accuracy of the index.
     */
    private void verifyIndexAccuracy() {
        edgesCache.traverseKeys(queryId -> {
            boolean foundInIndex = false;

            Iterator<Id> vertexIterator = index.snapshotKeys().iterator();
            while (vertexIterator.hasNext() && !foundInIndex) {
                Id vertexId = vertexIterator.next();
                try {
                    Set<Id> queryRefs = (Set<Id>) index.get(vertexId);
                    if (queryRefs != null && queryRefs.contains(queryId)) {
                        foundInIndex = true;
                        break;
                    }
                } catch (Exception e) {
                    LOG.error("Error verifying queryId: {}", queryId, e);
                }
            }

            Assert.assertFalse("QueryId " + queryId + " not found in index", !foundInIndex);
        });
    }
}
