/*
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

package org.apache.hugegraph.traversal.algorithm;

import java.util.Iterator;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.traversal.algorithm.records.KoutRecords;
import org.apache.hugegraph.traversal.algorithm.steps.Steps;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.gremlin.structure.Edge;

public class KoutTraverser extends OltpTraverser {

    public KoutTraverser(HugeGraph graph) {
        super(graph);
    }

    public Set<Id> kout(Id sourceV, Directions dir, String label,
                        int depth, boolean nearest,
                        long degree, long capacity, long limit) {
        E.checkNotNull(sourceV, "source vertex id");
        this.checkVertexExist(sourceV, "source vertex");
        E.checkNotNull(dir, "direction");
        checkPositive(depth, "k-out max_depth");
        checkDegree(degree);
        checkCapacity(capacity);
        checkLimit(limit);
        if (capacity != NO_LIMIT) {
            // Capacity must > limit because sourceV is counted in capacity
            E.checkArgument(capacity >= limit && limit != NO_LIMIT,
                            "Capacity can't be less than limit, " +
                            "but got capacity '%s' and limit '%s'",
                            capacity, limit);
        }

        Id labelId = this.getEdgeLabelId(label);

        Set<Id> latest = newIdSet();
        latest.add(sourceV);

        Set<Id> all = newIdSet();
        all.add(sourceV);

        long remaining = capacity == NO_LIMIT ?
                         NO_LIMIT : capacity - latest.size();
        this.vertexIterCounter.addAndGet(1L);
        while (depth-- > 0) {
            // Just get limit nodes in last layer if limit < remaining capacity
            if (depth == 0 && limit != NO_LIMIT &&
                (limit < remaining || remaining == NO_LIMIT)) {
                remaining = limit;
            }
            if (nearest) {
                latest = this.adjacentVertices(sourceV, latest, dir, labelId,
                                               all, degree, remaining);
                all.addAll(latest);
            } else {
                latest = this.adjacentVertices(sourceV, latest, dir, labelId,
                                               null, degree, remaining);
            }
            this.vertexIterCounter.addAndGet(1L);
            this.edgeIterCounter.addAndGet(latest.size());
            if (capacity != NO_LIMIT) {
                // Update 'remaining' value to record remaining capacity
                remaining -= latest.size();

                if (remaining <= 0 && depth > 0) {
                    throw new HugeException(
                            "Reach capacity '%s' while remaining depth '%s'",
                            capacity, depth);
                }
            }
        }

        return latest;
    }

    public KoutRecords customizedKout(Id source, Steps steps,
                                      int maxDepth, boolean nearest,
                                      long capacity, long limit) {
        E.checkNotNull(source, "source vertex id");
        this.checkVertexExist(source, "source vertex");
        checkPositive(maxDepth, "k-out max_depth");
        checkCapacity(capacity);
        checkLimit(limit);
        long[] depth = new long[1];
        depth[0] = maxDepth;
        boolean concurrent = maxDepth >= this.concurrentDepth();

        KoutRecords records = new KoutRecords(concurrent, source, nearest, 0);

        Consumer<Id> consumer = v -> {
            if (this.reachLimit(limit, depth[0], records.size())) {
                return;
            }
            Iterator<Edge> edges = edgesOfVertex(v, steps);
            this.vertexIterCounter.addAndGet(1L);
            while (!this.reachLimit(limit, depth[0], records.size()) &&
                   edges.hasNext()) {
                HugeEdge edge = (HugeEdge) edges.next();
                Id target = edge.id().otherVertexId();
                records.addPath(v, target);
                this.checkCapacity(capacity, records.accessed(), depth[0]);

                records.edgeResults().addEdge(v, target, edge);

                this.edgeIterCounter.addAndGet(1L);
            }
        };

        while (depth[0]-- > 0) {
            records.startOneLayer(true);
            this.traverseIds(records.keys(), consumer, concurrent);
            records.finishOneLayer();
        }
        return records;
    }

    public KoutRecords dfsKout(Id source, Steps steps,
                               int maxDepth, boolean nearest,
                               long capacity, long limit) {
        E.checkNotNull(source, "source vertex id");
        this.checkVertexExist(source, "source vertex");
        checkPositive(maxDepth, "k-out max_depth");
        checkCapacity(capacity);
        checkLimit(limit);

        Set<Id> all = newIdSet();
        all.add(source);

        KoutRecords records = new KoutRecords(false, source, nearest, maxDepth);
        Iterator<Edge> iterator = this.createNestedIterator(source, steps, maxDepth, all, nearest);
        while (iterator.hasNext()) {
            HugeEdge edge = (HugeEdge) iterator.next();
            this.edgeIterCounter.addAndGet(1L);

            Id target = edge.id().otherVertexId();
            if (!nearest || !all.contains(target)) {
                records.addFullPath(HugeTraverser.pathEdges(iterator, edge));
            }

            if (limit != NO_LIMIT && records.size() >= limit ||
                capacity != NO_LIMIT && all.size() > capacity) {
                break;
            }
        }

        return records;
    }

    private void checkCapacity(long capacity, long accessed, long depth) {
        if (capacity == NO_LIMIT) {
            return;
        }
        if (accessed >= capacity && depth > 0) {
            throw new HugeException(
                    "Reach capacity '%s' while remaining depth '%s'",
                    capacity, depth);
        }
    }

    private boolean reachLimit(long limit, long depth, int size) {
        return limit != NO_LIMIT && depth <= 0 && size >= limit;
    }
}
