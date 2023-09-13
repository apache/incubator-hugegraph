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

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.traversal.algorithm.records.KneighborRecords;
import org.apache.hugegraph.traversal.algorithm.steps.EdgeStep;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.gremlin.structure.Edge;

public class KneighborTraverser extends OltpTraverser {

    public KneighborTraverser(HugeGraph graph) {
        super(graph);
    }

    public Set<Id> kneighbor(Id sourceV, Directions dir,
                             String label, int depth,
                             long degree, long limit) {
        E.checkNotNull(sourceV, "source vertex id");
        this.checkVertexExist(sourceV, "source vertex");
        E.checkNotNull(dir, "direction");
        checkPositive(depth, "k-neighbor max_depth");
        checkDegree(degree);
        checkLimit(limit);

        Id labelId = this.getEdgeLabelId(label);

        Set<Id> latest = newSet();
        Set<Id> all = newSet();

        latest.add(sourceV);
        this.vertexIterCounter.addAndGet(1L);

        while (depth-- > 0) {
            long remaining = limit == NO_LIMIT ? NO_LIMIT : limit - all.size();
            latest = this.adjacentVertices(sourceV, latest, dir, labelId,
                                           all, degree, remaining);
            all.addAll(latest);
            this.vertexIterCounter.addAndGet(1L);
            this.edgeIterCounter.addAndGet(latest.size());
            if (reachLimit(limit, all.size())) {
                break;
            }
        }

        return all;
    }

    public KneighborRecords customizedKneighbor(Id source, EdgeStep step,
                                                int maxDepth, long limit) {
        E.checkNotNull(source, "source vertex id");
        this.checkVertexExist(source, "source vertex");
        checkPositive(maxDepth, "k-neighbor max_depth");
        checkLimit(limit);

        boolean concurrent = maxDepth >= this.concurrentDepth();

        KneighborRecords records = new KneighborRecords(concurrent,
                                                        source, true);

        Consumer<Id> consumer = v -> {
            if (this.reachLimit(limit, records.size())) {
                return;
            }
            Iterator<Edge> edges = edgesOfVertex(v, step);
            this.vertexIterCounter.addAndGet(1L);
            while (!this.reachLimit(limit, records.size()) && edges.hasNext()) {
                HugeEdge edge = (HugeEdge) edges.next();
                Id target = edge.id().otherVertexId();
                records.addPath(v, target);

                records.edgeResults().addEdge(v, target, edge);

                this.edgeIterCounter.addAndGet(1L);
            }
        };

        while (maxDepth-- > 0) {
            records.startOneLayer(true);
            traverseIds(records.keys(), consumer, concurrent);
            records.finishOneLayer();
        }
        return records;
    }

    private boolean reachLimit(long limit, int size) {
        return limit != NO_LIMIT && size >= limit;
    }
}
