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

package com.baidu.hugegraph.job.algorithm.cent;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Stack;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.job.UserJob;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser;
import com.baidu.hugegraph.type.define.Directions;


public class BetweennessCentralityAlgorithmV2 extends AbstractCentAlgorithm {

    @Override
    public String name() {
        return "betweenness_centrality";
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        super.checkParameters(parameters);
    }

    @Override
    public Object call(UserJob<Object> job, Map<String, Object> parameters) {
        try (Traverser traverser = new Traverser(job)) {
            return traverser.betweenessCentrality(direction(parameters),
                                                  edgeLabel(parameters),
                                                  depth(parameters),
                                                  degree(parameters),
                                                  sample(parameters),
                                                  sourceLabel(parameters),
                                                  sourceSample(parameters),
                                                  sourceCLabel(parameters),
                                                  top(parameters));
        }
    }

    private static class Traverser extends AbstractCentAlgorithm.Traverser {

        private Traverser(UserJob<Object> job) {
            super(job);
        }

        private Object betweenessCentrality(Directions direction,
                                           String label,
                                           int depth,
                                           long degree,
                                           long sample,
                                           String sourceLabel,
                                           long sourceSample,
                                           String sourceCLabel,
                                           long topN) {
            assert depth > 0;
            assert degree > 0L || degree == NO_LIMIT;
            assert topN >= 0L || topN == NO_LIMIT;

            Map<Id, Float> globalBetweennesses = new HashMap<>();
            Id edgeLabelId = null;
            if (label != null) {
                edgeLabelId = graph().edgeLabel(label).id();
            }

            // TODO: sample the startVertices
            Iterator<Vertex> startVertices = this.vertices(sourceLabel,
                                                           sourceCLabel,
                                                           Query.NO_LIMIT);
            while (startVertices.hasNext()) {
                Id startVertex  = ((HugeVertex) startVertices.next()).id();
                globalBetweennesses.putIfAbsent(startVertex, 0.0f);
                Stack<Id> traversedVertices = new Stack<>();
                Map<Id, BetweennessNode> localBetweennesses = new HashMap<>();
                BetweennessNode startNode = new BetweennessNode(1, 0);
                localBetweennesses.put(startVertex, startNode);
                this.computeDistance(startVertex, localBetweennesses,
                                     traversedVertices, direction,
                                     edgeLabelId, depth, degree);
                this.computeBetweenness(startVertex, traversedVertices,
                                        globalBetweennesses,
                                        localBetweennesses);
            }
            if (topN > 0) {
                return HugeTraverser.topN(globalBetweennesses, true, topN);
            } else {
                return globalBetweennesses;
            }
        }

        private void computeDistance(Id startVertex,
                                     Map<Id, BetweennessNode> betweennesses,
                                     Stack<Id> traversedVertices, Directions direction,
                                     Id edgeLabelId, long degree, long depth) {
            LinkedList<Id> traversingVertices = new LinkedList<>();
            traversingVertices.add(startVertex);

            while (!traversingVertices.isEmpty()) {
                Id source = traversingVertices.removeFirst();
                traversedVertices.push(source);
                BetweennessNode sourceNode = betweennesses.get(source);
                if (sourceNode == null) {
                    sourceNode = new BetweennessNode();
                    betweennesses.put(source, sourceNode);
                }
                // TODO: sample the edges
                Iterator<HugeEdge> edges = (Iterator) this.edgesOfVertex(
                                           source, direction, edgeLabelId,
                                           degree);
                while (edges.hasNext()) {
                    HugeEdge edge = edges.next();
                    Id targetId = edge.otherVertex().id();
                    BetweennessNode targetNode = betweennesses.get(targetId);
                    // edge's targetNode is arrived at first time
                    if (targetNode == null) {
                        targetNode = new BetweennessNode(sourceNode);
                        betweennesses.put(targetId, targetNode);
                        if (depth == NO_LIMIT ||
                            targetNode.distance() <= depth) {
                            traversingVertices.addLast(targetId);
                        }
                    }
                    targetNode.addParentNodeIfNeeded(sourceNode, source);
                }
            }
        }

        private void computeBetweenness(
                     Id startVertex,
                     Stack<Id> traversedVertices,
                     Map<Id, Float> globalBetweennesses,
                     Map<Id, BetweennessNode> localBetweennesses) {
            while (!traversedVertices.empty()) {
                Id currentId = traversedVertices.pop();
                BetweennessNode currentNode =
                                localBetweennesses.get(currentId);
                if (currentId.equals(startVertex)) {
                    continue;
                }
                // add to globalBetweennesses
                float betweenness = globalBetweennesses.getOrDefault(currentId,
                                                                     0.0f);
                betweenness += currentNode.betweenness();
                globalBetweennesses.put(currentId, betweenness);

                // contribute to parent
                for (Id v : currentNode.parents()) {
                    BetweennessNode parentNode = localBetweennesses.get(v);
                    parentNode.increaseBetweenness(currentNode);
                }
            }
        }
    }

    /**
     * the temp data structure for a vertex used in computing process.
     */
    private static class BetweennessNode {

        private Id[] parents;
        private int pathCount;
        private int distance;
        private float betweenness;

        public BetweennessNode() {
            this(0, -1);
        }

        public BetweennessNode(BetweennessNode parentNode) {
            this(0, parentNode.distance + 1);
        }

        public BetweennessNode(int pathCount, int distance) {
            this.pathCount = pathCount;
            this.distance = distance;
            this.parents = new Id[0];
            this.betweenness = 0.0f;
        }

        public int distance() {
            return this.distance;
        }

        public Id[] parents() {
            return this.parents;
        }

        public void addParent(Id parentId) {
            Id[] newParents = new Id[this.parents.length + 1];
            System.arraycopy(this.parents, 0, newParents, 0,
                             this.parents.length);
            newParents[newParents.length - 1] = parentId;
            this.parents = newParents;
        }

        public void increaseBetweenness(BetweennessNode childNode) {
            float increase = (float) this.pathCount / childNode.pathCount *
                             (1 + childNode.betweenness);
            this.betweenness += increase;
        }

        public void addParentNodeIfNeeded(BetweennessNode node, Id parentId) {
            if (this.distance == node.distance + 1) {
                this.pathCount += node.pathCount;
                this.addParent(parentId);
            }
        }

        public float betweenness() {
            return this.betweenness;
        }
    }
}
