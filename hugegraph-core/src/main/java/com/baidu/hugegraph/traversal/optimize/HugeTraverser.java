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

package com.baidu.hugegraph.traversal.optimize;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Edge;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.schema.SchemaLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableList;

public class HugeTraverser {

    private HugeGraph graph;

    public HugeTraverser(HugeGraph graph) {
        this.graph = graph;
    }

    public List<Id> shortestPath(Id sourceV, Id targetV, Directions dir,
                                 String label, int maxDepth) {
        E.checkArgument(maxDepth >= 1,
                        "Shortest path step must >= 1, but got '%s'",
                        maxDepth);
        E.checkArgument(dir == Directions.OUT || dir == Directions.IN,
                        "Direction of shortest path must be 'OUT' or 'IN', " +
                        "but got 'BOTH' or 'NULL'");
        Id[] labels = {};
        if (label != null) {
            Id labelId = SchemaLabel.getLabelId(graph, HugeType.EDGE, label);
            labels = new Id[]{labelId};
        }

        // TODO: change to InsertionOrderUtil
        Set<Node> prior = newSet();
        Set<Node> current = newSet();

        Node root = new Node(sourceV);
        prior.add(root);

        do {
            for (Node node : prior) {
                Query query = GraphTransaction.constructEdgesQuery(node.id(),
                                                                   dir, labels);
                Iterator<Edge> edges = this.graph.edges(query);
                while (edges.hasNext()) {
                    HugeEdge e = (HugeEdge) edges.next();
                    Node target = new Node(e.id().otherVertexId(), node);
                    if (!current.contains(target) && !prior.contains(target)) {
                        current.add(target);
                    }
                    if (target.id().equals(targetV)) {
                        return target.path();
                    }
                }
            }

            // Swap prior and current
            Set<Node> temp = prior;
            prior = current;
            current = temp;
            current.clear();
        } while (--maxDepth > 0);

        return ImmutableList.of();
    }

    public Set<Id> kout(Id sourceV, Directions dir,
                        String label, int depth) {
        E.checkArgument(depth >= 1,
                        "K-out depth must >= 1, but got '%s'", depth);
        E.checkArgument(dir == Directions.OUT || dir == Directions.IN,
                        "Direction of k-out must be 'OUT' or 'IN', " +
                        "but got 'BOTH' or 'NULL'");
        Id[] labels = {};
        if (label != null) {
            Id labelId = SchemaLabel.getLabelId(graph, HugeType.EDGE, label);
            labels = new Id[]{labelId};
        }

        Set<Id> prior = newSet();
        prior.add(sourceV);

        do {
            prior = this.adjacentVertices(prior, dir, labels);
        } while (--depth > 0);
        return prior;
    }

    public Set<Id> kneighbor(Id sourceV, Directions dir,
                             String label, int depth) {
        E.checkArgument(depth >= 1,
                        "K-neighbor depth must >= 1, but got '%s'", depth);
        E.checkArgument(dir == Directions.OUT || dir == Directions.IN,
                        "Direction of k-neighbor must be 'OUT' or 'IN', " +
                        "but got 'BOTH' or 'NULL'");
        Id[] labels = {};
        if (label != null) {
            Id labelId = SchemaLabel.getLabelId(graph, HugeType.EDGE, label);
            labels = new Id[]{labelId};
        }

        Set<Id> prior = newSet();
        prior.add(sourceV);

        Set<Id> allNeighbors = newSet();
        allNeighbors.add(sourceV);

        do {
            prior = this.adjacentVertices(prior, dir, labels);
            allNeighbors.addAll(prior);
        } while (--depth > 0);
        return allNeighbors;
    }

    private Set<Id> adjacentVertices(Set<Id> vertices, Directions dir,
                                     Id[] labels) {
        Set<Id> neighbors = newSet();
        for (Id source : vertices) {
            Query query = GraphTransaction.constructEdgesQuery(source, dir,
                                                               labels);
            Iterator<Edge> edges = this.graph.edges(query);
            while (edges.hasNext()) {
                HugeEdge e = (HugeEdge) edges.next();
                Id target = e.id().otherVertexId();
                neighbors.add(target);
            }
        }
        return neighbors;
    }

    private static class Node {

        private Id id;
        private Node parent;

        public Node(Id id) {
            this(id, null);
        }

        public Node(Id id, Node parent) {
            E.checkArgumentNotNull(id, "Id of Node can't be null");
            this.id = id;
            this.parent = parent;
        }

        public Id id() {
            return this.id;
        }

        public Node parent() {
            return this.parent;
        }

        public List<Id> path() {
            List<Id> ids = new ArrayList<>();
            Node current = this;
            do {
                ids.add(current.id);
                current = current.parent;
            } while (current != null);
            Collections.reverse(ids);
            return ids;
        }

        @Override
        public int hashCode() {
            return this.id.hashCode();
        }

        @Override
        public boolean equals(Object object) {
            if (!(object instanceof Node)) {
                return false;
            }
            Node other = (Node) object;
            return this.id.equals(other.id);
        }
    }

    public static <V> Set<V> newSet() {
        return new HashSet<>();
    }
}
