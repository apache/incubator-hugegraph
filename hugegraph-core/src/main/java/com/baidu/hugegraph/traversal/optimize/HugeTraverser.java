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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

    public static final List<Id> PATH_NONE = ImmutableList.of();

    private HugeGraph graph;

    public HugeTraverser(HugeGraph graph) {
        this.graph = graph;
    }

    public List<Id> shortestPath(Id sourceV, Id targetV, Directions dir,
                                 String label, int maxDepth) {
        E.checkArgument(maxDepth >= 1,
                        "Shortest path step must >= 1, but got '%s'",
                        maxDepth);

        Id labelId = this.getEdgeLabelId(label);
        ShortestPathTraverser traverser = new ShortestPathTraverser(
                                              sourceV, targetV, dir, labelId);
        List<Id> path;
        while (true) {
            // Found or reach max depth, stop searching
            if ((path = traverser.forward()) != PATH_NONE || --maxDepth <= 0) {
                break;
            }

            if ((path = traverser.backward()) != PATH_NONE || --maxDepth <= 0) {
                Collections.reverse(path);
                break;
            }
        }
        return path;
    }

    public Set<Id> kout(Id sourceV, Directions dir, String label,
                        int depth, boolean nearest) {
        E.checkArgument(depth >= 1,
                        "K-out depth must >= 1, but got '%s'", depth);

        Id labelId = this.getEdgeLabelId(label);

        Set<Id> latest = newSet();
        latest.add(sourceV);

        Set<Id> all = newSet();
        all.add(sourceV);

        while (depth-- > 0) {
            if (nearest) {
                latest = this.adjacentVertices(latest, dir, labelId, all);
                all.addAll(latest);
            } else {
                latest = this.adjacentVertices(latest, dir, labelId, null);
            }
        }

        return latest;
    }

    public Set<Id> kneighbor(Id sourceV, Directions dir,
                             String label, int depth) {
        E.checkArgument(depth >= 1,
                        "K-neighbor depth must >= 1, but got '%s'", depth);
        Id labelId = this.getEdgeLabelId(label);

        Set<Id> latest = newSet();
        latest.add(sourceV);

        Set<Id> all = newSet();
        all.add(sourceV);

        while (depth-- > 0) {
            latest = this.adjacentVertices(latest, dir, labelId, all);
            all.addAll(latest);
        }

        return all;
    }

    private Set<Id> adjacentVertices(Set<Id> vertices, Directions dir,
                                     Id label, Set<Id> excluded) {
        Set<Id> neighbors = newSet();
        for (Id source : vertices) {
            Iterator<Edge> edges = this.edgesOfVertex(source, dir, label);
            while (edges.hasNext()) {
                HugeEdge e = (HugeEdge) edges.next();
                Id target = e.id().otherVertexId();
                if (excluded != null && excluded.contains(target)) {
                    continue;
                }
                neighbors.add(target);
            }
        }
        return neighbors;
    }

    private Iterator<Edge> edgesOfVertex(Id source, Directions dir, Id label) {
        Id[] labels = {};
        if (label != null) {
            labels = new Id[]{label};
        }
        if (dir == Directions.BOTH) {
            dir = null;
        }
        Query query = GraphTransaction.constructEdgesQuery(source, dir, labels);
        return this.graph.edges(query);
    }

    private Id getEdgeLabelId(Object label) {
        if (label == null) {
            return null;
        }
        return SchemaLabel.getLabelId(this.graph, HugeType.EDGE, label);
    }

    private static <V> Set<V> newSet() {
        return new HashSet<>();
    }

    private static <K, V> Map<K, V> newMap() {
        return new HashMap<>();
    }

    private class ShortestPathTraverser {

        // TODO: change Map to Set to reduce memory cost
        private Map<Id, Node> sources = newMap();
        private Map<Id, Node> targets = newMap();

        private final Directions direction;
        private final Id label;

        public ShortestPathTraverser(Id sourceV, Id targetV,
                                     Directions dir, Id label) {
            this.sources.put(sourceV, new Node(sourceV));
            this.targets.put(targetV, new Node(targetV));
            this.direction = dir;
            this.label = label;
        }

        /**
         * Search forward from source
         */
        public List<Id> forward() {
            Map<Id, Node> newVertices = newMap();
            // Traversal vertices of previous level
            for (Node v : this.sources.values()) {
                Iterator<Edge> edges = edgesOfVertex(v.id(), this.direction,
                                                     this.label);
                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    // If cross point exists, shortest path found, concat them
                    if (this.targets.containsKey(target)) {
                        Node targetNode = new Node(target, v);
                        return targetNode.joinPath(this.targets.get(target));
                    }

                    /*
                     * Not found shortest path yet, node is added to
                     * newVertices if:
                     * 1. not in sources and newVertices yet
                     * 2. path of node doesn't have loop
                     */
                    if (!newVertices.containsKey(target) &&
                        !this.sources.containsKey(target) &&
                        !v.contains(target)) {
                        newVertices.put(target, new Node(target, v));
                    }
                }
            }

            // Re-init sources
            this.sources = newVertices;

            return PATH_NONE;
        }

        /**
         * Search backward from target
         */
        public List<Id> backward() {
            Map<Id, Node> newVertices = newMap();
            Directions oppo = this.direction.opposite();
            // Traversal vertices of previous level
            for (Node v : this.targets.values()) {
                Iterator<Edge> edges = edgesOfVertex(v.id(), oppo, this.label);
                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    // If cross point exists, shortest path found, concat them
                    if (this.sources.containsKey(target)) {
                        Node targetNode = new Node(target, v);
                        return targetNode.joinPath(this.sources.get(target));
                    }

                    /*
                     * Not found shortest path yet, node is added to
                     * newVertices if:
                     * 1. not in targets and newVertices yet
                     * 2. path of node doesn't have loop
                     */
                    if (!newVertices.containsKey(target) &&
                        !this.targets.containsKey(target) &&
                        !v.contains(target)) {
                        newVertices.put(target, new Node(target, v));
                    }
                }
            }

            // Re-init targets
            this.targets = newVertices;

            return PATH_NONE;
        }
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

        public List<Id> joinPath(Node back) {
            // Get self path
            List<Id> path = this.path();

            // Get reversed other path
            List<Id> backPath = back.path();
            Collections.reverse(backPath);
            // Remove cross point in back path
            backPath.remove(0);

            // Append other path behind self path
            path.addAll(backPath);
            return path;
        }

        public boolean contains(Id id) {
            Node node = this;
            do {
                if (node.id.equals(id)) {
                    return true;
                }
                node = node.parent;
            } while (node != null);
            return false;
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
}
