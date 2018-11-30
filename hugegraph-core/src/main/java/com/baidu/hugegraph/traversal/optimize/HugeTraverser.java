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

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.tinkerpop.gremlin.structure.Edge;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.rest.ClientException;
import com.baidu.hugegraph.schema.SchemaLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class HugeTraverser {

    private HugeGraph graph;

    public static final List<Id> PATH_NONE = ImmutableList.of();
    public static final long NO_LIMIT = -1L;

    public HugeTraverser(HugeGraph graph) {
        this.graph = graph;
    }

    public List<Id> shortestPath(Id sourceV, Id targetV, Directions dir,
                                 String label, int depth, long degree,
                                 long capacity) {
        E.checkNotNull(sourceV, "source vertex id");
        E.checkNotNull(targetV, "target vertex id");
        E.checkNotNull(dir, "direction");
        checkPositive(depth, "max depth");
        checkDegree(degree);
        checkCapacity(capacity);

        if (sourceV.equals(targetV)) {
            return ImmutableList.of(sourceV);
        }

        Id labelId = this.getEdgeLabelId(label);
        ShortestPathTraverser traverser = new ShortestPathTraverser(
                                              sourceV, targetV, dir, labelId,
                                              degree, capacity);
        List<Id> path;
        while (true) {
            // Found, reach max depth or reach capacity, stop searching
            if ((path = traverser.forward()) != PATH_NONE ||
                --depth <= 0 || traverser.reachCapacity()) {
                break;
            }

            if ((path = traverser.backward()) != PATH_NONE ||
                --depth <= 0 || traverser.reachCapacity()) {
                Collections.reverse(path);
                break;
            }
        }
        return path;
    }

    public Set<Path> paths(Id sourceV, Directions sourceDir,
                           Id targetV, Directions targetDir, String label,
                           int depth, long degree, long capacity, long limit) {
        E.checkNotNull(sourceV, "source vertex id");
        E.checkNotNull(targetV, "target vertex id");
        E.checkNotNull(sourceDir, "source direction");
        E.checkNotNull(targetDir, "target direction");
        E.checkArgument(sourceDir == targetDir ||
                        sourceDir == targetDir.opposite(),
                        "Source direction must equal to target direction," +
                        "or opposite to target direction");
        checkPositive(depth, "max depth");
        checkDegree(degree);
        checkCapacity(capacity);
        checkLimit(limit);

        Set<Path> paths = new HashSet<>();
        if (sourceV.equals(targetV)) {
            paths.add(new Path(sourceV, ImmutableList.of(sourceV)));
        }

        Id labelId = this.getEdgeLabelId(label);
        PathsTraverser traverser = new PathsTraverser(sourceV, targetV, labelId,
                                                      degree, capacity, limit);
        while (true) {
            if (--depth < 0 || traverser.reachLimit()) {
                break;
            }
            List<Path> foundPaths = traverser.forward(sourceDir);
            paths.addAll(foundPaths);

            if (--depth < 0 || traverser.reachLimit()) {
                break;
            }
            foundPaths = traverser.backward(targetDir);
            for (Path path : foundPaths) {
                path.reverse();
                paths.add(path);
            }
        }
        return paths;
    }

    public List<Path> rays(Id sourceV, Directions dir, String label,
                           int depth, long degree, long capacity, long limit) {
        return this.subGraphPaths(sourceV, dir, label, depth, degree,
                                  capacity, limit, false);
    }

    public List<Path> rings(Id sourceV, Directions dir, String label,
                            int depth, long degree, long capacity, long limit) {
        return this.subGraphPaths(sourceV, dir, label, depth, degree,
                                  capacity, limit, true);
    }

    private List<Path> subGraphPaths(Id sourceV, Directions dir, String label,
                                     int depth, long degree, long capacity,
                                     long limit, boolean rings) {
        E.checkNotNull(sourceV, "source vertex id");
        E.checkNotNull(dir, "direction");
        checkPositive(depth, "max depth");
        checkDegree(degree);
        checkCapacity(capacity);
        checkLimit(limit);

        Id labelId = this.getEdgeLabelId(label);
        SubGraphTraverser traverser = new SubGraphTraverser(sourceV, labelId,
                                                            degree, capacity,
                                                            limit, rings);
        List<Path> paths = new ArrayList<>();
        while (true) {
            paths.addAll(traverser.forward(dir));
            if (--depth < 0 || traverser.reachLimit() ||
                traverser.finished()) {
                break;
            }
        }
        return paths;
    }

    public Set<Id> kout(Id sourceV, Directions dir, String label,
                        int depth, boolean nearest,
                        long degree, long capacity, long limit) {
        E.checkNotNull(sourceV, "source vertex id");
        E.checkNotNull(dir, "direction");
        checkPositive(depth, "k-out depth");
        checkDegree(degree);
        checkCapacity(capacity);
        checkLimit(limit);
        if (capacity != NO_LIMIT) {
            // Capacity must > limit because sourceV is counted in capacity
            E.checkArgument(capacity > limit && limit != NO_LIMIT,
                            "Capacity must be greater than limit, " +
                            "but got capacity '%s' and limit '%s'",
                            capacity, limit);
        }

        Id labelId = this.getEdgeLabelId(label);

        Set<Id> latest = newSet();
        latest.add(sourceV);

        Set<Id> all = newSet();
        all.add(sourceV);

        long remaining = capacity == NO_LIMIT ?
                         NO_LIMIT : capacity - latest.size();
        while (depth-- > 0) {
            // Just get limit nodes in last layer if limit < remaining capacity
            if (depth == 0 && limit != NO_LIMIT &&
                (limit < remaining || remaining == NO_LIMIT)) {
                remaining = limit;
            }
            if (nearest) {
                latest = this.adjacentVertices(latest, dir, labelId, all,
                                               degree, remaining);
                all.addAll(latest);
            } else {
                latest = this.adjacentVertices(latest, dir, labelId, null,
                                               degree, remaining);
            }
            if (capacity != NO_LIMIT) {
                // Update 'remaining' value to record remaining capacity
                remaining -= latest.size();

                if (remaining <= 0 && depth > 0) {
                    throw new ClientException(
                              "Reach limit '%s' while remaining depth '%s'",
                              limit, depth);
                }
            }
        }

        return latest;
    }

    public Set<Id> kneighbor(Id sourceV, Directions dir,
                             String label, int depth,
                             long degree, long limit) {
        E.checkNotNull(sourceV, "source vertex id");
        E.checkNotNull(dir, "direction");
        checkPositive(depth, "k-neighbor depth");
        checkDegree(degree);
        checkLimit(limit);

        Id labelId = this.getEdgeLabelId(label);

        Set<Id> latest = newSet();
        latest.add(sourceV);

        Set<Id> all = newSet();
        all.add(sourceV);

        while (depth-- > 0) {
            long remaining = limit == NO_LIMIT ? NO_LIMIT : limit - all.size();
            latest = this.adjacentVertices(latest, dir, labelId, all,
                                           degree, remaining);
            all.addAll(latest);
            if (limit != NO_LIMIT && all.size() >= limit) {
                break;
            }
        }

        return all;
    }

    private Set<Id> adjacentVertices(Set<Id> vertices, Directions dir,
                                     Id label, Set<Id> excluded,
                                     long degree, long limit) {
        if (limit == 0) {
            return ImmutableSet.of();
        }

        Set<Id> neighbors = newSet();
        for (Id source : vertices) {
            Iterator<Edge> edges = this.edgesOfVertex(source, dir,
                                                      label, degree);
            while (edges.hasNext()) {
                HugeEdge e = (HugeEdge) edges.next();
                Id target = e.id().otherVertexId();
                if (excluded != null && excluded.contains(target)) {
                    continue;
                }
                neighbors.add(target);
                if (limit != NO_LIMIT && neighbors.size() >= limit) {
                    return neighbors;
                }
            }
        }
        return neighbors;
    }

    private Iterator<Edge> edgesOfVertex(Id source, Directions dir,
                                         Id label, long limit) {
        Id[] labels = {};
        if (label != null) {
            labels = new Id[]{label};
        }

        Query query = GraphTransaction.constructEdgesQuery(source, dir, labels);
        if (limit != NO_LIMIT) {
            query.limit(limit);
        }
        return this.graph.edges(query);
    }

    private Id getEdgeLabelId(Object label) {
        if (label == null) {
            return null;
        }
        return SchemaLabel.getLabelId(this.graph, HugeType.EDGE, label);
    }

    private static void checkPositive(int value, String name) {
        E.checkArgument(value > 0,
                        "The %s parameter must be > 0, but got '%s'",
                        name, value);
    }

    private static void checkDegree(long degree) {
        checkPositiveOrNoLimit(degree, "max degree");
    }

    private static void checkCapacity(long capacity) {
        checkPositiveOrNoLimit(capacity, "capacity");
    }

    private static void checkLimit(long limit) {
        checkPositiveOrNoLimit(limit, "limit");
    }

    private static void checkPositiveOrNoLimit(long value, String name) {
        E.checkArgument(value > 0 || value == NO_LIMIT,
                        "The %s parameter must be > 0 or == %s, but got: %s",
                        name, NO_LIMIT, value);
    }

    private static <V> Set<V> newSet() {
        return new HashSet<>();
    }

    private static <K, V> Map<K, V> newMap() {
        return new HashMap<>();
    }

    private static <K, V> MultivaluedMap<K, V> newMultivalueMap() {
        return new MultivaluedHashMap<>();
    }

    private class ShortestPathTraverser {

        // TODO: change Map to Set to reduce memory cost
        private Map<Id, Node> sources = newMap();
        private Map<Id, Node> targets = newMap();

        private final Directions direction;
        private final Id label;
        private final long degree;
        private final long capacity;
        private long size;

        public ShortestPathTraverser(Id sourceV, Id targetV, Directions dir,
                                     Id label, long degree, long capacity) {
            this.sources.put(sourceV, new Node(sourceV));
            this.targets.put(targetV, new Node(targetV));
            this.direction = dir;
            this.label = label;
            this.degree = degree;
            this.capacity = capacity;
            this.size = 0L;
        }

        /**
         * Search forward from source
         */
        public List<Id> forward() {
            Map<Id, Node> newVertices = newMap();
            // Traversal vertices of previous level
            for (Node v : this.sources.values()) {
                Iterator<Edge> edges = edgesOfVertex(v.id(), this.direction,
                                                     this.label, this.degree);
                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    // If cross point exists, shortest path found, concat them
                    if (this.targets.containsKey(target)) {
                        return v.joinPath(this.targets.get(target));
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
            this.size += newVertices.size();

            return PATH_NONE;
        }

        /**
         * Search backward from target
         */
        public List<Id> backward() {
            Map<Id, Node> newVertices = newMap();
            Directions opposite = this.direction.opposite();
            // Traversal vertices of previous level
            for (Node v : this.targets.values()) {
                Iterator<Edge> edges = edgesOfVertex(v.id(), opposite,
                                                     this.label, this.degree);
                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    // If cross point exists, shortest path found, concat them
                    if (this.sources.containsKey(target)) {
                        return v.joinPath(this.sources.get(target));
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
            this.size += newVertices.size();

            return PATH_NONE;
        }

        private boolean reachCapacity() {
            if (this.capacity == NO_LIMIT || this.size < this.capacity) {
                return false;
            }
            return true;
        }
    }

    private class PathsTraverser {

        private MultivaluedMap<Id, Node> sources = newMultivalueMap();
        private MultivaluedMap<Id, Node> targets = newMultivalueMap();
        private MultivaluedMap<Id, Node> sourcesAll = newMultivalueMap();
        private MultivaluedMap<Id, Node> targetsAll = newMultivalueMap();

        private final Id label;
        private final long degree;
        private final long capacity;
        private final long limit;
        private long count;

        public PathsTraverser(Id sourceV, Id targetV, Id label,
                              long degree, long capacity, long limit) {
            this.sources.add(sourceV, new Node(sourceV));
            this.targets.add(targetV, new Node(targetV));
            this.sourcesAll.putAll(this.sources);
            this.targetsAll.putAll(this.targets);
            this.label = label;
            this.degree = degree;
            this.capacity = capacity;
            this.limit = limit;
            this.count = 0L;
        }

        /**
         * Search forward from source
         */
        public List<Path> forward(Directions direction) {
            List<Path> paths = new ArrayList<>();
            MultivaluedMap<Id, Node> newVertices = newMultivalueMap();
            Iterator<Edge> edges;
            // Traversal vertices of previous level
            for (List<Node> nodes : this.sources.values()) {
                for (Node n : nodes) {
                    edges = edgesOfVertex(n.id(), direction,
                                          this.label, this.degree);
                    while (edges.hasNext()) {
                        HugeEdge edge = (HugeEdge) edges.next();
                        Id target = edge.id().otherVertexId();

                        // If have loop, skip target
                        if (n.contains(target)) {
                            continue;
                        }

                        // If cross point exists, path found, concat them
                        if (this.targetsAll.containsKey(target)) {
                            for (Node node : this.targetsAll.get(target)) {
                                List<Id> path = n.joinPath(node);
                                if (!path.isEmpty()) {
                                    paths.add(new Path(target, path));
                                    ++this.count;
                                    if (this.reachLimit()) {
                                        return paths;
                                    }
                                }
                            }
                        }

                        // Add node to next start-nodes
                        newVertices.add(target, new Node(target, n));
                    }
                }
            }
            // Re-init sources
            this.sources = newVertices;
            // Record all passed vertices
            this.sourcesAll.putAll(newVertices);

            return paths;
        }

        /**
         * Search backward from target
         */
        public List<Path> backward(Directions direction) {
            List<Path> paths = new ArrayList<>();
            MultivaluedMap<Id, Node> newVertices = newMultivalueMap();
            Iterator<Edge> edges;
            // Traversal vertices of previous level
            for (List<Node> nodes : this.targets.values()) {
                for (Node n : nodes) {
                    edges = edgesOfVertex(n.id(), direction,
                                          this.label, this.degree);
                    while (edges.hasNext()) {
                        HugeEdge edge = (HugeEdge) edges.next();
                        Id target = edge.id().otherVertexId();

                        // If have loop, skip target
                        if (n.contains(target)) {
                            continue;
                        }

                        // If cross point exists, path found, concat them
                        if (this.sourcesAll.containsKey(target)) {
                            for (Node node : this.sourcesAll.get(target)) {
                                List<Id> path = n.joinPath(node);
                                if (!path.isEmpty()) {
                                    paths.add(new Path(target, path));
                                    ++this.count;
                                    if (this.reachLimit()) {
                                        return paths;
                                    }
                                }
                            }
                        }

                        // Add node to next start-nodes
                        newVertices.add(target, new Node(target, n));
                    }
                }
            }

            // Re-init targets
            this.targets = newVertices;
            // Record all passed vertices
            this.targetsAll.putAll(newVertices);

            return paths;
        }

        private int accessedNodes() {
            return this.sourcesAll.size() + this.targetsAll.size();
        }

        private boolean reachLimit() {
            if (this.capacity != NO_LIMIT &&
                this.accessedNodes() > this.capacity) {
                return true;
            }
            if (this.limit == NO_LIMIT || this.count < this.limit) {
                return false;
            }
            return true;
        }
    }

    private class SubGraphTraverser {

        private MultivaluedMap<Id, Node> sources = newMultivalueMap();
        private Set<Id> accessedVertices = newSet();

        private final Id label;
        private final long degree;
        private final long capacity;
        private final long limit;
        private final boolean rings;
        private long pathCount;

        public SubGraphTraverser(Id sourceV, Id label, long degree,
                                 long capacity, long limit, boolean rings) {
            this.sources.add(sourceV, new Node(sourceV));
            this.accessedVertices.add(sourceV);
            this.label = label;
            this.degree = degree;
            this.capacity = capacity;
            this.limit = limit;
            this.rings = rings;
            this.pathCount = 0L;
        }

        /**
         * Search forward from source
         */
        public List<Path> forward(Directions direction) {
            List<Path> paths = new ArrayList<>();
            MultivaluedMap<Id, Node> newVertices = newMultivalueMap();
            Iterator<Edge> edges;
            // Traversal vertices of previous level
            for (List<Node> nodes : this.sources.values()) {
                for (Node n : nodes) {
                    edges = edgesOfVertex(n.id(), direction,
                                          this.label, this.degree);
                    if (!edges.hasNext()) {
                        // Reach the end, rays found
                        if (this.rings) {
                            continue;
                        }
                        // Store rays
                        paths.add(new Path(null, n.path()));
                        this.pathCount++;
                        if (reachLimit()) {
                            return paths;
                        }
                        continue;
                    }
                    while (edges.hasNext()) {
                        HugeEdge edge = (HugeEdge) edges.next();
                        Id target = edge.id().otherVertexId();
                        this.accessedVertices.add(target);

                        if (!n.contains(target)) {
                            // Add node to next start-nodes
                            newVertices.add(target, new Node(target, n));
                            continue;
                        }
                        // Rings found and expect rings
                        if (this.rings) {
                            assert n.contains(target);
                            List<Id> prePath = n.path();
                            prePath.add(target);
                            paths.add(new Path(null, prePath));
                            this.pathCount++;
                            if (reachLimit()) {
                                return paths;
                            }
                        }
                    }
                }
            }
            // Re-init sources
            this.sources = newVertices;

            return paths;
        }

        private boolean reachLimit() {
            if (this.capacity != NO_LIMIT &&
                this.accessedVertices.size() > this.capacity) {
                return true;
            }
            if (this.limit == NO_LIMIT || this.pathCount < this.limit) {
                return false;
            }
            return true;
        }

        private boolean finished() {
            return this.sources.isEmpty();
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

        @SuppressWarnings("unused")
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

            // Avoid loop in path
            if (CollectionUtils.containsAny(path, backPath)) {
                return ImmutableList.of();
            }

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

    public static class Path {

        private Id crosspoint;
        private List<Id> vertices;

        public Path(Id crosspoint, List<Id> vertices) {
            this.crosspoint = crosspoint;
            this.vertices = vertices;
        }

        public Id crosspoint() {
            return this.crosspoint;
        }

        public List<Id> vertices() {
            return this.vertices;
        }

        public void reverse() {
            Collections.reverse(this.vertices);
        }

        public Map<String, Object> toMap(boolean withCrossPoint) {
            if (withCrossPoint) {
                return ImmutableMap.of("crosspoint", this.crosspoint,
                                       "objects", this.vertices);
            } else {
                return ImmutableMap.of("objects", this.vertices);
            }
        }

        @Override
        public int hashCode() {
            return this.vertices.hashCode();
        }

        /**
         * Compares the specified object with this path for equality.
         * Returns <tt>true</tt> if and only if both have same vertices list
         * without regard of crosspoint.
         * @param other the object to be compared for equality with this path
         * @return <tt>true</tt> if the specified object is equal to this path
         */
        @Override
        public boolean equals(Object other) {
            if (other == null || !(other instanceof Path)) {
                return false;
            }
            return this.vertices.equals(((Path) other).vertices);
        }
    }
}
