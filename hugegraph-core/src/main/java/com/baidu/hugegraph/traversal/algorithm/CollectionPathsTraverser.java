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

package com.baidu.hugegraph.traversal.algorithm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.ws.rs.core.MultivaluedMap;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public class CollectionPathsTraverser extends TpTraverser {

    private static final Logger LOG = Log.logger(CollectionPathsTraverser.class);

    public CollectionPathsTraverser(HugeGraph graph) {
        super(graph, "collection-paths");
    }

    @SuppressWarnings("unchecked")
    public Collection<Path> paths(Iterator<Vertex> sources,
                                  Iterator<Vertex> targets,
                                  EdgeStep step, int depth, boolean nearest,
                                  long capacity, long limit) {
        checkCapacity(capacity);
        checkLimit(limit);

        List<Id> sourceList = new ArrayList<>();
        while (sources.hasNext()) {
            sourceList.add(((HugeVertex) sources.next()).id());
        }
        int sourceSize = sourceList.size();
        E.checkState(sourceSize >= 1 && sourceSize <= MAX_VERTICES,
                     "The number of source vertices must in [1, %s], " +
                     "but got: %s", MAX_VERTICES, sourceList.size());
        List<Id> targetList = new ArrayList<>();
        while (targets.hasNext()) {
            targetList.add(((HugeVertex) targets.next()).id());
        }
        int targetSize = targetList.size();
        E.checkState(targetSize >= 1 && targetSize <= MAX_VERTICES,
                     "The number of target vertices must in [1, %s], " +
                     "but got: %s", MAX_VERTICES, sourceList.size());
        checkPositive(depth, "max depth");

        Traverser traverser = depth >= this.concurrentDepth() ?
                              this.concurrentTraverser(sourceList, targetList,
                                                       step, nearest,
                                                       capacity, limit) :
                              this.singleTraverser(sourceList, targetList, step,
                                                   nearest, capacity, limit);

        while (true) {
            if (--depth < 0 || traverser.reachLimit()) {
                break;
            }
            traverser.forward();

            if (--depth < 0 || traverser.reachLimit()) {
                break;
            }
            traverser.backward();
        }
        return traverser.paths();
    }

    private Traverser singleTraverser(List<Id> sources, List<Id> targets,
                                      EdgeStep step, boolean nearest,
                                      long capacity, long limit) {
        return nearest ? new SingleNearestTraverser(sources, targets, step,
                                                    capacity, limit) :
                         new SingleAllTraverser(sources, targets, step,
                                                capacity, limit);
    }

    private Traverser concurrentTraverser(List<Id> sources, List<Id> targets,
                                          EdgeStep step, boolean nearest,
                                          long capacity, long limit) {
        return new ConcurrentTraverser(sources, targets, step, capacity, limit);
    }

    private class Traverser {

        protected final EdgeStep step;
        protected final long capacity;
        protected final long limit;

        public Traverser(EdgeStep step, long capacity, long limit) {
            this.step = step;
            this.capacity = capacity;
            this.limit = limit;
        }

        public void forward() {
        }

        public void backward() {
        }

        public Set<Path> paths() {
            return new PathSet();
        }

        public int pathCount() {
            return 0;
        }

        protected int accessedNodes() {
            return 0;
        }

        protected boolean reachLimit() {
            checkCapacity(this.capacity, this.accessedNodes(),
                          "collection paths");
            if (this.limit == NO_LIMIT || this.pathCount() < this.limit) {
                return false;
            }
            return true;
        }
    }

    private class ConcurrentTraverser extends Traverser {

        private ConcurrentMultiValuedMap<Id, Node> sources =
                new ConcurrentMultiValuedMap<>();
        private ConcurrentMultiValuedMap<Id, Node> targets =
                new ConcurrentMultiValuedMap<>();
        private ConcurrentMultiValuedMap<Id, Node> sourcesAll =
                new ConcurrentMultiValuedMap<>();
        private ConcurrentMultiValuedMap<Id, Node> targetsAll =
                new ConcurrentMultiValuedMap<>();

        private Set<Path> paths;

        public ConcurrentTraverser(Collection<Id> sources,
                                   Collection<Id> targets, EdgeStep step,
                                   long capacity, long limit) {
            super(step, capacity, limit);
            for (Id id : sources) {
                this.sources.add(id, new Node(id));
            }
            for (Id id : targets) {
                this.targets.add(id, new Node(id));
            }
            this.sourcesAll.putAll(this.sources);
            this.targetsAll.putAll(this.targets);

            this.paths = ConcurrentHashMap.newKeySet();
        }

        /**
         * Search forward from sources
         */
        public void forward() {
            ConcurrentMultiValuedMap<Id, Node> newVertices =
                                               new ConcurrentMultiValuedMap<>();
            // Traversal vertices of previous level
            traverseIds(this.sources.keySet().iterator(), vid -> {
                if (this.reachLimit()) {
                    return;
                }
                Iterator<Edge> edges = edgesOfVertex(vid, this.step);
                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    for (Node n : this.sources.get(vid)) {
                        // If have loop, skip target
                        if (n.contains(target)) {
                            continue;
                        }

                        // If cross point exists, path found, concat them
                        if (this.targetsAll.containsKey(target)) {
                            for (Node node : this.targetsAll.get(target)) {
                                List<Id> path = n.joinPath(node);
                                if (!path.isEmpty()) {
                                    this.paths.add(new Path(target, path));
                                    if (this.reachLimit()) {
                                        return;
                                    }
                                }
                            }
                        }

                        // Add node to next start-nodes
                        newVertices.add(target, new Node(target, n));
                    }
                }
            });

            // Re-init sources
            this.sources = newVertices;
            // Record all passed vertices
            for (Map.Entry<Id, List<Node>> entry : newVertices.entrySet()) {
                this.sourcesAll.addAll(entry.getKey(), entry.getValue());
            }
        }

        /**
         * Search backward from target
         */
        public void backward() {
            ConcurrentMultiValuedMap<Id, Node> newVertices =
                                               new ConcurrentMultiValuedMap<>();
            this.step.swithDirection();
            // Traversal vertices of previous level
            traverseIds(this.targets.keySet().iterator(), vid -> {
                if (this.reachLimit()) {
                    return;
                }
                Iterator<Edge> edges = edgesOfVertex(vid, this.step);

                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    for (Node n : this.targets.get(vid)) {
                        // If have loop, skip target
                        if (n.contains(target)) {
                            continue;
                        }

                        // If cross point exists, path found, concat them
                        if (this.sourcesAll.containsKey(target)) {
                            for (Node node : this.sourcesAll.get(target)) {
                                List<Id> path = n.joinPath(node);
                                if (!path.isEmpty()) {
                                    Path newPath = new Path(target, path);
                                    newPath.reverse();
                                    this.paths.add(newPath);
                                    if (this.reachLimit()) {
                                        return;
                                    }
                                }
                            }
                        }

                        // Add node to next start-nodes
                        newVertices.add(target, new Node(target, n));
                    }
                }
            });
            this.step.swithDirection();

            // Re-init targets
            this.targets = newVertices;
            // Record all passed vertices
            for (Map.Entry<Id, List<Node>> entry : newVertices.entrySet()) {
                this.targetsAll.addAll(entry.getKey(), entry.getValue());
            }
        }

        @Override
        public Set<Path> paths() {
            return this.paths;
        }

        @Override
        public int pathCount() {
            return this.paths.size();
        }

        protected int accessedNodes() {
            return this.sourcesAll.size() + this.targetsAll.size();
        }
    }

    private class SingleAllTraverser extends Traverser {

        private MultivaluedMap<Id, Node> sources = newMultivalueMap();
        private MultivaluedMap<Id, Node> targets = newMultivalueMap();
        private MultivaluedMap<Id, Node> sourcesAll = newMultivalueMap();
        private MultivaluedMap<Id, Node> targetsAll = newMultivalueMap();

        private PathSet paths;

        public SingleAllTraverser(Collection<Id> sources,
                                  Collection<Id> targets,
                                  EdgeStep step, long capacity, long limit) {
            super(step, capacity, limit);
            for (Id id : sources) {
                this.sources.add(id, new Node(id));
            }
            for (Id id : targets) {
                this.targets.add(id, new Node(id));
            }
            this.sourcesAll.putAll(this.sources);
            this.targetsAll.putAll(this.targets);
            this.paths = new PathSet();
        }

        /**
         * Search forward from sources
         */
        public void forward() {
            MultivaluedMap<Id, Node> newVertices = newMultivalueMap();
            Iterator<Edge> edges;
            // Traversal vertices of previous level
            for (Map.Entry<Id, List<Node>> entry : this.sources.entrySet()) {
                Id vid = entry.getKey();
                edges = edgesOfVertex(vid, this.step);

                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    for (Node n : entry.getValue()) {
                        // If have loop, skip target
                        if (n.contains(target)) {
                            continue;
                        }

                        // If cross point exists, path found, concat them
                        if (this.targetsAll.containsKey(target)) {
                            for (Node node : this.targetsAll.get(target)) {
                                List<Id> path = n.joinPath(node);
                                if (!path.isEmpty()) {
                                    this.paths.add(new Path(target, path));
                                    if (this.reachLimit()) {
                                        return;
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
            this.sources = newVertices;
            // Record all passed vertices
            for (Map.Entry<Id, List<Node>> entry : newVertices.entrySet()) {
                this.sourcesAll.addAll(entry.getKey(), entry.getValue());
            }
        }

        /**
         * Search backward from target
         */
        public void backward() {
            PathSet paths = new PathSet();
            MultivaluedMap<Id, Node> newVertices = newMultivalueMap();
            this.step.swithDirection();
            Iterator<Edge> edges;
            // Traversal vertices of previous level
            for (Map.Entry<Id, List<Node>> entry : this.targets.entrySet()) {
                Id vid = entry.getKey();
                edges = edgesOfVertex(vid, this.step);

                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    for (Node n : entry.getValue()) {
                        // If have loop, skip target
                        if (n.contains(target)) {
                            continue;
                        }

                        // If cross point exists, path found, concat them
                        if (this.sourcesAll.containsKey(target)) {
                            for (Node node : this.sourcesAll.get(target)) {
                                List<Id> path = n.joinPath(node);
                                if (!path.isEmpty()) {
                                    Path newPath = new Path(target, path);
                                    newPath.reverse();
                                    this.paths.add(newPath);
                                    if (this.reachLimit()) {
                                        return;
                                    }
                                }
                            }
                        }

                        // Add node to next start-nodes
                        newVertices.add(target, new Node(target, n));
                    }
                }
            }
            this.step.swithDirection();

            // Re-init targets
            this.targets = newVertices;
            // Record all passed vertices
            for (Map.Entry<Id, List<Node>> entry : newVertices.entrySet()) {
                this.targetsAll.addAll(entry.getKey(), entry.getValue());
            }
        }

        @Override
        public Set<Path> paths() {
            return this.paths;
        }

        @Override
        public int pathCount() {
            return this.paths.size();
        }

        protected int accessedNodes() {
            return this.sourcesAll.size() + this.targetsAll.size();
        }
    }


    private class SingleNearestTraverser extends Traverser {

        private Map<Id, Node> sources = new HashMap<>();
        private Map<Id, Node> targets = new HashMap<>();
        private Map<Id, Node> sourcesAll = new HashMap<>();
        private Map<Id, Node> targetsAll = new HashMap<>();

        private PathSet paths;

        public SingleNearestTraverser(Collection<Id> sources,
                                      Collection<Id> targets,
                                      EdgeStep step, long capacity,
                                      long limit) {
            super(step, capacity, limit);
            for (Id id : sources) {
                this.sources.put(id, new KNode(id, null));
            }
            for (Id id : targets) {
                this.targets.put(id, new KNode(id, null));
            }
            this.sourcesAll.putAll(this.sources);
            this.targetsAll.putAll(this.targets);
            this.paths = new PathSet();
        }

        /**
         * Search forward from sources
         */
        public void forward() {
            LOG.info("Forward with sources size {} and sources all size {}",
                     this.sources.size(), this.sourcesAll.size());
            Map<Id, Node> newVertices = new HashMap<>();
            Iterator<Edge> edges;
            // Traversal vertices of previous level
            for (Map.Entry<Id, Node> entry : this.sources.entrySet()) {
                Id vid = entry.getKey();
                edges = edgesOfVertex(vid, this.step);

                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    Node n = entry.getValue();
                    // If have loop, skip target
                    if (n.contains(target)) {
                        continue;
                    }

                    // If cross point exists, path found, concat them
                    if (this.targetsAll.containsKey(target)) {
                        Node node = this.targetsAll.get(target);
                        List<Id> path = n.joinPath(node);
                        if (!path.isEmpty()) {
                            this.paths.add(new Path(target, path));
                            if (this.reachLimit()) {
                                return;
                            }
                        }
                    }

                    // Add node to next start-nodes
                    newVertices.putIfAbsent(target,
                                            new KNode(target, (KNode) n));
                }
            }

            // Re-init targets
            this.sources = newVertices;
            // Record all passed vertices
            for (Map.Entry<Id, Node> entry : newVertices.entrySet()) {
                this.sourcesAll.putIfAbsent(entry.getKey(), entry.getValue());
            }
            LOG.info("Done forward with sources size {} and sources all size {}",
                     this.sources.size(), this.sourcesAll.size());
        }

        /**
         * Search backward from target
         */
        public void backward() {
            LOG.info("Backward with targets size {} and targets all size {}",
                     this.targets.size(), this.targetsAll.size());
            Map<Id, Node> newVertices = new HashMap<>();
            this.step.swithDirection();
            Iterator<Edge> edges;
            // Traversal vertices of previous level
            for (Map.Entry<Id, Node> entry : this.targets.entrySet()) {
                Id vid = entry.getKey();
                edges = edgesOfVertex(vid, this.step);

                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    Node n = entry.getValue();
                    // If have loop, skip target
                    if (n.contains(target)) {
                        continue;
                    }

                    // If cross point exists, path found, concat them
                    if (this.sourcesAll.containsKey(target)) {
                        Node node = this.sourcesAll.get(target);
                        List<Id> path = n.joinPath(node);
                        if (!path.isEmpty()) {
                            Path newPath = new Path(target, path);
                            newPath.reverse();
                            this.paths.add(newPath);
                            if (this.reachLimit()) {
                                return;
                            }
                        }
                    }

                    // Add node to next start-nodes
                    newVertices.putIfAbsent(target,
                                            new KNode(target, (KNode) n));
                }
            }
            this.step.swithDirection();

            // Re-init targets
            this.targets = newVertices;
            // Record all passed vertices
            for (Map.Entry<Id, Node> entry : newVertices.entrySet()) {
                this.targetsAll.putIfAbsent(entry.getKey(), entry.getValue());
            }
            LOG.info("Done backward with sources size {} and sources all size {}",
                     this.targets.size(), this.targetsAll.size());
        }

        @Override
        public Set<Path> paths() {
            return this.paths;
        }

        @Override
        public int pathCount() {
            return this.paths.size();
        }

        protected int accessedNodes() {
            return this.sourcesAll.size() + this.targetsAll.size();
        }
    }
}
