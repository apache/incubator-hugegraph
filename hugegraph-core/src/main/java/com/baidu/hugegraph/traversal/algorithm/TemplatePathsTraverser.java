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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.core.MultivaluedMap;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.util.E;

public class TemplatePathsTraverser extends TpTraverser {

    public TemplatePathsTraverser(HugeGraph graph) {
        super(graph, "template-paths");
    }

    @SuppressWarnings("unchecked")
    public Set<Path> templatePaths(Iterator<Vertex> sources,
                                   Iterator<Vertex> targets,
                                   List<EdgeStep> steps,
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

        Traverser traverser = steps.size() > 20 ?
                              new ConcurrentTraverser(sourceList, targetList,
                                                      steps, capacity, limit) :
                              new SingleTraverser(sourceList, targetList,
                                                  steps, capacity, limit);
        Set<Path> paths;
        do {
            // Forward
            paths = traverser.forward();
            if (traverser.finish()) {
                return paths;
            }

            // Backward
            paths = traverser.backward();
            if (traverser.finish()) {
                for (Path path : paths) {
                    path.reverse();
                }
                return paths;
            }
        } while (true);
    }

    private class Traverser {

        protected final List<EdgeStep> steps;
        protected int stepCount;
        protected final long capacity;
        protected final long limit;

        public Traverser(List<EdgeStep> steps, long capacity, long limit) {
            this.steps = steps;
            this.capacity = capacity;
            this.limit = limit;

            this.stepCount = 0;
        }

        public Set<Path> forward() {
            return new PathSet();
        }

        public Set<Path> backward() {
            return new PathSet();
        }

        public int pathCount() {
            return 0;
        }

        protected boolean finish() {
            return this.stepCount == this.steps.size();
        }

        protected boolean lastStep() {
            return this.stepCount == this.steps.size() - 1;
        }

        protected int accessedNodes() {
            return 0;
        }

        protected boolean reachLimit() {
            checkCapacity(this.capacity, this.accessedNodes(),
                          "template paths");
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

        protected AtomicInteger pathCount;

        public ConcurrentTraverser(Collection<Id> sources,
                                   Collection<Id> targets, List<EdgeStep> steps,
                                   long capacity, long limit) {
            super(steps, capacity, limit);
            for (Id id : sources) {
                this.sources.add(id, new Node(id));
            }
            for (Id id : targets) {
                this.targets.add(id, new Node(id));
            }
            this.pathCount = new AtomicInteger(0);
        }

        /**
         * Search forward from sources
         */
        public Set<Path> forward() {
            Set<Path> paths = ConcurrentHashMap.newKeySet();
            ConcurrentMultiValuedMap<Id, Node> newVertices =
                                               new ConcurrentMultiValuedMap<>();
            EdgeStep step = this.steps.get(this.stepCount / 2);
            // Traversal vertices of previous level
            traverseIds(this.sources.keySet().iterator(), vid -> {
                if (this.reachLimit()) {
                    return;
                }
                Iterator<Edge> edges = edgesOfVertex(vid, step);
                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    for (Node n : this.sources.get(vid)) {
                        // If have loop, skip target
                        if (n.contains(target)) {
                            continue;
                        }

                        // If cross point exists, path found, concat them
                        if (this.lastStep() &&
                            this.targets.containsKey(target)) {
                            for (Node node : this.targets.get(target)) {
                                List<Id> path = n.joinPath(node);
                                if (!path.isEmpty()) {
                                    paths.add(new Path(target, path));
                                    this.pathCount.incrementAndGet();
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

            this.stepCount++;

            return paths;
        }

        /**
         * Search backward from target
         */
        public Set<Path> backward() {
            Set<Path> paths = ConcurrentHashMap.newKeySet();
            ConcurrentMultiValuedMap<Id, Node> newVertices =
                                               new ConcurrentMultiValuedMap<>();
            int index = this.steps.size() - stepCount / 2 - 1;
            EdgeStep step = this.steps.get(index);
            step.swithDirection();
            // Traversal vertices of previous level
            traverseIds(this.targets.keySet().iterator(), vid -> {
                if (this.reachLimit()) {
                    return;
                }
                Iterator<Edge> edges = edgesOfVertex(vid, step);

                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    for (Node n : this.targets.get(vid)) {
                        // If have loop, skip target
                        if (n.contains(target)) {
                            continue;
                        }

                        // If cross point exists, path found, concat them
                        if (this.lastStep() &&
                            this.sources.containsKey(target)) {
                            for (Node node : this.sources.get(target)) {
                                List<Id> path = n.joinPath(node);
                                if (!path.isEmpty()) {
                                    paths.add(new Path(target, path));
                                    this.pathCount.incrementAndGet();
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

            // Re-init targets
            this.targets = newVertices;

            this.stepCount++;

            return paths;
        }

        @Override
        public int pathCount() {
            return this.pathCount.get();
        }

        protected int accessedNodes() {
            return this.sources.size() + this.targets.size();
        }
    }

    private class SingleTraverser extends Traverser {

        private MultivaluedMap<Id, Node> sources = newMultivalueMap();
        private MultivaluedMap<Id, Node> targets = newMultivalueMap();

        private int pathCount;

        public SingleTraverser(Collection<Id> sources, Collection<Id> targets,
                               List<EdgeStep> steps, long capacity,
                               long limit) {
            super(steps, capacity, limit);
            for (Id id : sources) {
                this.sources.add(id, new Node(id));
            }
            for (Id id : targets) {
                this.targets.add(id, new Node(id));
            }
            this.pathCount = 0;
        }

        /**
         * Search forward from sources
         */
        public PathSet forward() {
            PathSet paths = new PathSet();
            MultivaluedMap<Id, Node> newVertices = newMultivalueMap();
            EdgeStep step = this.steps.get(this.stepCount / 2);
            Iterator<Edge> edges;
            // Traversal vertices of previous level
            for (Map.Entry<Id, List<Node>> entry : this.sources.entrySet()) {
                Id vid = entry.getKey();
                edges = edgesOfVertex(vid, step);

                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    for (Node n : entry.getValue()) {
                        // If have loop, skip target
                        if (n.contains(target)) {
                            continue;
                        }

                        // If cross point exists, path found, concat them
                        if (this.lastStep() &&
                            this.targets.containsKey(target)) {
                            for (Node node : this.targets.get(target)) {
                                List<Id> path = n.joinPath(node);
                                if (!path.isEmpty()) {
                                    paths.add(new Path(target, path));
                                    ++this.pathCount;
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

            this.stepCount++;

            return paths;
        }

        /**
         * Search backward from target
         */
        public PathSet backward() {
            PathSet paths = new PathSet();
            MultivaluedMap<Id, Node> newVertices = newMultivalueMap();
            int index = this.steps.size() - stepCount / 2 - 1;
            EdgeStep step = this.steps.get(index);
            step.swithDirection();
            Iterator<Edge> edges;
            // Traversal vertices of previous level
            for (Map.Entry<Id, List<Node>> entry : this.targets.entrySet()) {
                Id vid = entry.getKey();
                edges = edgesOfVertex(vid, step);

                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    for (Node n : entry.getValue()) {
                        // If have loop, skip target
                        if (n.contains(target)) {
                            continue;
                        }

                        // If cross point exists, path found, concat them
                        if (this.lastStep() &&
                            this.sources.containsKey(target)) {
                            for (Node node : this.sources.get(target)) {
                                List<Id> path = n.joinPath(node);
                                if (!path.isEmpty()) {
                                    paths.add(new Path(target, path));
                                    ++this.pathCount;
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

            this.stepCount++;

            return paths;
        }

        @Override
        public int pathCount() {
            return this.pathCount;
        }

        protected int accessedNodes() {
            return this.sources.size() + this.targets.size();
        }
    }
}
