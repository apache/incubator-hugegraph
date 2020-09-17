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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.ImmutableList;

public class TemplatePathsTraverser extends TpTraverser {

    private static final Logger LOG = Log.logger(TemplatePathsTraverser.class);

    public TemplatePathsTraverser(HugeGraph graph) {
        super(graph, "template-paths");
    }

    @SuppressWarnings("unchecked")
    public Set<Path> templatePaths(Iterator<Vertex> sources,
                                   Iterator<Vertex> targets,
                                   List<RepeatEdgeStep> steps,
                                   boolean withRing,
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

        int totalSteps = 0;
        for (RepeatEdgeStep step : steps) {
            totalSteps += step.maxTimes;
        }
        Traverser traverser = totalSteps >= this.concurrentDepth() ?
                              new ConcurrentTraverser(sourceList, targetList,
                                                      steps, withRing,
                                                      capacity, limit) :
                              new SingleTraverser(sourceList, targetList,
                                                  steps, withRing,
                                                  capacity, limit);

        do {
            // Forward
            traverser.forward();
            if (traverser.finish()) {
                return traverser.paths();
            }

            // Backward
            traverser.backward();
            if (traverser.finish()) {
                return traverser.paths();
            }
        } while (true);
    }

    private class Traverser {

        protected final List<RepeatEdgeStep> steps;
        protected int stepCount;
        protected final long capacity;
        protected final long limit;
        protected int totalSteps;
        protected boolean withRing;
        protected int sourceIndex;
        protected int targetIndex;

        protected boolean sourceFinishOneStep = false;
        protected boolean targetFinishOneStep = false;

        public Traverser(List<RepeatEdgeStep> steps,
                         long capacity, long limit, boolean withRing) {
            this.steps = steps;
            this.capacity = capacity;
            this.limit = limit;
            this.withRing = withRing;

            this.stepCount = 0;
            for (RepeatEdgeStep step : steps) {
                this.totalSteps += step.maxTimes;
            }
            this.sourceIndex = 0;
            this.targetIndex = this.steps.size() - 1;
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

        protected boolean finish() {
            return this.stepCount >= this.totalSteps || this.reachLimit();
        }

        protected boolean lastStep() {
            return this.stepCount == this.totalSteps - 1;
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

        public boolean lastSuperStep() {
            return this.targetIndex == this.sourceIndex ||
                   this.targetIndex == this.sourceIndex + 1;
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
                                   Collection<Id> targets,
                                   List<RepeatEdgeStep> steps, boolean withRing,
                                   long capacity, long limit) {
            super(steps, capacity, limit, withRing);
            for (Id id : sources) {
                this.sources.add(id, new Node(id));
            }
            for (Id id : targets) {
                this.targets.add(id, new Node(id));
            }
            this.paths = ConcurrentHashMap.newKeySet();
        }

        /**
         * Search forward from sources
         */
        public void forward() {
            ConcurrentMultiValuedMap<Id, Node> newVertices =
                                               new ConcurrentMultiValuedMap<>();
            RepeatEdgeStep currentStep = null;
            // Find next step to forward
            for (int i = 0; i < this.steps.size(); i++) {
                RepeatEdgeStep step = this.steps.get(i);
                if (step.remainTimes() > 0) {
                    currentStep = step;
                    this.sourceIndex = i;
                    break;
                }
            }
            if (currentStep == null) {
                return;
            }

            // Re-init source all if last forward finished one super step and
            // not last super step
            if (this.sourceFinishOneStep && !this.lastSuperStep()) {
                this.sourcesAll = new ConcurrentMultiValuedMap<>();
                this.sourceFinishOneStep = false;
            }

            // Traversal vertices of previous level
            RepeatEdgeStep finalCurrentStep = currentStep;
            traverseIds(this.sources.keySet().iterator(), vid -> {
                if (this.reachLimit()) {
                    return;
                }
                Iterator<Edge> edges = edgesOfVertex(vid, finalCurrentStep);
                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    for (Node n : this.sources.get(vid)) {
                        // If have loop, skip target
                        if (!this.withRing && n.contains(target)) {
                            continue;
                        }

                        // If cross point exists, path found, concat them
                        if (this.lastSuperStep() &&
                            this.targets.containsKey(target)) {
                            for (Node node : this.targets.get(target)) {
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

            for (Map.Entry<Id, List<Node>> entry : newVertices.entrySet()) {
                this.sourcesAll.addAll(entry.getKey(), entry.getValue());
            }

            currentStep.decreaseTimes();
            // Re-init sources
            if (currentStep.remainTimes() > 0) {
                this.sources = newVertices;
            } else {
                this.sources = this.sourcesAll;
                this.sourceFinishOneStep = true;
            }
            this.stepCount++;
        }

        /**
         * Search backward from target
         */
        public void backward() {
            ConcurrentMultiValuedMap<Id, Node> newVertices =
                                               new ConcurrentMultiValuedMap<>();
            RepeatEdgeStep currentStep = null;

            // Find next step to backward
            for (int i = this.steps.size() - 1; i >= 0; i--) {
                RepeatEdgeStep step = this.steps.get(i);
                if (step.remainTimes() > 0) {
                    currentStep = step;
                    this.targetIndex = i;
                    break;
                }
            }
            if (currentStep == null) {
                return;
            }

            // Re-init target all if last forward finished one super step and
            // not last super step
            if (this.targetFinishOneStep && !this.lastSuperStep()) {
                this.targetsAll = new ConcurrentMultiValuedMap<>();
                this.targetFinishOneStep = false;
            }

            currentStep.swithDirection();
            // Traversal vertices of previous level
            RepeatEdgeStep finalCurrentStep = currentStep;
            traverseIds(this.targets.keySet().iterator(), vid -> {
                if (this.reachLimit()) {
                    return;
                }
                Iterator<Edge> edges = edgesOfVertex(vid, finalCurrentStep);

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

            currentStep.swithDirection();

            for (Map.Entry<Id, List<Node>> entry : newVertices.entrySet()) {
                this.targetsAll.addAll(entry.getKey(), entry.getValue());
            }

            currentStep.decreaseTimes();
            // Re-init targets
            if (currentStep.remainTimes() > 0) {
                this.targets = newVertices;
            } else {
                this.targets = this.targetsAll;
                this.targetFinishOneStep = true;
            }
            this.stepCount++;
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
            int size = 0;
            for (List<Node> value : this.sourcesAll.values()) {
                size += value.size();
            }
            for (List<Node> value : this.targetsAll.values()) {
                size += value.size();
            }
            return size;
        }
    }

    private class SingleTraverser extends Traverser {

        private MultivaluedMap<Id, Node> sources = newMultivalueMap();
        private MultivaluedMap<Id, Node> targets = newMultivalueMap();
        private MultivaluedMap<Id, Node> sourcesAll = newMultivalueMap();
        private MultivaluedMap<Id, Node> targetsAll = newMultivalueMap();

        private Set<Path> paths;

        public SingleTraverser(Collection<Id> sources, Collection<Id> targets,
                               List<RepeatEdgeStep> steps, boolean withRing,
                               long capacity, long limit) {
            super(steps, capacity, limit, withRing);
            for (Id id : sources) {
                this.sources.add(id, new Node(id));
            }
            for (Id id : targets) {
                this.targets.add(id, new Node(id));
            }
            this.paths = new PathSet();
        }

        /**
         * Search forward from sources
         */
        public void forward() {
            MultivaluedMap<Id, Node> newVertices = newMultivalueMap();
            RepeatEdgeStep currentStep = null;
            // Find next step to forward
            for (int i = 0; i < this.steps.size(); i++) {
                RepeatEdgeStep step = this.steps.get(i);
                if (step.remainTimes() > 0) {
                    currentStep = step;
                    this.sourceIndex = i;
                    break;
                }
            }
            if (currentStep == null) {
                return;
            }

            // Re-init source all if last forward finished one super step and
            // not last super step
            if (this.sourceFinishOneStep && !this.lastSuperStep()) {
                this.sourcesAll = newMultivalueMap();
                this.sourceFinishOneStep = false;
            }

            Iterator<Edge> edges;
            // Traversal vertices of previous level
            for (Map.Entry<Id, List<Node>> entry : this.sources.entrySet()) {
                Id vid = entry.getKey();
                edges = edgesOfVertex(vid, currentStep);

                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    for (Node n : entry.getValue()) {
                        // If have loop, skip target
                        if (!this.withRing && n.contains(target)) {
                            continue;
                        }

                        // If cross point exists, path found, concat them
                        if (this.lastSuperStep() &&
                            this.targetsAll.containsKey(target)) {
                            for (Node node : this.targetsAll.get(target)) {
                                List<Id> path = joinPath(n, node, withRing);
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

            for (Map.Entry<Id, List<Node>> entry : newVertices.entrySet()) {
                this.sourcesAll.addAll(entry.getKey(), entry.getValue());
            }

            currentStep.decreaseTimes();
            // Re-init sources
            if (currentStep.remainTimes() > 0) {
                this.sources = newVertices;
            } else {
                this.sources = this.sourcesAll;
                this.sourceFinishOneStep = true;
            }
            this.stepCount++;
        }

        /**
         * Search backward from target
         */
        public void backward() {
            MultivaluedMap<Id, Node> newVertices = newMultivalueMap();
            RepeatEdgeStep currentStep = null;

            // Find next step to backward
            for (int i = this.steps.size() - 1; i >= 0; i--) {
                RepeatEdgeStep step = this.steps.get(i);
                if (step.remainTimes() > 0) {
                    currentStep = step;
                    this.targetIndex = i;
                    break;
                }
            }
            if (currentStep == null) {
                return;
            }

            // Re-init target all if last forward finished one super step and
            // not last super step
            if (this.targetFinishOneStep && !this.lastSuperStep()) {
                this.targetsAll = newMultivalueMap();
                this.targetFinishOneStep = false;
            }

            currentStep.swithDirection();
            Iterator<Edge> edges;
            // Traversal vertices of previous level
            for (Map.Entry<Id, List<Node>> entry : this.targets.entrySet()) {
                Id vid = entry.getKey();
                edges = edgesOfVertex(vid, currentStep);

                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();

                    for (Node n : entry.getValue()) {
                        // If have loop, skip target
                        if (!this.withRing && n.contains(target)) {
                            continue;
                        }

                        // If cross point exists, path found, concat them
                        if (this.lastSuperStep() &&
                            this.sourcesAll.containsKey(target)) {
                            for (Node node : this.sourcesAll.get(target)) {
                                List<Id> path = joinPath(n, node, withRing);
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

            currentStep.swithDirection();

            for (Map.Entry<Id, List<Node>> entry : newVertices.entrySet()) {
                this.targetsAll.addAll(entry.getKey(), entry.getValue());
            }

            currentStep.decreaseTimes();
            // Re-init targets
            if (currentStep.remainTimes() > 0) {
                this.targets = newVertices;
            } else {
                this.targets = this.targetsAll;
                this.targetFinishOneStep = true;
            }
            this.stepCount++;
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
            int size = 0;
            for (List<Node> value : this.sourcesAll.values()) {
                size += value.size();
            }
            for (List<Node> value : this.targetsAll.values()) {
                size += value.size();
            }
            return size;
        }
    }

    public static List<Id> joinPath(Node pre, Node back, boolean ring) {
        // Get self path
        List<Id> path = pre.path();

        // Get reversed other path
        List<Id> backPath = back.path();
        Collections.reverse(backPath);

        if (!ring) {
            // Avoid loop in path
            if (CollectionUtils.containsAny(path, backPath)) {
                return ImmutableList.of();
            }
        }

        // Append other path behind self path
        path.addAll(backPath);
        return path;
    }

    public static class RepeatEdgeStep extends EdgeStep {

        private int maxTimes = 1;

        public RepeatEdgeStep(HugeGraph g, Directions direction,
                              List<String> labels,
                              Map<String, Object> properties, long degree,
                              long skipDegree, int maxTimes) {
            super(g, direction, labels, properties, degree, skipDegree);
            this.maxTimes = maxTimes;
        }

        private int remainTimes() {
            return this.maxTimes;
        }

        private void decreaseTimes() {
            this.maxTimes--;
        }
    }
}
