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
import java.util.function.BiConsumer;

import javax.ws.rs.core.MultivaluedMap;

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

    private abstract class Traverser {

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

        protected Map<Id, List<Node>> sources = this.newMultiValueMap();
        protected Map<Id, List<Node>> sourcesAll = this.newMultiValueMap();
        protected Map<Id, List<Node>> targets = this.newMultiValueMap();
        protected Map<Id, List<Node>> targetsAll = this.newMultiValueMap();

        protected Map<Id, List<Node>> newVertices;

        private Set<Path> paths;

        public Traverser(Collection<Id> sources, Collection<Id> targets,
                         List<RepeatEdgeStep> steps, boolean withRing,
                         long capacity, long limit) {
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

            for (Id id : sources) {
                this.addNode(this.sources, id, new Node(id));
            }
            for (Id id : targets) {
                this.addNode(this.targets, id, new Node(id));
            }
            this.sourcesAll.putAll(this.sources);
            this.targetsAll.putAll(this.targets);

            this.paths = this.newPathSet();
        }

        public void forward() {
            RepeatEdgeStep currentStep = this.nextStep(true);
            if (currentStep == null) {
                return;
            }

            this.beforeTraverse(true);

            // Traversal vertices of previous level
            traverseOneLayer(this.sources, currentStep, this::forward);

            this.afterTraverse(currentStep, true);
        }

        public void backward() {
            RepeatEdgeStep currentStep = this.nextStep(false);
            if (currentStep == null) {
                return;
            }

            this.beforeTraverse(false);

            currentStep.swithDirection();
            // Traversal vertices of previous level
            traverseOneLayer(this.targets, currentStep, this::backward);
            currentStep.swithDirection();

            this.afterTraverse(currentStep, false);
        }

        public RepeatEdgeStep nextStep(boolean forward) {
            return forward ? this.forwardStep() : this.backwardStep();
        }

        public void beforeTraverse(boolean forward) {
            this.clearNewVertices();
            this.reInitAllIfNeeded(forward);
        }

        public abstract void traverseOneLayer(
                             Map<Id, List<Node>> vertices,
                             RepeatEdgeStep step,
                             BiConsumer<Id, RepeatEdgeStep> consumer);

        public void afterTraverse(RepeatEdgeStep step, boolean forward) {
            Map<Id, List<Node>> all = forward ? this.sourcesAll :
                                                this.targetsAll;
            this.addNewVerticesToAll(all);
            this.reInitCurrentStepIfNeeded(step, forward);
            this.stepCount++;
        }

        private void forward(Id v, RepeatEdgeStep step) {
            this.traverseOne(v, step, true);
        }

        private void backward(Id v, RepeatEdgeStep step) {
            this.traverseOne(v, step, false);
        }

        private void traverseOne(Id source, RepeatEdgeStep step,
                                 boolean forward) {
            if (this.reachLimit()) {
                return;
            }

            Iterator<Edge> edges = edgesOfVertex(source, step);
            while (edges.hasNext()) {
                HugeEdge edge = (HugeEdge) edges.next();
                Id target = edge.id().otherVertexId();

                this.processOne(source, target, forward);
            }
        }

        private void processOne(Id source, Id target, boolean forward) {
            if (forward) {
                processOneForForward(source, target);
            } else {
                processOneForBackward(source, target);
            }
        }

        private void processOneForForward(Id sourceV, Id targetV) {
            for (Node source : this.sources.get(sourceV)) {
                // If have loop, skip target
                if (!this.withRing && source.contains(targetV)) {
                    continue;
                }

                // If cross point exists, path found, concat them
                if (this.lastSuperStep() &&
                    this.targetsAll.containsKey(targetV)) {
                    for (Node target : this.targetsAll.get(targetV)) {
                        List<Id> path = joinPath(source, target, this.withRing);
                        if (!path.isEmpty()) {
                            this.paths.add(new Path(targetV, path));
                            if (this.reachLimit()) {
                                return;
                            }
                        }
                    }
                }

                // Add node to next start-nodes
                this.addNodeToNewVertices(targetV, new Node(targetV, source));
            }
        }

        private void processOneForBackward(Id sourceV, Id targetV) {
            for (Node source : this.targets.get(sourceV)) {
                // If have loop, skip target
                if (!this.withRing && source.contains(targetV)) {
                    continue;
                }

                // If cross point exists, path found, concat them
                if (this.lastSuperStep() &&
                    this.sourcesAll.containsKey(targetV)) {
                    for (Node target : this.sourcesAll.get(targetV)) {
                        List<Id> path = joinPath(source, target, this.withRing);
                        if (!path.isEmpty()) {
                            Path newPath = new Path(targetV, path);
                            newPath.reverse();
                            this.paths.add(newPath);
                            if (this.reachLimit()) {
                                return;
                            }
                        }
                    }
                }

                // Add node to next start-nodes
                this.addNodeToNewVertices(targetV, new Node(targetV, source));
            }
        }

        private void reInitAllIfNeeded(boolean forward) {
            if (forward) {
                /*
                 * Re-init source all if last forward finished one super step
                 * and current step is not last super step
                 */
                if (this.sourceFinishOneStep && !this.lastSuperStep()) {
                    this.sourcesAll = this.newMultiValueMap();
                    this.sourceFinishOneStep = false;
                }
            } else {
                /*
                 * Re-init target all if last forward finished one super step
                 * and current step is not last super step
                 */
                if (this.targetFinishOneStep && !this.lastSuperStep()) {
                    this.targetsAll = this.newMultiValueMap();
                    this.targetFinishOneStep = false;
                }
            }
        }

        private void reInitCurrentStepIfNeeded(RepeatEdgeStep step,
                                               boolean forward) {
            step.decreaseTimes();
            if (forward) {
                // Re-init sources
                if (step.remainTimes() > 0) {
                    this.sources = this.newVertices;
                } else {
                    this.sources = this.sourcesAll;
                    this.sourceFinishOneStep = true;
                }
            } else {
                // Re-init targets
                if (step.remainTimes() > 0) {
                    this.targets = this.newVertices;
                } else {
                    this.targets = this.targetsAll;
                    this.targetFinishOneStep = true;
                }
            }
        }

        public RepeatEdgeStep forwardStep() {
            RepeatEdgeStep currentStep = null;
            // Find next step to backward
            for (int i = 0; i < this.steps.size(); i++) {
                RepeatEdgeStep step = this.steps.get(i);
                if (step.remainTimes() > 0) {
                    currentStep = step;
                    this.targetIndex = i;
                    break;
                }
            }
            return currentStep;
        }

        public RepeatEdgeStep backwardStep() {
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
            return currentStep;
        }

        public boolean lastSuperStep() {
            return this.targetIndex == this.sourceIndex ||
                   this.targetIndex == this.sourceIndex + 1;
        }

        public void clearNewVertices() {
            this.newVertices = this.newMultiValueMap();
        }

        public void addNodeToNewVertices(Id id, Node node) {
            this.addNode(this.newVertices, id, node);
        }

        public abstract Map<Id, List<Node>> newMultiValueMap();

        public abstract Set<Path> newPathSet();

        public abstract void addNode(Map<Id, List<Node>> vertices,
                                     Id id, Node node);

        public abstract void addNewVerticesToAll(Map<Id, List<Node>> targets);

        public Set<Path> paths() {
            return this.paths;
        }

        public int pathCount() {
            return this.paths.size();
        }

        protected boolean finish() {
            return this.stepCount >= this.totalSteps || this.reachLimit();
        }

        protected boolean reachLimit() {
            checkCapacity(this.capacity, this.accessedNodes(),
                          "template paths");
            if (this.limit == NO_LIMIT || this.pathCount() < this.limit) {
                return false;
            }
            return true;
        }

        private int accessedNodes() {
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

    private class ConcurrentTraverser extends Traverser {

        public ConcurrentTraverser(Collection<Id> sources,
                                   Collection<Id> targets,
                                   List<RepeatEdgeStep> steps,
                                   boolean withRing,
                                   long capacity, long limit) {
            super(sources, targets, steps, withRing, capacity, limit);
        }

        @Override
        public Map<Id, List<Node>> newMultiValueMap() {
            return new ConcurrentMultiValuedMap<>();
        }

        @Override
        public void traverseOneLayer(
                    Map<Id, List<Node>> vertices, RepeatEdgeStep step,
                    BiConsumer<Id, RepeatEdgeStep> consumer) {
            traverseIds(this.sources.keySet().iterator(), id -> {
                consumer.accept(id, step);
            });
        }

        @Override
        public Set<Path> newPathSet() {
            return ConcurrentHashMap.newKeySet();
        }

        @Override
        public void addNode(Map<Id, List<Node>> vertices, Id id, Node node) {
            ((ConcurrentMultiValuedMap<Id, Node>) vertices).add(id, node);
        }

        @Override
        public void addNewVerticesToAll(Map<Id, List<Node>> targets) {
            ConcurrentMultiValuedMap<Id, Node> vertices =
                    (ConcurrentMultiValuedMap<Id, Node>) targets;
            for (Map.Entry<Id, List<Node>> entry : this.newVertices.entrySet()) {
                vertices.addAll(entry.getKey(), entry.getValue());
            }
        }
    }

    private class SingleTraverser extends Traverser {

        public SingleTraverser(Collection<Id> sources, Collection<Id> targets,
                               List<RepeatEdgeStep> steps, boolean withRing,
                               long capacity, long limit) {
            super(sources, targets, steps, withRing, capacity, limit);
        }

        @Override
        public Map<Id, List<Node>> newMultiValueMap() {
            return newMultivalueMap();
        }

        @Override
        public Set<Path> newPathSet() {
            return new PathSet();
        }

        @Override
        public void traverseOneLayer(
                    Map<Id, List<Node>> vertices, RepeatEdgeStep step,
                    BiConsumer<Id, RepeatEdgeStep> consumer) {
            for (Id id : vertices.keySet()) {
                consumer.accept(id, step);
            }
        }

        @Override
        public void addNode(Map<Id, List<Node>> vertices, Id id, Node node) {
            ((MultivaluedMap<Id, Node>) vertices).add(id, node);
        }

        @Override
        public void addNewVerticesToAll(Map<Id, List<Node>> targets) {
            MultivaluedMap<Id, Node> vertices =
                                     (MultivaluedMap<Id, Node>) targets;
            for (Map.Entry<Id, List<Node>> entry : this.newVertices.entrySet()) {
                vertices.addAll(entry.getKey(), entry.getValue());
            }
        }
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
