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
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

import org.apache.commons.collections.CollectionUtils;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.eclipse.collections.api.iterator.IntIterator;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.collection.ObjectIntMapping;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class PathsTraverser extends HugeTraverser {

    public PathsTraverser(HugeGraph graph) {
        super(graph);
        this.idMapping = new ObjectIntMapping();
    }

    @Watched
    public PathSet paths(Id sourceV, Directions sourceDir,
                         Id targetV, Directions targetDir, String label,
                         int depth, long degree, long capacity, long limit) {
        E.checkNotNull(sourceV, "source vertex id");
        E.checkNotNull(targetV, "target vertex id");
        this.checkVertexExist(sourceV, "source vertex");
        this.checkVertexExist(targetV, "target vertex");
        E.checkNotNull(sourceDir, "source direction");
        E.checkNotNull(targetDir, "target direction");
        E.checkArgument(sourceDir == targetDir ||
                        sourceDir == targetDir.opposite(),
                        "Source direction must equal to target direction" +
                        " or opposite to target direction");
        checkPositive(depth, "max depth");
        checkDegree(degree);
        checkCapacity(capacity);
        checkLimit(limit);

        PathSet paths = new PathSet();
        if (sourceV.equals(targetV)) {
            return paths;
        }

        Id labelId = this.getEdgeLabelId(label);
        Traverser traverser = new Traverser(sourceV, targetV, labelId,
                                            degree, capacity, limit);
        while (true) {
            if (--depth < 0 || traverser.reachLimit()) {
                break;
            }
            traverser.forward(sourceDir);

            if (--depth < 0 || traverser.reachLimit()) {
                break;
            }
            traverser.backward(targetDir);
        }
        paths.addAll(traverser.paths());
        return paths;
    }

    private class Traverser {

        private Stack<IntObjectHashMap<IntHashSet>> sourceLayers;
        private Stack<IntObjectHashMap<IntHashSet>> targetLayers;

        private final Id label;
        private final long degree;
        private final long capacity;
        private final long limit;
        private PathSet paths;
        private int accessed;

        public Traverser(Id sourceV, Id targetV, Id label,
                         long degree, long capacity, long limit) {
            this.accessed = 2;
            this.label = label;
            this.degree = degree;
            this.capacity = capacity;
            this.limit = limit;
            this.paths = new PathSet();

            int sourceCode = this.code(sourceV);
            int targetCode = this.code(targetV);
            IntObjectHashMap<IntHashSet> firstSourceLayer =
                                         new IntObjectHashMap<>();
            IntObjectHashMap<IntHashSet> firstTargetLayer =
                                         new IntObjectHashMap<>();
            firstSourceLayer.put(sourceCode, new IntHashSet());
            firstTargetLayer.put(targetCode, new IntHashSet());
            this.sourceLayers = new Stack<>();
            this.targetLayers = new Stack<>();
            this.sourceLayers.push(firstSourceLayer);
            this.targetLayers.push(firstTargetLayer);
        }

        /**
         * Search forward from source
         */
        @Watched
        public void forward(Directions direction) {
            // Traversal vertices of previous level
            assert !this.sourceLayers.isEmpty();
            IntObjectHashMap<IntHashSet> sourceTopLayer =
                                         this.sourceLayers.peek();
            IntObjectHashMap<IntHashSet> newSourceLayer =
                                         new IntObjectHashMap<>();
            Iterator<Edge> edges;
            IntIterator keys = sourceTopLayer.keySet().intIterator();
            while (keys.hasNext()) {
                int id = keys.next();
                Id vid = this.id(id);
                edges = edgesOfVertex(vid, direction, this.label, this.degree);

                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id targetId = edge.id().otherVertexId();
                    int target = this.code(targetId);

                    // If cross point exists, path found, concat them
                    if (this.targetLayers.peek().containsKey(target)) {
                        List<Path> paths = this.getPath(id, target, false);
                        for (Path path : paths) {
                            this.paths.add(path);
                            if (this.reachLimit()) {
                                return;
                            }
                        }
                    }
                    this.add(newSourceLayer, target, id);
                }

            }
            this.accessed += newSourceLayer.size();
            this.sourceLayers.push(newSourceLayer);
        }

        /**
         * Search backward from target
         */
        @Watched
        public void backward(Directions direction) {
            assert !this.targetLayers.isEmpty();
            IntObjectHashMap<IntHashSet> targetTopLayer =
                                         this.targetLayers.peek();
            IntObjectHashMap<IntHashSet> newTargetLayer =
                                         new IntObjectHashMap<>();
            Iterator<Edge> edges;
            IntIterator keys = targetTopLayer.keySet().intIterator();
            while (keys.hasNext()) {
                int id = keys.next();
                Id vid = this.id(id);
                edges = edgesOfVertex(vid, direction, this.label, this.degree);

                while (edges.hasNext()) {
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id targetId = edge.id().otherVertexId();
                    int target = this.code(targetId);

                    // If cross point exists, path found, concat them
                    if (this.sourceLayers.peek().containsKey(target)) {
                        List<Path> paths = this.getPath(target, id, false);
                        for (Path path : paths) {
                            this.paths.add(path);
                            if (this.reachLimit()) {
                                return;
                            }
                        }
                    }

                    this.add(newTargetLayer, target, id);
                }
            }

            // Re-init targets
            this.accessed += newTargetLayer.size();
            this.targetLayers.push(newTargetLayer);
        }

        private boolean hasLoop(Stack<IntObjectHashMap<IntHashSet>> all,
                                int current, int target) {

            if (current == target) {
                return true;
            }
            int layers = all.size();
            IntHashSet keys = IntHashSet.newSetWith(current);
            for (int i = layers - 1; i > 0 ; i--) {
                IntObjectHashMap<IntHashSet> currentLayer =
                                             this.sourceLayers.elementAt(i);
                IntIterator iterator = keys.intIterator();
                IntHashSet parents = null;
                while (iterator.hasNext()) {
                    int key = iterator.next();
                    parents = currentLayer.get(key);
                    if (!parents.isEmpty() && parents.contains(target)) {
                        return true;
                    }
                }
                keys = parents;
            }
            return false;
        }

        @Watched
        private List<Path> getPath(int source, int target, boolean ring) {
            List<Path> results = new ArrayList<>();
            List<Path> sources = this.getSourcePath(source);
            List<Path> targets = this.getTargetPath(target);
            for (Path tpath : targets) {
                tpath.reverse();
                for (Path spath : sources) {
                    if (!ring) {
                        // Avoid loop in path
                        if (CollectionUtils.containsAny(spath.vertices(),
                                                        tpath.vertices())) {
                            continue;
                        }
                    }
                    List<Id> ids = new ArrayList<>(spath.vertices());
                    ids.addAll(tpath.vertices());
                    results.add(new Path(ids));
                }
            }
            return results;
        }

        private List<Path> getPath(Stack<IntObjectHashMap<IntHashSet>> all,
                                   int id, int layerIndex) {
            if (layerIndex == 0) {
                Id sid = this.id(id);
                return ImmutableList.of(new Path(Lists.newArrayList(sid)));
            }

            Id sid = this.id(id);
            List<Path> results = new ArrayList<>();
            IntObjectHashMap<IntHashSet> layer = all.elementAt(layerIndex);
            IntHashSet parents = layer.get(id);
            IntIterator iterator = parents.intIterator();
            while (iterator.hasNext()) {
                int parent = iterator.next();
                List<Path> paths = this.getPath(all, parent, layerIndex - 1);
                for (Iterator<Path> iter = paths.iterator(); iter.hasNext();) {
                    Path path = iter.next();
                    if (path.vertices().contains(sid)) {
                        iter.remove();
                        continue;
                    }
                    path.addToEnd(sid);
                }

                results.addAll(paths);
            }
            return results;
        }

        @Watched
        private List<Path> getSourcePath(int source) {
            return this.getPath(this.sourceLayers, source,
                                this.sourceLayers.size() - 1);
        }

        @Watched
        private List<Path> getTargetPath(int target) {
            return this.getPath(this.targetLayers, target,
                                this.targetLayers.size() - 1);
        }

        @Watched
        private void add(IntObjectHashMap<IntHashSet> layer,
                         int current, int parent) {

            if (layer.containsKey(current)) {
                layer.get(current).add(parent);
            } else {
                layer.put(current, IntHashSet.newSetWith(parent));
            }
        }

        public PathSet paths() {
            return this.paths;
        }

        private boolean reachLimit() {
            checkCapacity(this.capacity, this.accessed, "paths");
            if (this.limit == NO_LIMIT || this.paths.size() < this.limit) {
                return false;
            }
            return true;
        }

        private int code(Id id) {
            return PathsTraverser.this.code(id);
        }

        private Id id(int code) {
            return PathsTraverser.this.id(code);
        }
    }
}
