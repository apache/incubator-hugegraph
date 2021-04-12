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

package com.baidu.hugegraph.traversal.algorithm.records;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;
import java.util.function.Function;

import org.apache.commons.collections.CollectionUtils;
import org.eclipse.collections.api.iterator.IntIterator;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser.PathSet;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser.Path;
import com.baidu.hugegraph.util.collection.ObjectIntMapping;
import com.google.common.collect.Lists;

public class PathsRecords implements Records {

    private final ObjectIntMapping idMapping;

    private final Stack<IntObjectHashMap<IntHashSet>> sourceLayers;
    private final Stack<IntObjectHashMap<IntHashSet>> targetLayers;

    private IntObjectHashMap<IntHashSet> currentLayer;
    private IntIterator lastLayerKeys;
    private int current;
    private boolean forward;

    private int accessed;

    public PathsRecords(Id sourceV, Id targetV) {

        this.idMapping = new ObjectIntMapping();

        int sourceCode = this.code(sourceV);
        int targetCode = this.code(targetV);
        IntObjectHashMap<IntHashSet> firstSourceLayer = new IntObjectHashMap<>();
        IntObjectHashMap<IntHashSet> firstTargetLayer = new IntObjectHashMap<>();
        firstSourceLayer.put(sourceCode, new IntHashSet());
        firstTargetLayer.put(targetCode, new IntHashSet());
        this.sourceLayers = new Stack<>();
        this.targetLayers = new Stack<>();
        this.sourceLayers.push(firstSourceLayer);
        this.targetLayers.push(firstTargetLayer);

        this.accessed = 2;
    }

    public void startOneLayer(boolean forward) {
        this.forward = forward;
        this.currentLayer = new IntObjectHashMap<>();
        this.lastLayerKeys = this.forward ?
                        this.sourceLayers.peek().keySet().intIterator() :
                        this.targetLayers.peek().keySet().intIterator();
    }

    public void finishOneLayer() {
        if (this.forward) {
            this.sourceLayers.push(this.currentLayer);
        } else {
            this.targetLayers.push(this.currentLayer);
        }
        this.accessed += this.currentLayer.size();
    }

    @Watched
    public boolean hasNextKey() {
        return this.lastLayerKeys.hasNext();
    }

    @Watched
    public Id nextKey() {
        this.current = this.lastLayerKeys.next();
        return this.id(current);
    }

    @Watched
    public PathSet findPath(Id target, Function<Id, Boolean> filter,
                            boolean all, boolean ring) {
        assert all;
        PathSet results = new PathSet();
        int targetCode = this.code(target);
        // If cross point exists, path found, concat them
        if (this.contains(targetCode)) {
            results = this.forward ?
                      this.concatPath(this.current, targetCode, ring) :
                      this.concatPath(targetCode, this.current, ring);
        }
        this.addPath(targetCode, this.current);
        return results;
    }

    public long accessed() {
        return this.accessed;
    }

    private boolean contains(int node) {
        return this.forward ? this.targetContains(node) :
                              this.sourceContains(node);
    }

    private boolean sourceContains(int node) {
        return this.sourceLayers.peek().containsKey(node);
    }

    private boolean targetContains(int node) {
        return this.targetLayers.peek().containsKey(node);
    }

    @Watched
    private PathSet concatPath(int source, int target, boolean ring) {
        PathSet results = new PathSet();
        PathSet sources = this.getSourcePath(source);
        PathSet targets = this.getTargetPath(target);
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

    private PathSet concatPath(Stack<IntObjectHashMap<IntHashSet>> all,
                               int id, int layerIndex) {
        PathSet results = new PathSet();
        if (layerIndex == 0) {
            Id sid = this.id(id);
            results.add(new Path(Lists.newArrayList(sid)));
            return results;
        }

        Id sid = this.id(id);
        IntObjectHashMap<IntHashSet> layer = all.elementAt(layerIndex);
        IntHashSet parents = layer.get(id);
        IntIterator iterator = parents.intIterator();
        while (iterator.hasNext()) {
            int parent = iterator.next();
            PathSet paths = this.concatPath(all, parent, layerIndex - 1);
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

    private PathSet getSourcePath(int source) {
        return this.concatPath(this.sourceLayers, source,
                               this.sourceLayers.size() - 1);
    }

    private PathSet getTargetPath(int target) {
        return this.concatPath(this.targetLayers, target,
                               this.targetLayers.size() - 1);
    }

    @Watched
    private void addPath(int current, int parent) {
        if (this.currentLayer.containsKey(current)) {
            this.currentLayer.get(current).add(parent);
        } else {
            this.currentLayer.put(current, IntHashSet.newSetWith(parent));
        }
    }

    @Watched
    private int code(Id id) {
        return this.idMapping.object2Code(id);
    }

    @Watched
    private Id id(int code) {
        return (Id) this.idMapping.code2Object(code);
    }
}
