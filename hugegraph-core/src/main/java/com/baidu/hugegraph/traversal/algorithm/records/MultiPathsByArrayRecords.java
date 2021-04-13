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

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser.Path;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser.PathSet;
import com.baidu.hugegraph.util.collection.Int2IntsMap;
import com.baidu.hugegraph.util.collection.ObjectIntMapping;
import com.google.common.collect.Lists;

public class MultiPathsByArrayRecords implements Records {

    private final ObjectIntMapping idMapping;

    private final Stack<Int2IntsMap> sourceLayers;
    private final Stack<Int2IntsMap> targetLayers;

    private Int2IntsMap currentLayer;
    private IntIterator lastLayerKeys;
    private int current;
    private boolean forward;

    private int accessed;

    public MultiPathsByArrayRecords(Id sourceV, Id targetV) {
        this.idMapping = new ObjectIntMapping();

        int sourceCode = this.code(sourceV);
        int targetCode = this.code(targetV);
        Int2IntsMap firstSourceLayer = new Int2IntsMap();
        Int2IntsMap firstTargetLayer = new Int2IntsMap();
        firstSourceLayer.add(sourceCode, 0);
        firstTargetLayer.add(targetCode, 0);
        this.sourceLayers = new Stack<>();
        this.targetLayers = new Stack<>();
        this.sourceLayers.push(firstSourceLayer);
        this.targetLayers.push(firstTargetLayer);

        this.accessed = 2;
    }

    public void startOneLayer(boolean forward) {
        this.forward = forward;
        this.currentLayer = new Int2IntsMap();
        this.lastLayerKeys = this.forward ?
                             this.sourceLayers.peek().keyIterator() :
                             this.targetLayers.peek().keyIterator();
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
                      this.linkPath(this.current, targetCode, ring) :
                      this.linkPath(targetCode, this.current, ring);
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
    private PathSet linkPath(int source, int target, boolean ring) {
        PathSet results = new PathSet();
        PathSet sources = this.linkSourcePath(source);
        PathSet targets = this.linkTargetPath(target);
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

    private PathSet linkSourcePath(int source) {
        return this.linkPath(this.sourceLayers, source,
                             this.sourceLayers.size() - 1);
    }

    private PathSet linkTargetPath(int target) {
        return this.linkPath(this.targetLayers, target,
                             this.targetLayers.size() - 1);
    }

    private PathSet linkPath(Stack<Int2IntsMap> all, int id, int layerIndex) {
        PathSet results = new PathSet();
        if (layerIndex == 0) {
            Id sid = this.id(id);
            results.add(new Path(Lists.newArrayList(sid)));
            return results;
        }

        Id sid = this.id(id);
        Int2IntsMap layer = all.elementAt(layerIndex);
        int[] parents = layer.get(id);
        for (int parent : parents) {
            PathSet paths = this.linkPath(all, parent, layerIndex - 1);
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
    private void addPath(int current, int parent) {
        this.currentLayer.add(current, parent);
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
