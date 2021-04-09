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
import java.util.Collections;
import java.util.List;
import java.util.Stack;
import java.util.function.Function;

import org.eclipse.collections.api.iterator.IntIterator;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser.PathSet;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser.Path;
import com.baidu.hugegraph.util.collection.ObjectIntMapping;

public class ShortestPathRecord implements Record {

    private ObjectIntMapping idMapping;

    private final Stack<IntIntHashMap> sourceLayers;
    private final Stack<IntIntHashMap> targetLayers;

    private IntIntHashMap currentLayer;
    private IntIterator iterator;
    private int current;
    private boolean forward;

    private final IntHashSet accessed;
    private long size;
    private boolean foundPath;

    public ShortestPathRecord(Id sourceV, Id targetV) {

        this.idMapping = new ObjectIntMapping();

        int sourceCode = this.code(sourceV);
        int targetCode = this.code(targetV);
        IntIntHashMap firstSourceLayer = new IntIntHashMap();
        IntIntHashMap firstTargetLayer = new IntIntHashMap();
        firstSourceLayer.put(sourceCode, 0);
        firstTargetLayer.put(targetCode, 0);
        this.sourceLayers = new Stack<>();
        this.targetLayers = new Stack<>();
        this.sourceLayers.push(firstSourceLayer);
        this.targetLayers.push(firstTargetLayer);

        this.accessed = new IntHashSet();
        this.accessed.add(sourceCode);
        this.accessed.add(targetCode);

        this.size = 2L;
    }

    public void startOneLayer(boolean forward) {
        this.forward = forward;
        this.currentLayer = new IntIntHashMap();
        this.iterator = this.forward ?
                this.sourceLayers.peek().keySet().intIterator() :
                this.targetLayers.peek().keySet().intIterator();
    }

    public void finishOneLayer() {
        if (this.forward) {
            this.sourceLayers.push(this.currentLayer);
        } else {
            this.targetLayers.push(this.currentLayer);
        }
        this.size += this.currentLayer.size();
    }

    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    public Id next() {
        this.current = this.iterator.next();
        return this.id(current);
    }

    public PathSet findPath(Id target, Function<Id, Boolean> filter,
                            boolean all, boolean ring) {
        assert !ring;
        PathSet paths = new PathSet();
        int targetCode = this.code(target);
        // If cross point exists, shortest path found, concat them
        if (this.contains(targetCode)) {
            if (filter.apply(target)) {
                return paths;
            }

            Path path = this.forward ? this.getPath(this.current, targetCode) :
                                       this.getPath(targetCode, this.current);
            this.foundPath = true;
            if (!all) {
                paths.add(path);
                return paths;
            }
        }

        /*
         * Not found shortest path yet, node is added to
         * newVertices if:
         * 1. not in sources and newVertices yet
         * 2. path of node doesn't have loop
         */
        if (!this.foundPath && this.isNew(targetCode)) {
            this.addOneStep(this.current, targetCode);
        }
        return paths;
    }

    public long accessed() {
        return this.size;
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

    private boolean isNew(int node) {
        return !this.currentLayer.containsKey(node) &&
               !this.accessed.contains(node);
    }

    private void addOneStep(int source, int target) {
        this.currentLayer.put(target, source);
    }

    private Path getPath(int source, int target) {
        int sourceLayerSize = this.sourceLayers.size();
        int targetLayerSize = this.targetLayers.size();

        List<Id> ids = new ArrayList<>(sourceLayerSize + targetLayerSize);

        ids.add(this.id(source));
        int value = source;
        for (int i = sourceLayerSize - 1; i > 0 ; i--) {
            IntIntHashMap layer = this.sourceLayers.elementAt(i);
            value = layer.get(value);
            ids.add(this.id(value));
        }
        Collections.reverse(ids);
        ids.add(this.id(target));
        value = target;
        for (int i = this.targetLayers.size() - 1; i > 0 ; i--) {
            IntIntHashMap layer = this.targetLayers.elementAt(i);
            value = layer.get(value);
            ids.add(this.id(value));
        }
        return new Path(ids);
    }

    private int code(Id id) {
        return this.idMapping.object2Code(id);
    }

    private Id id(int code) {
        return (Id) this.idMapping.code2Object(code);
    }
}
