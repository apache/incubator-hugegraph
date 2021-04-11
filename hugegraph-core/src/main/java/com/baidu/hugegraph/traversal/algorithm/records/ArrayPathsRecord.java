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
import java.util.Random;
import java.util.Stack;
import java.util.function.Function;

import org.apache.commons.collections.CollectionUtils;
import org.eclipse.collections.api.iterator.IntIterator;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser.Path;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser.PathSet;
import com.baidu.hugegraph.util.collection.ObjectIntMapping;
import com.google.common.collect.Lists;

public class ArrayPathsRecord implements Record {

    private ObjectIntMapping idMapping;

    private final Stack<Int2IntsMap> sourceLayers;
    private final Stack<Int2IntsMap> targetLayers;

    private Int2IntsMap currentLayer;
    private IntIterator iterator;
    private int current;
    private boolean forward;

    private int accessed;

    public ArrayPathsRecord(Id sourceV, Id targetV) {

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
        this.iterator = this.forward ?
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
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    @Watched
    public Id next() {
        this.current = this.iterator.next();
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
                      this.getPath(this.current, targetCode, ring) :
                      this.getPath(targetCode, this.current, ring);
        }
        this.add(targetCode, this.current);
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
    private PathSet getPath(int source, int target, boolean ring) {
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

    private PathSet getPath(Stack<Int2IntsMap> all, int id, int layerIndex) {
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
            PathSet paths = this.getPath(all, parent, layerIndex - 1);
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
        return this.getPath(this.sourceLayers, source,
                            this.sourceLayers.size() - 1);
    }

    private PathSet getTargetPath(int target) {
        return this.getPath(this.targetLayers, target,
                            this.targetLayers.size() - 1);
    }

    @Watched
    private void add(int current, int parent) {
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

    public static class Int2IntsMap {

        private static final int INIT_CAPACITY = 16;
        private static final int CHUNK_SIZE = 10;

        private IntIntHashMap offsetMap;
        private int[] intsTable;

        private int nextBlock;

        public Int2IntsMap() {
            this.offsetMap = new IntIntHashMap(INIT_CAPACITY);
            this.intsTable = new int[INIT_CAPACITY * CHUNK_SIZE];
            this.nextBlock = 0;
        }

        public void add(int key, int value) {
            if (this.offsetMap.containsKey(key)) {
                int positionIndex = this.offsetMap.get(key);
                int position = this.intsTable[positionIndex];
                if (this.endOfChunk(position)) {
                    this.ensureCapacity();

                    this.intsTable[position] = this.nextBlock;

                    this.intsTable[this.nextBlock] = value;
                    this.intsTable[positionIndex] = this.nextBlock + 1;

                    // Update next block
                    this.nextBlock += CHUNK_SIZE;

                } else {
                    intsTable[position] = value;
                    this.intsTable[positionIndex]++;
                }
                this.intsTable[positionIndex + 1]++;
            } else {
                // New key, allocate 1st chunk and init
                this.ensureCapacity();

                // Allocate 1st chunk
                this.offsetMap.put(key, this.nextBlock);

                // Init first chunk
                this.intsTable[this.nextBlock] = this.nextBlock + 3;
                this.intsTable[this.nextBlock + 1] = 1;
                this.intsTable[this.nextBlock + 2] = value;

                // Update next block
                this.nextBlock += CHUNK_SIZE;
            }
        }

        public boolean containsKey(int key) {
            return this.offsetMap.containsKey(key);
        }

        public int[] get(int key) {
            int firstChunk = this.offsetMap.get(key);
            int size = this.intsTable[firstChunk + 1];
            int[] values = new int[size];
            for (int i = 0, position = firstChunk + 2; i < size; i++) {
                if (!this.endOfChunk(position)) {
                    values[i] = this.intsTable[position++];
                } else {
                    position = this.intsTable[position];
                    i--;
                }
            }
            return values;
        }

        public IntIterator keyIterator() {
            return this.offsetMap.keySet().intIterator();
        }

        public int size() {
            return this.offsetMap.size();
        }

        private boolean endOfChunk(int position) {
            return (position + 1) % CHUNK_SIZE == 0;
        }

        private void ensureCapacity() {
            if (this.nextBlock >= this.intsTable.length) {
                this.expansion();
            }
        }

        private void expansion() {
            int currentSize = this.intsTable.length;
            int[] newTable = new int[2 * currentSize];
            System.arraycopy(this.intsTable, 0, newTable, 0, currentSize);
            this.intsTable = newTable;
        }
    }

    public static void main(String[] args) {
        int[] array = new int[5];
        for (int i = 0; i < array.length; i++) {
            System.out.println(array[i]);
        }
        array[3]++;
        System.out.println(array[3]);

        Int2IntsMap map = new Int2IntsMap();

        Random random = new Random();
        int i = 100;
        int j = 200;
        int k = 250;
        int l = 255;
        int m = 270;

        for (int n = 0; n < 1000; n++) {
            switch (random.nextInt() % 5) {
                case 0:
                if (i < 200) {
                    map.add(1, i++);
                }
                break;
                case 1:
                if (j < 250) {
                    map.add(2, j++);
                }
                    break;
                case 2:
                if (k < 255) {
                    map.add(3, k++);
                }
                    break;
                case 3:
                if (l < 270) {
                    map.add(4, l++);
                }
                    break;
                case 4:
                if (m < 300) {
                    map.add(5, m++);
                }
                break;
            }
        }

        for (int ii = 1; ii <= 5; ii++) {
            int[] result = map.get(ii);
            for (int jj : result) {
                System.out.print(jj);
                System.out.print(",");
            }
            System.out.println();
        }
    }
}
