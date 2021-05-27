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

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser.Path;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser.PathSet;
import com.baidu.hugegraph.traversal.algorithm.records.record.IntIterator;
import com.baidu.hugegraph.traversal.algorithm.records.record.Record;
import com.baidu.hugegraph.traversal.algorithm.records.record.RecordType;
import com.google.common.collect.Lists;

public abstract class DoubleWayMultiPathsRecords extends AbstractRecords {

    private final Stack<Record> sourceRecords;
    private final Stack<Record> targetRecords;

    private IntIterator lastRecordKeys;
    private int currentKey;
    private boolean forward;
    private int accessed;

    public DoubleWayMultiPathsRecords(RecordType type, boolean concurrent,
                                      Id sourceV, Id targetV) {
        super(type, concurrent);
        int sourceCode = this.code(sourceV);
        int targetCode = this.code(targetV);
        Record firstSourceRecord = this.newRecord();
        Record firstTargetRecord = this.newRecord();
        firstSourceRecord.addPath(sourceCode, 0);
        firstTargetRecord.addPath(targetCode, 0);
        this.sourceRecords = new Stack<>();
        this.targetRecords = new Stack<>();
        this.sourceRecords.push(firstSourceRecord);
        this.targetRecords.push(firstTargetRecord);

        this.accessed = 2;
    }

    @Override
    public void startOneLayer(boolean forward) {
        this.forward = forward;
        this.currentRecord(this.newRecord());
        this.lastRecordKeys = this.forward ? this.sourceRecords.peek().keys() :
                                             this.targetRecords.peek().keys();
    }

    @Override
    public void finishOneLayer() {
        Record record = this.currentRecord();
        if (this.forward) {
            this.sourceRecords.push(record);
        } else {
            this.targetRecords.push(record);
        }
        this.accessed += record.size();
    }

    @Watched
    @Override
    public boolean hasNextKey() {
        return this.lastRecordKeys.hasNext();
    }

    @Watched
    @Override
    public Id nextKey() {
        this.currentKey = this.lastRecordKeys.next();
        return this.id(this.currentKey);
    }

    @Watched
    public PathSet findPath(Id target, Function<Id, Boolean> filter,
                            boolean all, boolean ring) {
        assert all;
        PathSet results = new PathSet();
        int targetCode = this.code(target);
        // If cross point exists, path found, concat them
        if (this.forward && this.targetContains(targetCode)) {
            results = this.linkPath(this.currentKey, targetCode, ring);
        }
        if (!this.forward && this.sourceContains(targetCode)) {
            results = this.linkPath(targetCode, this.currentKey, ring);
        }
        this.addPath(targetCode, this.currentKey);
        return results;
    }

    @Override
    public long accessed() {
        return this.accessed;
    }

    protected boolean sourceContains(int node) {
        return this.sourceRecords.peek().containsKey(node);
    }

    protected boolean targetContains(int node) {
        return this.targetRecords.peek().containsKey(node);
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
                Id crosspoint = this.id(this.forward ? target : source);
                results.add(new Path(crosspoint, ids));
            }
        }
        return results;
    }

    private PathSet linkSourcePath(int source) {
        return this.linkPath(this.sourceRecords, source,
                             this.sourceRecords.size() - 1);
    }

    private PathSet linkTargetPath(int target) {
        return this.linkPath(this.targetRecords, target,
                             this.targetRecords.size() - 1);
    }

    private PathSet linkPath(Stack<Record> all, int id, int layerIndex) {
        PathSet results = new PathSet();
        if (layerIndex == 0) {
            Id sid = this.id(id);
            results.add(new Path(Lists.newArrayList(sid)));
            return results;
        }

        Id sid = this.id(id);
        Record layer = all.elementAt(layerIndex);
        IntIterator iterator = layer.get(id);
        while (iterator.hasNext()) {
            int parent = iterator.next();
            PathSet paths = this.linkPath(all, parent, layerIndex - 1);
            for (Iterator<Path> iter = paths.iterator(); iter.hasNext();) {
                Path path = iter.next();
                if (path.vertices().contains(sid)) {
                    iter.remove();
                    continue;
                }
                path.addToLast(sid);
            }

            results.addAll(paths);
        }
        return results;
    }

    @Watched
    protected void addPath(int current, int parent) {
        this.currentRecord().addPath(current, parent);
    }

    protected Stack<Record> sourceRecords() {
        return this.sourceRecords;
    }

    protected Stack<Record> targetRecords() {
        return this.targetRecords;
    }

    protected boolean forward() {
        return this.forward;
    }

    protected int current() {
        return this.currentKey;
    }
}
