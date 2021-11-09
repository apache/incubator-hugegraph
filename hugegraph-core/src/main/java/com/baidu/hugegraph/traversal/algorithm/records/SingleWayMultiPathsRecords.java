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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;
import java.util.function.Function;

import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.iterator.MapperIterator;
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser.Path;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser.PathSet;
import com.baidu.hugegraph.traversal.algorithm.records.record.Int2IntRecord;
import com.baidu.hugegraph.traversal.algorithm.records.record.IntIterator;
import com.baidu.hugegraph.traversal.algorithm.records.record.Record;
import com.baidu.hugegraph.traversal.algorithm.records.record.RecordType;
import com.baidu.hugegraph.type.define.CollectionType;
import com.baidu.hugegraph.util.collection.CollectionFactory;

public abstract class SingleWayMultiPathsRecords extends AbstractRecords {

    private final Stack<Record> records;

    private final int sourceCode;
    private final boolean nearest;
    private final MutableIntSet accessedVertices;

    private IntIterator lastRecordKeys;

    public SingleWayMultiPathsRecords(RecordType type, boolean concurrent,
                                      Id source, boolean nearest) {
        super(type, concurrent);

        this.nearest = nearest;

        this.sourceCode = this.code(source);
        Record firstRecord = this.newRecord();
        firstRecord.addPath(this.sourceCode, 0);
        this.records = new Stack<>();
        this.records.push(firstRecord);

        this.accessedVertices = concurrent ? new IntHashSet().asSynchronized() :
                                new IntHashSet();
    }

    @Override
    public void startOneLayer(boolean forward) {
        this.currentRecord(this.newRecord());
        this.lastRecordKeys = this.records.peek().keys();
    }

    @Override
    public void finishOneLayer() {
        this.records.push(this.currentRecord());
    }

    @Override
    public boolean hasNextKey() {
        return this.lastRecordKeys.hasNext();
    }

    @Override
    public Id nextKey() {
        return this.id(this.lastRecordKeys.next());
    }

    @Override
    public PathSet findPath(Id target, Function<Id, Boolean> filter,
                            boolean all, boolean ring) {
        PathSet paths = new PathSet();
        for (int i = 1; i < this.records.size(); i++) {
            IntIterator iterator = this.records.get(i).keys();
            while (iterator.hasNext()) {
                paths.add(this.linkPath(i, iterator.next()));
            }
        }
        return paths;
    }

    @Override
    public long accessed() {
        return this.accessedVertices.size();
    }

    public Iterator<Id> keys() {
        return new MapperIterator<>(this.lastRecordKeys, this::id);
    }

    @Watched
    public void addPath(Id source, Id target) {
        int sourceCode = this.code(source);
        int targetCode = this.code(target);
        if (this.nearest && this.accessedVertices.contains(targetCode) ||
            !this.nearest && this.currentRecord().containsKey(targetCode) ||
            targetCode == this.sourceCode) {
            return;
        }
        this.currentRecord().addPath(targetCode, sourceCode);

        this.accessedVertices.add(targetCode);
    }

    protected final Path linkPath(int target) {
        List<Id> ids = CollectionFactory.newList(CollectionType.EC);
        // Find the layer where the target is located
        int foundLayer = -1;
        for (int i = 0; i < this.records.size(); i++) {
            IntIntHashMap layer = this.layer(i);
            if (!layer.containsKey(target)) {
                continue;
            }

            foundLayer = i;
            // Collect self node
            ids.add(this.id(target));
            break;
        }
        // If a layer found, then concat parents
        if (foundLayer > 0) {
            for (int i = foundLayer; i > 0; i--) {
                IntIntHashMap layer = this.layer(i);
                // Uptrack parents
                target = layer.get(target);
                ids.add(this.id(target));
            }
        }
        return new Path(ids);
    }

    protected final Path linkPath(int layerIndex, int target) {
        List<Id> ids = CollectionFactory.newList(CollectionType.EC);
        IntIntHashMap layer = this.layer(layerIndex);
        if (!layer.containsKey(target)) {
            throw new HugeException("Failed to get path for %s",
                                    this.id(target));
        }
        // Collect self node
        ids.add(this.id(target));
        // Concat parents
        for (int i = layerIndex; i > 0; i--) {
            layer = this.layer(i);
            // Uptrack parents
            target = layer.get(target);
            ids.add(this.id(target));
        }
        Collections.reverse(ids);
        return new Path(ids);
    }

    protected final IntIntHashMap layer(int layerIndex) {
        Record record = this.records.elementAt(layerIndex);
        IntIntHashMap layer = ((Int2IntRecord) record).layer();
        return layer;
    }

    protected final Stack<Record> records() {
        return this.records;
    }

    public abstract int size();

    public abstract List<Id> ids(long limit);

    public abstract PathSet paths(long limit);
}
