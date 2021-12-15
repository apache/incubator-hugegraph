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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.function.Function;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.iterator.MapperIterator;
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser.Path;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser.PathSet;
import com.baidu.hugegraph.traversal.algorithm.records.record.Int2IntRecord;
import com.baidu.hugegraph.traversal.algorithm.records.record.IntIterator;
import com.baidu.hugegraph.traversal.algorithm.records.record.Record;
import com.baidu.hugegraph.traversal.algorithm.records.record.RecordType;

public abstract class SingleWayMultiPathsRecords extends AbstractRecords {

    private final Stack<Record> records;

    private final int sourceCode;
    private final boolean nearest;
    private final MutableIntSet accessedVertices;

    private IntIterator parentRecordKeys;

    // collection of edges
    private HashSet<Id> edgeIds = new HashSet<>();
    private final ArrayList<Edge> edges = new ArrayList<>();

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
        Record parentRecord = this.records.peek();
        this.currentRecord(this.newRecord(), parentRecord);
        this.parentRecordKeys = parentRecord.keys();
    }

    @Override
    public void finishOneLayer() {
        this.records.push(this.currentRecord());
    }

    @Override
    public boolean hasNextKey() {
        return this.parentRecordKeys.hasNext();
    }

    @Override
    public Id nextKey() {
        return this.id(this.parentRecordKeys.next());
    }

    @Override
    public PathSet findPath(Id target, Function<Id, Boolean> filter,
                            boolean all, boolean ring) {
        PathSet paths = new PathSet();
        for (int i = 1; i < this.records.size(); i++) {
            IntIterator iterator = this.records.get(i).keys();
            while (iterator.hasNext()) {
                paths.add(this.getPath(i, iterator.next()));
            }
        }
        return paths;
    }

    @Override
    public long accessed() {
        return this.accessedVertices.size();
    }

    public Iterator<Id> keys() {
        return new MapperIterator<>(this.parentRecordKeys, this::id);
    }

    @Watched
    public void addPath(Id source, Id target) {
        this.addPathToRecord(this.code(source), this.code(target), this.currentRecord());
    }

    protected void addPathToRecord(int sourceCode, int targetCode, Record record) {
        if (this.nearest && this.accessedVertices.contains(targetCode) ||
            !this.nearest && this.currentRecord().containsKey(targetCode) ||
            targetCode == this.sourceCode) {
            return;
        }
        record.addPath(targetCode, sourceCode);
        this.accessedVertices.add(targetCode);
    }

    public abstract int size();

    public Path getPath(int layerIndex, int target) {
        List<Integer> ids = getPathCodes(layerIndex, target);
        return this.codesToPath(ids);
    }

    public List<Integer> getPathCodes(int layerIndex, int target) {
        List<Integer> ids = new ArrayList<>();
        IntIntHashMap layer = ((Int2IntRecord) this.records
                              .elementAt(layerIndex)).layer();
        if (!layer.containsKey(target)) {
            throw new HugeException("Failed to get path for %s",
                                    this.id(target));
        }
        ids.add(target);
        int parent = layer.get(target);
        ids.add(parent);
        layerIndex--;
        for (; layerIndex > 0; layerIndex--) {
            layer = ((Int2IntRecord) this.records
                    .elementAt(layerIndex)).layer();
            parent = layer.get(parent);
            ids.add(parent);
        }
        Collections.reverse(ids);
        return ids;
    }

    public Path codesToPath(List<Integer> codes) {
        ArrayList<Id> ids = new ArrayList<>();
        for ( int code : codes) {
            ids.add(this.id(code));
        }
        return new Path(ids);
    }

    public Stack<Record> records() {
        return this.records;
    }

    public void addEdge(HugeEdge edge) {
        if( !edgeIds.contains(edge.id())) {
            this.edgeIds.add(edge.id());
            this.edges.add(edge);
        }
    }

    // for breadth-first only
    public void addEdgeId(Id edgeId) {
        this.edgeIds.add(edgeId);
    }

    public Iterator<Edge> getEdges() {
        if (this.edges.size() == 0)
            return null;
        return this.edges.iterator();
    }

    public Set<Id> getEdgeIds() {
        return this.edgeIds;
    }

    protected static Long makeCodePair(int source, int target) {
        return ((long) source & 0xFFFFFFFFl) |
               (((long) target << 32) & 0xFFFFFFFF00000000l);
    }

    protected void addEdgeToCodePair(HashSet<Long> codePairs,
                                     int layerIndex, int target) {
        List<Integer> codes = this.getPathCodes(layerIndex, target);
        for (int i = 1; i < codes.size(); i++) {
            codePairs.add(makeCodePair(codes.get(i - 1), codes.get(i)));
        }
    }

    protected void filterEdges(HashSet<Long> codePairs) {
        HashSet<Id> edgeIds = this.edgeIds;
        this.edgeIds = new HashSet<>();
        for (Id id : edgeIds) {
            EdgeId edgeId = (EdgeId) id;
            Long pair = makeCodePair(this.code(edgeId.ownerVertexId()),
                                     this.code(edgeId.otherVertexId()));
            if (codePairs.contains(pair)) {
                // need edge
                this.edgeIds.add(id);
            }
        }
    }
}
