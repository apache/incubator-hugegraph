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

import static com.baidu.hugegraph.backend.query.Query.NO_LIMIT;

import java.util.HashSet;
import java.util.List;
import java.util.Stack;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser.PathSet;
import com.baidu.hugegraph.traversal.algorithm.records.record.IntIterator;
import com.baidu.hugegraph.traversal.algorithm.records.record.Record;
import com.baidu.hugegraph.traversal.algorithm.records.record.RecordType;
import com.baidu.hugegraph.type.define.CollectionType;
import com.baidu.hugegraph.util.collection.CollectionFactory;

public class KoutRecords extends SingleWayMultiPathsRecords {

    // Non-zero depth is used for deepFirst traverse mode.
    // In such case, startOneLayer/finishOneLayer should not be called,
    // instead, we should use addFullPath
    private final int depth;

    public KoutRecords(RecordType type, boolean concurrent,
                       Id source, boolean nearest, int depth) {
        super(type, concurrent, source, nearest);

        this.depth = depth;

        for (int i = 0; i < depth; i++) {
            this.records().push(this.newRecord());
        }
        assert (this.records().size() == (depth + 1));
        // top most layer
        this.currentRecord(this.records().peek(), null);
    }

    @Override
    public int size() {
        return this.currentRecord().size();
    }

    public List<Id> ids(long limit) {
        List<Id> ids = CollectionFactory.newList(CollectionType.EC);
        IntIterator iterator = this.records().peek().keys();
        while ((limit == NO_LIMIT || limit-- > 0L) && iterator.hasNext()) {
            ids.add(this.id(iterator.next()));
        }
        return ids;
    }

    public PathSet paths(long limit) {
        PathSet paths = new PathSet();
        Stack<Record> records = this.records();
        IntIterator iterator = records.peek().keys();
        while ((limit == NO_LIMIT || limit-- > 0L) && iterator.hasNext()) {
            paths.add(this.getPath(records.size() - 1, iterator.next()));
        }
        return paths;
    }

    public void addFullPath(List<HugeEdge> edges, boolean withEdge) {
        assert (depth == edges.size());
        assert (records().size() == edges.size());

        int sourceCode = this.code(edges.get(0).id().ownerVertexId());
        int targetCode;
        Record record;
        HugeEdge edge;
        for (int i = 0; i < edges.size(); i++) {
            edge = edges.get(i);
            assert (this.code(edge.id().ownerVertexId()) == sourceCode);

            if (withEdge) {
                this.addEdge(edge);
            }

            targetCode = this.code(edge.id().otherVertexId());
            record = this.records().elementAt(i + 1);
            this.addPathToRecord(sourceCode, targetCode, record);
            sourceCode = targetCode;
        }
    }

    public void filterUnusedEdges(long limit) {
        // for breadth-first algorithm, edges are collected when met,
        // but only those which connect to last-level should be kept.
        HashSet<Long> codePairs = new HashSet<>();

        Stack<Record> records = this.records();
        IntIterator iterator = records.peek().keys();
        while ((limit == NO_LIMIT || limit-- > 0L) && iterator.hasNext()) {
            addEdgeToCodePair(codePairs, records.size() - 1, iterator.next());
        }

        filterEdges(codePairs);
    }
}
