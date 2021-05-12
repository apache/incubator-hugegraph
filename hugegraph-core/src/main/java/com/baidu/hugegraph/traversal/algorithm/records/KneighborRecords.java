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

import java.util.Set;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser.PathSet;
import com.baidu.hugegraph.traversal.algorithm.records.record.IntIterator;
import com.baidu.hugegraph.traversal.algorithm.records.record.RecordType;
import com.baidu.hugegraph.type.define.CollectionImplType;
import com.baidu.hugegraph.util.collection.CollectionFactory;
import com.google.common.collect.ImmutableList;

public class KneighborRecords extends SingleWayMultiPathsRecords {

    private final Id source;

    public KneighborRecords(RecordType type, boolean concurrent,
                            Id source, boolean nearest) {
        super(type, concurrent, source, nearest);
        this.source = source;
    }

    public int size() {
        return (int) this.accessed();
    }

    public Set<Id> ids(long limit) {
        Set<Id> ids = CollectionFactory.newIdSet(CollectionImplType.EC);
        ids.add(this.source);
        for (int i = 1; i < this.records.size(); i++) {
            IntIterator iterator = this.records.get(i).keys();
            while ((limit == NO_LIMIT || limit > 1L) && iterator.hasNext()) {
                ids.add(this.id(iterator.next()));
                limit--;
            }
        }
        return ids;
    }

    public PathSet paths(long limit) {
        PathSet paths = new PathSet();
        paths.add(new HugeTraverser.Path(ImmutableList.of(this.source)));
        for (int i = 1; i < this.records.size(); i++) {
            IntIterator iterator = this.records.get(i).keys();
            while ((limit == NO_LIMIT || limit > 1L) && iterator.hasNext()) {
                paths.add(this.getPath(i, iterator.next()));
                limit--;
            }
        }
        return paths;
    }
}
