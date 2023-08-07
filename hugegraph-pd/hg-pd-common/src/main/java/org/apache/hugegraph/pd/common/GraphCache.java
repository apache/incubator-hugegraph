/*
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

package org.apache.hugegraph.pd.common;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hugegraph.pd.grpc.Metapb.Graph;
import org.apache.hugegraph.pd.grpc.Metapb.Partition;

import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

import lombok.Data;

@Data
public class GraphCache {

    private Graph graph;
    private AtomicBoolean initialized = new AtomicBoolean(false);
    private AtomicBoolean writing = new AtomicBoolean(false);
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private Map<Integer, AtomicBoolean> state = new ConcurrentHashMap<>();
    private Map<Integer, Partition> partitions = new ConcurrentHashMap<>();
    private RangeMap<Long, Integer> range = TreeRangeMap.create();

    public GraphCache(Graph graph) {
        this.graph = graph;
    }

    public GraphCache() {
    }

    public Partition getPartition(Integer id) {
        return partitions.get(id);
    }

    public Partition addPartition(Integer id, Partition p) {
        return partitions.put(id, p);
    }

    public Partition removePartition(Integer id) {
        return partitions.remove(id);
    }
}
