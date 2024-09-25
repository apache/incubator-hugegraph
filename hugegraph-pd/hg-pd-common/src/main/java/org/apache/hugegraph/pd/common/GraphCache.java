/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.pd.common;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.Range;

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
    private RangeMap<Long, Integer> range = new SynchronizedRangeMap<Long, Integer>().rangeMap;

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

    public class SynchronizedRangeMap<K extends Comparable<K>, V> {

        private final RangeMap<K, V> rangeMap = TreeRangeMap.create();
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

        public void put(Range<K> range, V value) {
            lock.writeLock().lock();
            try {
                rangeMap.put(range, value);
            } finally {
                lock.writeLock().unlock();
            }
        }

        public V get(K key) {
            lock.readLock().lock();
            try {
                return rangeMap.get(key);
            } finally {
                lock.readLock().unlock();
            }
        }

        public void remove(Range<K> range) {
            lock.writeLock().lock();
            try {
                rangeMap.remove(range);
            } finally {
                lock.writeLock().unlock();
            }
        }

        public Map.Entry<Range<K>, V> getEntry(K key) {
            lock.readLock().lock();
            try {
                return rangeMap.getEntry(key);
            } finally {
                lock.readLock().unlock();
            }
        }

        public void clear() {
            lock.writeLock().lock();
            try {
                rangeMap.clear();
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

}
