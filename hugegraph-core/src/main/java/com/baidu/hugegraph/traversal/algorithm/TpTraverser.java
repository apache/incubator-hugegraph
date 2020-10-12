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

package com.baidu.hugegraph.traversal.algorithm;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.iterator.FilterIterator;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.util.Consumers;

import jersey.repackaged.com.google.common.base.Objects;

public abstract class TpTraverser extends HugeTraverser
                                  implements AutoCloseable {

    protected static ExecutorService executor;

    protected TpTraverser(HugeGraph graph) {
        super(graph);
    }

    protected TpTraverser(HugeGraph graph, String name) {
        super(graph);
        if (executor == null) {
            int workers = this.config().get(CoreOptions.OLTP_CONCURRENT_THREADS);
            if (workers > 0) {
                executor = Consumers.newThreadPool(name, workers);
            }
        }
    }

    protected int concurrentDepth() {
        if (executor == null) {
            return Integer.MAX_VALUE;
        }
        return this.config().get(CoreOptions.OLTP_CONCURRENT_DEPTH);
    }

    protected HugeConfig config() {
        return ((HugeConfig) this.graph().hugegraph().configuration());
    }

    @Override
    public void close() {
        if (executor != null) {
            executor.shutdown();
            executor = null;
        }
    }

    protected Set<Node> adjacentVertices(Set<Node> latest, EdgeStep step,
                                         Set<Node> all, long remaining,
                                         boolean single) {
        if (single) {
            return this.adjacentVertices(latest, step, all, remaining);
        } else {
            AtomicLong remain = new AtomicLong(remaining);
            return this.adjacentVertices(latest, step, all, remain);
        }
    }

    protected Set<Node> adjacentVertices(Set<Node> vertices, EdgeStep step,
                                         Set<Node> excluded,
                                         AtomicLong remaining) {
        Set<Node> neighbors = ConcurrentHashMap.newKeySet();
        this.traverseNodes(vertices.iterator(), v -> {
            Iterator<Edge> edges = this.edgesOfVertex(v.id(), step);
            while (edges.hasNext()) {
                Id target = ((HugeEdge) edges.next()).id().otherVertexId();
                KNode kNode = new KNode(target, (KNode) v);
                if (excluded != null && excluded.contains(kNode)) {
                    continue;
                }
                neighbors.add(kNode);
                if (remaining.decrementAndGet() <= 0L) {
                    return;
                }
            }
        });
        return neighbors;
    }

    protected long traverseNodes(Iterator<Node> vertices,
                                 Consumer<Node> consumer) {
        return this.traverse(vertices, consumer, "traverse-nodes");
    }

    protected long traversePairs(Iterator<Pair<Id, Id>> pairs,
                                 Consumer<Pair<Id, Id>> consumer) {
        return this.traverse(pairs, consumer, "traverse-pairs");
    }

    protected long traverseIds(Iterator<Id> ids, Consumer<Id> consumer) {
        return this.traverse(ids, consumer, "traverse-ids");
    }

    protected <K> long traverse(Iterator<K> iterator, Consumer<K> consumer,
                                String name) {
        Consumers<K> consumers = new Consumers<>(executor, consumer, null);
        consumers.start(name);
        long total = 0L;
        try {
            while (iterator.hasNext()) {
                total++;
                K v = iterator.next();
                consumers.provide(v);
            }
        } catch (Consumers.StopExecution e) {
            // pass
        } catch (Throwable e) {
            throw Consumers.wrapException(e);
        } finally {
            try {
                consumers.await();
            } catch (Throwable e) {
                Consumers.wrapException(e);
            } finally {
                CloseableIterator.closeIterator(iterator);
            }
        }
        return total;
    }

    protected Iterator<Vertex> filter(Iterator<Vertex> vertices,
                                      String key, Object value) {
        return new FilterIterator<>(vertices, vertex -> {
            return match(vertex, key, value);
        });
    }

    protected boolean match(Element elem, String key, Object value) {
        // check property key exists
        this.graph().propertyKey(key);
        // return true if property value exists & equals to specified value
        Property<Object> p = elem.property(key);
        return p.isPresent() && Objects.equal(p.value(), value);
    }

    public class ConcurrentMultiValuedMap<K, V>
           extends ConcurrentHashMap<K, List<V>> {

        public ConcurrentMultiValuedMap() {
            super();
        }

        public void add(K key, V value) {
            List<V> values = this.getValues(key);
            values.add(value);
        }

        public void addAll(K key, List<V> value) {
            List<V> values = this.getValues(key);
            values.addAll(value);
        }

        public List<V> getValues(K key) {
            List<V> values = this.get(key);
            if (values == null) {
                values = new CopyOnWriteArrayList<>();
                List<V> old = this.putIfAbsent(key, values);
                if (old != null) {
                    values = old;
                }
            }
            return values;
        }
    }
}
