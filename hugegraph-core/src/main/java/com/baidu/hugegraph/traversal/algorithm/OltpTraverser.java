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
import com.baidu.hugegraph.iterator.FilterIterator;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.traversal.algorithm.steps.EdgeStep;
import com.baidu.hugegraph.util.Consumers;

import jersey.repackaged.com.google.common.base.Objects;

public abstract class OltpTraverser extends HugeTraverser
                                    implements AutoCloseable {

    private static final String EXECUTOR_NAME = "oltp";
    private static Consumers.ExecutorPool executors;

    protected OltpTraverser(HugeGraph graph) {
        super(graph);
        if (executors != null) {
            return;
        }
        synchronized (OltpTraverser.class) {
            if (executors != null) {
                return;
            }
            int workers = this.graph()
                              .option(CoreOptions.OLTP_CONCURRENT_THREADS);
            if (workers > 0) {
                executors = new Consumers.ExecutorPool(EXECUTOR_NAME, workers);
            }
        }
    }

    @Override
    public void close() {
        // pass
    }

    public static void destroy() {
        synchronized (OltpTraverser.class) {
            if (executors != null) {
                executors.destroy();
                executors = null;
            }
        }
    }

    protected Set<Node> adjacentVertices(Id source, Set<Node> latest,
                                         EdgeStep step, Set<Node> all,
                                         long remaining, boolean single) {
        if (single) {
            return this.adjacentVertices(source, latest, step, all, remaining);
        } else {
            AtomicLong remain = new AtomicLong(remaining);
            return this.adjacentVertices(latest, step, all, remain);
        }
    }

    protected Set<Node> adjacentVertices(Set<Node> vertices, EdgeStep step,
                                         Set<Node> excluded, long remaining) {
        Set<Node> neighbors = newSet();
        for (Node source : vertices) {
            Iterator<Edge> edges = this.edgesOfVertex(source.id(), step);
            while (edges.hasNext()) {
                Id target = ((HugeEdge) edges.next()).id().otherVertexId();
                KNode kNode = new KNode(target, (KNode) source);
                if (excluded != null && excluded.contains(kNode)) {
                    continue;
                }
                neighbors.add(kNode);
                if (remaining != NO_LIMIT && --remaining <= 0L) {
                    return neighbors;
                }
            }
        }
        return neighbors;
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
        return this.traverse(vertices, consumer, "traverse-nodes", false);
    }

    protected long traversePairs(Iterator<Pair<Id, Id>> pairs,
                                 Consumer<Pair<Id, Id>> consumer) {
        return this.traverse(pairs, consumer, "traverse-pairs", false);
    }

    protected long traverseIds(Iterator<Id> ids, Consumer<Id> consumer,
                               boolean single, boolean stop) {
        if (!single) {
            return this.traverseIds(ids, consumer);
        } else {
            long count = 0L;
            while (ids.hasNext()) {
                count++;
                consumer.accept(ids.next());
            }
            return count;
        }
    }

    protected long traverseIds(Iterator<Id> ids, Consumer<Id> consumer) {
        return this.traverseIds(ids, consumer, false);
    }

    protected long traverseIds(Iterator<Id> ids, Consumer<Id> consumer,
                               boolean stop) {
        return this.traverse(ids, consumer, "traverse-ids", stop);
    }

    protected <K> long traverse(Iterator<K> iterator, Consumer<K> consumer,
                                String name, boolean stop) {
        if (!iterator.hasNext() || stop) {
            return 0L;
        }

        Consumers<K> consumers = new Consumers<>(executors.getExecutor(),
                                                 consumer, null);
        consumers.start(name);
        long total = 0L;
        try {
            while (!stop && iterator.hasNext()) {
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
                throw Consumers.wrapException(e);
            } finally {
                executors.returnExecutor(consumers.executor());
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

        private static final long serialVersionUID = -7249946839643493614L;

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
