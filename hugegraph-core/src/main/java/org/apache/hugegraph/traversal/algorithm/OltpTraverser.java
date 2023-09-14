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

package org.apache.hugegraph.traversal.algorithm;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.EdgeId;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.EdgesQueryIterator;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.iterator.FilterIterator;
import org.apache.hugegraph.iterator.MapperIterator;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.traversal.algorithm.steps.Steps;
import org.apache.hugegraph.type.define.Directions;
import org.apache.hugegraph.util.Consumers;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import com.google.common.base.Objects;

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

    public static void destroy() {
        synchronized (OltpTraverser.class) {
            if (executors != null) {
                executors.destroy();
                executors = null;
            }
        }
    }

    @Override
    public void close() {
        // pass
    }

    protected long traversePairs(Iterator<Pair<Id, Id>> pairs,
                                 Consumer<Pair<Id, Id>> consumer) {
        return this.traverse(pairs, consumer, "traverse-pairs");
    }

    protected long traverseIds(Iterator<Id> ids, Consumer<Id> consumer,
                               boolean concurrent) {
        if (concurrent) {
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
        return this.traverse(ids, consumer, "traverse-ids");
    }

    protected <K> long traverse(Iterator<K> iterator, Consumer<K> consumer,
                                String name) {
        if (!iterator.hasNext()) {
            return 0L;
        }

        Consumers<K> consumers = new Consumers<>(executors.getExecutor(),
                                                 consumer, null);
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
                throw Consumers.wrapException(e);
            } finally {
                executors.returnExecutor(consumers.executor());
                CloseableIterator.closeIterator(iterator);
            }
        }
        return total;
    }

    protected <K> long traverseBatch(Iterator<Iterator<K>> iterator,
                                     Consumer<Iterator<K>> consumer,
                                     String name, int queueWorkerSize) {
        if (!iterator.hasNext()) {
            return 0L;
        }
        AtomicBoolean done = new AtomicBoolean(false);
        Consumers<Iterator<K>> consumers = null;
        try {
            consumers = getConsumers(consumer, queueWorkerSize, done,
                                     executors.getExecutor());
            return consumersStart(iterator, name, done, consumers);
        } finally {
            assert consumers != null;
            executors.returnExecutor(consumers.executor());
        }
    }

    private <K> long consumersStart(Iterator<Iterator<K>> iterator, String name,
                                    AtomicBoolean done,
                                    Consumers<Iterator<K>> consumers) {
        long total = 0L;
        try {
            consumers.start(name);
            while (iterator.hasNext() && !done.get()) {
                total++;
                Iterator<K> v = iterator.next();
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
                CloseableIterator.closeIterator(iterator);
            }
        }
        return total;
    }

    private <K> Consumers<Iterator<K>> getConsumers(Consumer<Iterator<K>> consumer,
                                                    int queueWorkerSize,
                                                    AtomicBoolean done,
                                                    ExecutorService executor) {
        Consumers<Iterator<K>> consumers;
        consumers = new Consumers<>(executor,
                                    consumer,
                                    null,
                                    e -> done.set(true),
                                    queueWorkerSize);
        return consumers;
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

    protected void bfsQuery(Iterator<Id> vertices,
                            Directions dir,
                            Id label,
                            long degree,
                            long capacity,
                            Consumer<EdgeId> parseConsumer) {
        List<Id> labels =
                label == null ? Collections.emptyList() : Collections.singletonList(label);
        CapacityConsumer consumer = new CapacityConsumer(parseConsumer, capacity);

        EdgesIterator edgeIts = edgesOfVertices(vertices, dir, labels, degree);
        // 并行乱序处理
        this.traverseBatch(edgeIts, consumer, "traverse-ite-edge", 1);
    }

    protected void bfsQuery(Iterator<Id> vertices,
                            Steps steps,
                            long capacity,
                            Consumer<Edge> parseConsumer) {
        CapacityConsumerWithStep consumer =
                new CapacityConsumerWithStep(parseConsumer, capacity, steps);

        EdgesQueryIterator queryIterator =
                new EdgesQueryIterator(vertices, steps.direction(), steps.edgeLabels(),
                                       steps.degree());

        // 这里获取边数据，以便支持 step
        EdgesIterator edgeIts = new EdgesIterator(queryIterator);

        // 并行乱序处理
        this.traverseBatch(edgeIts, consumer, "traverse-ite-edge", 1);
    }

    public static class ConcurrentMultiValuedMap<K, V>
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

    public static class ConcurrentVerticesConsumer implements Consumer<EdgeId> {
        private final Id sourceV;
        private final Set<Id> excluded;
        private final Set<Id> neighbors;
        private final long limit;
        private final AtomicInteger count = new AtomicInteger(0);

        public ConcurrentVerticesConsumer(Id sourceV, Set<Id> excluded, long limit,
                                          Set<Id> neighbors) {
            this.sourceV = sourceV;
            this.excluded = excluded;
            this.limit = limit;
            this.neighbors = neighbors;
        }

        @Override
        public void accept(EdgeId edgeId) {
            if (limit != NO_LIMIT && count.get() >= limit) {
                throw new Consumers.StopExecution("reach limit");
            }

            Id targetV = edgeId.otherVertexId();
            if (sourceV.equals(targetV)) {
                return;
            }

            if (excluded != null && excluded.contains(targetV)) {
                return;
            }

            if (neighbors.add(targetV)) {
                if (limit != NO_LIMIT) {
                    count.getAndIncrement();
                }
            }
        }
    }

    public abstract class EdgeItConsumer<T, E> implements Consumer<Iterator<T>> {
        private final Consumer<E> parseConsumer;
        private final long capacity;

        public EdgeItConsumer(Consumer<E> parseConsumer, long capacity) {
            this.parseConsumer = parseConsumer;
            this.capacity = capacity;
        }

        protected abstract Iterator<E> prepare(Iterator<T> it);

        @Override
        public void accept(Iterator<T> edges) {
            Iterator<E> ids = prepare(edges);
            long counter = 0;
            while (ids.hasNext()) {
                if (Thread.currentThread().isInterrupted()) {
                    LOG.warn("Consumer isInterrupted");
                    break;
                }
                counter++;
                parseConsumer.accept(ids.next());
            }
            long total = edgeIterCounter.addAndGet(counter);
            // 按批次检测 capacity，以提高性能
            if (this.capacity != NO_LIMIT && total >= capacity) {
                throw new Consumers.StopExecution("reach capacity");
            }
        }
    }

    public class CapacityConsumer extends EdgeItConsumer<Edge, EdgeId> {
        public CapacityConsumer(Consumer<EdgeId> parseConsumer, long capacity) {
            super(parseConsumer, capacity);
        }

        @Override
        protected Iterator<EdgeId> prepare(Iterator<Edge> edges) {
            return new MapperIterator<>(edges, (e) -> ((HugeEdge) e).id());
        }
    }

    public class CapacityConsumerWithStep extends EdgeItConsumer<Edge, Edge> {
        private final Steps steps;

        public CapacityConsumerWithStep(Consumer<Edge> parseConsumer, long capacity,
                                        Steps steps) {
            super(parseConsumer, capacity);
            this.steps = steps;
        }

        @Override
        protected Iterator<Edge> prepare(Iterator<Edge> edges) {
            return edgesOfVertexStep(edges, steps);
        }
    }

}
