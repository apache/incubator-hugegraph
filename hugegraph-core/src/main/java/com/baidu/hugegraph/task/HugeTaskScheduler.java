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

package com.baidu.hugegraph.task;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.iterator.MapperIterator;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.task.HugeTask.P;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Events;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class HugeTaskScheduler {

    private final HugeGraph graph;
    private final ExecutorService taskExecutor;
    private final ExecutorService dbExecutor;

    private final Map<Id, HugeTask<?>> tasks;

    private volatile TaskTransaction taskTx;

    private static final long NO_LIMIT = -1l;

    public HugeTaskScheduler(HugeGraph graph,
                             ExecutorService taskExecutor,
                             ExecutorService dbExecutor) {
        E.checkNotNull(graph, "graph");
        E.checkNotNull(taskExecutor, "taskExecutor");
        E.checkNotNull(dbExecutor, "dbExecutor");

        this.graph = graph;
        this.taskExecutor = taskExecutor;
        this.dbExecutor = dbExecutor;

        this.tasks = new ConcurrentHashMap<>();

        this.taskTx = null;

        this.listenChanges();
    }

    public HugeGraph graph() {
        return this.graph;
    }

    private TaskTransaction tx() {
        // NOTE: only the owner thread can access task tx
        if (this.taskTx == null) {
            synchronized (this) {
                if (this.taskTx == null) {
                    BackendStore store = this.graph.loadSystemStore();
                    this.taskTx = new TaskTransaction(this.graph, store);
                }
            }
        }
        assert this.taskTx != null;
        return this.taskTx;
    }

    private void listenChanges() {
        // Listen store event: "store.init", "store.truncate"
        Set<String> storeEvents = ImmutableSet.of(Events.STORE_INIT,
                                                  Events.STORE_TRUNCATE);
        this.graph.loadSystemStore().provider().listen(event -> {
            if (storeEvents.contains(event.name())) {
                this.submit(() -> this.tx().initSchema());
                return true;
            }
            return false;
        });
    }

    public <V> Future<?> restore(HugeTask<V> task) {
        E.checkArgumentNotNull(task, "Task can't be null");
        E.checkState(!task.isDone(), "No need to restore task '%s', " +
                     "it has been completed", task.id());
        task.status(Status.RESTORING);
        return this.submitTask(task);
    }

    public <V> Future<?> schedule(HugeTask<V> task) {
        E.checkArgumentNotNull(task, "Task can't be null");
        task.status(Status.QUEUED);
        return this.submitTask(task);
    }

    private <V> Future<?> submitTask(HugeTask<V> task) {
        this.tasks.put(task.id(), task);
        task.callable().scheduler(this);
        task.callable().task(task);
        return this.taskExecutor.submit(task);
    }

    public <V> void cancel(HugeTask<V> task) {
        E.checkArgumentNotNull(task, "Task can't be null");
        this.tasks.remove(task.id());
        task.cancel(false);
    }

    protected void remove(Id taskId) {
        HugeTask<?> task = this.tasks.remove(taskId);
        assert task == null || task.completed();
    }

    public <V> void save(HugeTask<V> task) {
        E.checkArgumentNotNull(task, "Task can't be null");
        this.submit(() -> {
            // Construct vertex from task
            HugeVertex vertex = this.tx().constructVertex(task);
            // Delete the old record if exist
            Iterator<Vertex> old = this.tx().queryVertices(vertex.id());
            if (old.hasNext()) {
                HugeVertex oldV = (HugeVertex) old.next();
                assert !old.hasNext();
                if (this.tx().indexValueChanged(oldV, vertex)) {
                    // Only delete vertex if index value changed else override
                    this.tx().removeVertex(oldV);
                }
            }
            // Do update
            return this.tx().addVertex(vertex);
        });
    }

    public boolean close() {
        if (!this.dbExecutor.isShutdown()) {
            this.submit(() -> {
                this.tx().close();
                this.graph.closeTx();
            });
        }
        return true;
    }

    public <V> HugeTask<V> task(Id id) {
        @SuppressWarnings("unchecked")
        HugeTask<V> task = (HugeTask<V>) this.tasks.get(id);
        if (task != null) {
            return task;
        }
        return this.findTask(id);
    }

    public <V> HugeTask<V> findTask(Id id) {
        HugeTask<V> result =  this.submit(() -> {
            HugeTask<V> task = null;
            Iterator<Vertex> vertices = this.tx().queryVertices(id);
            if (vertices.hasNext()) {
                task = HugeTask.fromVertex(vertices.next());
                assert !vertices.hasNext();
            }
            return task;
        });
        if (result == null) {
            throw new NotFoundException("Can't find task with id '%s'", id);
        }
        return result;
    }

    public <V> Iterator<HugeTask<V>> findAllTask(long limit) {
        return this.queryTask(ImmutableMap.of(), limit);
    }

    public <V> Iterator<HugeTask<V>> findTask(Status status, long limit) {
        return this.queryTask(P.STATUS, status.code(), limit);
    }

    public <V> HugeTask<V> deleteTask(Id id) {
        return this.submit(() -> {
            Iterator<Vertex> vertices = this.tx().queryVertices(id);
            if (vertices.hasNext()) {
                this.tx().removeVertex((HugeVertex) vertices.next());
                assert !vertices.hasNext();
            }
        });
    }

    public <V> HugeTask<V> waitUntilTaskCompleted(Id id, long seconds)
                                                  throws TimeoutException {
        for (long pass = 0;; pass++) {
            HugeTask<V> task = this.task(id);
            if (task.completed()) {
                return task;
            }
            if (pass >= seconds) {
                break;
            }
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException ignored) {
                // Ignore InterruptedException
            }
        }
        throw new TimeoutException(String.format(
                  "Task '%s' was not completed in %s seconds", id, seconds));
    }

    private <V> Iterator<HugeTask<V>> queryTask(String key, Object value,
                                                long limit) {
        return this.queryTask(ImmutableMap.of(key, value), limit);
    }

    private <V> Iterator<HugeTask<V>> queryTask(Map<String, Object> conditions,
                                                long limit) {
        return this.submit(() -> {
            ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
            VertexLabel vl = this.graph.vertexLabel(TaskTransaction.TASK);
            query.eq(HugeKeys.LABEL, vl.id());
            for (Map.Entry<String, Object> entry : conditions.entrySet()) {
                PropertyKey pk = this.graph.propertyKey(entry.getKey());
                query.query(Condition.eq(pk.id(), entry.getValue()));
            }
            query.showHidden(true);
            if (limit != NO_LIMIT) {
                query.limit(limit);
            }
            Iterator<Vertex> vertices = this.tx().queryVertices(query);
            return new MapperIterator<>(vertices, HugeTask::fromVertex);
        });
    }

    private <V> V submit(Runnable runnable) {
        return this.submit(Executors.callable(runnable, null));
    }

    private <V> V submit(Callable<V> callable) {
        try {
            return this.dbExecutor.submit(callable).get();
        } catch (Exception e) {
            throw new HugeException("Failed to update/query TaskStore", e);
        }
    }

    private static class TaskTransaction extends GraphTransaction {

        public static final String TASK = P.TASK;

        public TaskTransaction(HugeGraph graph, BackendStore store) {
            super(graph, store);
            this.autoCommit(true);
        }

        public HugeVertex constructVertex(HugeTask<?> task) {
            return this.constructVertex(false, task.asArray());
        }

        public boolean indexValueChanged(Vertex oldV, HugeVertex newV) {
            if (!oldV.value(P.STATUS).equals(newV.value(P.STATUS))) {
                return true;
            }
            return false;
        }

        protected void initSchema() {
            HugeGraph graph = this.graph();
            VertexLabel label = graph.schemaTransaction().getVertexLabel(TASK);
            if (label != null) {
                return;
            }

            SchemaManager schema = graph.schema();

            String[] properties = this.initProperties();

            // Create vertex label '~task'
            label = schema.vertexLabel(TASK)
                          .properties(properties)
                          .useCustomizeNumberId()
                          .nullableKeys(P.DESCRIPTION, P.UPDATE,
                                        P.INPUT, P.RESULT)
                          .enableLabelIndex(true)
                          .build();
            graph.schemaTransaction().addVertexLabel(label);

            // Create index
            this.createIndex(label, P.STATUS);
        }

        private String[] initProperties() {
            List<String> props = new ArrayList<>();

            props.add(createPropertyKey(P.TYPE));
            props.add(createPropertyKey(P.NAME));
            props.add(createPropertyKey(P.CALLABLE));
            props.add(createPropertyKey(P.DESCRIPTION));
            props.add(createPropertyKey(P.STATUS, DataType.BYTE));
            props.add(createPropertyKey(P.PROGRESS, DataType.INT));
            props.add(createPropertyKey(P.CREATE, DataType.DATE));
            props.add(createPropertyKey(P.UPDATE, DataType.DATE));
            props.add(createPropertyKey(P.RETRIES, DataType.INT));
            props.add(createPropertyKey(P.INPUT));
            props.add(createPropertyKey(P.RESULT));

            return props.toArray(new String[0]);
        }

        private String createPropertyKey(String name) {
            return this.createPropertyKey(name, DataType.TEXT);
        }

        private String createPropertyKey(String name, DataType dataType) {
            return this.createPropertyKey(name, dataType, Cardinality.SINGLE);
        }

        private String createPropertyKey(String name, DataType dataType,
                                         Cardinality cardinality) {
            HugeGraph graph = this.graph();
            SchemaManager schema = graph.schema();
            PropertyKey propertyKey = schema.propertyKey(name)
                                            .dataType(dataType)
                                            .cardinality(cardinality)
                                            .build();
            graph.schemaTransaction().addPropertyKey(propertyKey);
            return name;
        }

        private IndexLabel createIndex(VertexLabel label, String field) {
            HugeGraph graph = this.graph();
            SchemaManager schema = graph.schema();
            String name = Hidden.hide("task-index-by-" + field);
            IndexLabel indexLabel = schema.indexLabel(name)
                                          .on(HugeType.VERTEX_LABEL, TASK)
                                          .by(field)
                                          .build();
            graph.schemaTransaction().addIndexLabel(label, indexLabel);
            return indexLabel;
        }
    }
}
