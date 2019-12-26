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

import org.apache.commons.collections.IteratorUtils;
import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.page.PageInfo;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.event.EventListener;
import com.baidu.hugegraph.exception.ConnectionException;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.iterator.ExtendableIterator;
import com.baidu.hugegraph.iterator.MapperIterator;
import com.baidu.hugegraph.iterator.WrappedIterator;
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

public class TaskScheduler {

    private final HugeGraph graph;
    private final ExecutorService taskExecutor;
    private final ExecutorService dbExecutor;

    private final EventListener eventListener;
    private final Map<Id, HugeTask<?>> tasks;

    private volatile TaskTransaction taskTx;

    private static final long NO_LIMIT = -1L;
    private static final long PAGE_SIZE = 500L;
    private static final long QUERY_INTERVAL = 100L;
    private static final int MAX_PENDING_TASKS = 10000;

    public TaskScheduler(HugeGraph graph,
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

        this.eventListener = this.listenChanges();
    }

    public HugeGraph graph() {
        return this.graph;
    }

    public int pendingTasks() {
        return this.tasks.size();
    }

    private TaskTransaction tx() {
        // NOTE: only the owner thread can access task tx
        if (this.taskTx == null) {
            synchronized (this) {
                if (this.taskTx == null) {
                    BackendStore store = this.graph.loadSystemStore();
                    TaskTransaction tx = new TaskTransaction(this.graph, store);
                    assert this.taskTx == null; // may be reentrant?
                    this.taskTx = tx;
                }
            }
        }
        assert this.taskTx != null;
        return this.taskTx;
    }

    private EventListener listenChanges() {
        // Listen store event: "store.inited"
        Set<String> storeEvents = ImmutableSet.of(Events.STORE_INITED);
        EventListener eventListener = event -> {
            // Ensure task schema create after system info initialized
            if (storeEvents.contains(event.name())) {
                this.call(() -> this.tx().initSchema());
                return true;
            }
            return false;
        };
        this.graph.loadSystemStore().provider().listen(eventListener);
        return eventListener;
    }

    private void unlistenChanges() {
        this.graph.loadSystemStore().provider().unlisten(this.eventListener);
    }

    public <V> void restoreTasks() {
        boolean supportsPaging = this.call(() -> {
            return this.tx().store().features().supportsQueryByPage();
        });
        // Restore 'RESTORING', 'RUNNING' and 'QUEUED' tasks in order.
        for (TaskStatus status : TaskStatus.PENDING_STATUSES) {
            String page = supportsPaging ? PageInfo.PAGE_NONE : null;
            do {
                Iterator<HugeTask<V>> iter;
                for (iter = this.findTask(status, PAGE_SIZE, page);
                     iter.hasNext();) {
                    HugeTask<V> task = iter.next();
                    this.restore(task);
                }
                if (page != null) {
                    page = PageInfo.pageInfo(iter);
                }
            } while (page != null);
        }
    }

    public <V> Future<?> restore(HugeTask<V> task) {
        E.checkArgumentNotNull(task, "Task can't be null");
        E.checkArgument(!this.tasks.containsKey(task.id()),
                        "Task '%s' is already in the queue", task.id());
        E.checkArgument(!task.isDone() && !task.completed(),
                        "No need to restore completed task '%s' with status %s",
                        task.id(), task.status());
        task.status(TaskStatus.RESTORING);
        task.retry();
        return this.submitTask(task);
    }

    public <V> Future<?> schedule(HugeTask<V> task) {
        E.checkArgumentNotNull(task, "Task can't be null");
        task.status(TaskStatus.QUEUED);
        return this.submitTask(task);
    }

    private <V> Future<?> submitTask(HugeTask<V> task) {
        int size = this.tasks.size() + 1;
        E.checkArgument(size <= MAX_PENDING_TASKS,
                        "Pending tasks size %s has exceeded the max limit %s",
                        size, MAX_PENDING_TASKS);
        this.initTaskCallable(task);
        this.tasks.put(task.id(), task);
        return this.taskExecutor.submit(task);
    }

    private <V> void initTaskCallable(HugeTask<V> task) {
        if (this.tasks.containsKey(task.id())) {
            // Assume initialized
            return;
        }
        TaskCallable<V> callable = task.callable();
        callable.scheduler(this);
        callable.task(task);
    }

    public <V> boolean cancel(HugeTask<V> task) {
        E.checkArgumentNotNull(task, "Task can't be null");
        boolean cancelled = false;
        if (!task.completed()) {
            /*
             * Task may be loaded from backend store and not initialized. like:
             * A task is completed but failed to save in the last step,
             * resulting in the status of the task not being updated to storage,
             * the task is not in memory, so it's not initialized when canceled.
             */
            this.initTaskCallable(task);

            cancelled = task.cancel(true);
            this.remove(task.id());
        }
        assert task.completed();
        return cancelled;
    }

    protected void remove(Id id) {
        HugeTask<?> task = this.tasks.remove(id);
        assert task == null || task.completed() || task.isCancelled();
    }

    public <V> void save(HugeTask<V> task) {
        E.checkArgumentNotNull(task, "Task can't be null");
        // Check property size
        task.checkProperties();
        // Do save
        this.call(() -> {
            // Construct vertex from task
            HugeVertex vertex = this.tx().constructVertex(task);
            // Delete index of old vertex
            this.tx().deleteIndex(vertex);
            // Add or update task info in backend store, stale index might exist
            return this.tx().addVertex(vertex);
        });
    }

    public boolean close() {
        this.unlistenChanges();
        if (!this.dbExecutor.isShutdown()) {
            this.call(() -> {
                try {
                    this.tx().close();
                } catch (ConnectionException ignored) {
                    // ConnectionException means no connection established
                }
                this.graph.closeTx();
            });
        }
        return true;
    }

    public <V> HugeTask<V> task(Id id) {
        E.checkArgumentNotNull(id, "Parameter task id can't be null");
        @SuppressWarnings("unchecked")
        HugeTask<V> task = (HugeTask<V>) this.tasks.get(id);
        if (task != null) {
            return task;
        }
        return this.findTask(id);
    }

    public <V> Iterator<HugeTask<V>> tasks(List<Id> ids) {
        List<Id> taskIdsNotInMem = new ArrayList<>();
        List<HugeTask<V>> taskInMem = new ArrayList<>();
        for (Id id : ids) {
            @SuppressWarnings("unchecked")
            HugeTask<V> task = (HugeTask<V>) this.tasks.get(id);
            if (task != null) {
                taskInMem.add(task);
            } else {
                taskIdsNotInMem.add(id);
            }
        }
        ExtendableIterator<HugeTask<V>> iterator;
        if (taskInMem.isEmpty()) {
            iterator = new ExtendableIterator<>();
        } else {
            iterator = new ExtendableIterator<>(taskInMem.iterator());
        }
        iterator.extend(this.findTasks(taskIdsNotInMem));
        return iterator;
    }

    public <V> HugeTask<V> findTask(Id id) {
        HugeTask<V> result =  this.call(() -> {
            HugeTask<V> task = null;
            Iterator<Vertex> vertices = this.tx().queryVertices(id);
            try {
                if (vertices.hasNext()) {
                    task = HugeTask.fromVertex(vertices.next());
                    assert !vertices.hasNext();
                }
            } finally {
                CloseableIterator.closeIterator(vertices);
            }
            return task;
        });
        if (result == null) {
            throw new NotFoundException("Can't find task with id '%s'", id);
        }
        return result;
    }

    public <V> Iterator<HugeTask<V>> findTasks(List<Id> ids) {
        return this.queryTask(ids);
    }

    public <V> Iterator<HugeTask<V>> findAllTask(long limit, String page) {
        return this.queryTask(ImmutableMap.of(), limit, page);
    }

    public <V> Iterator<HugeTask<V>> findTask(TaskStatus status,
                                              long limit, String page) {
        return this.queryTask(P.STATUS, status.code(), limit, page);
    }

    public <V> HugeTask<V> deleteTask(Id id) {
        HugeTask<?> task = this.tasks.get(id);
        /*
         * Tasks are removed from memory after completed at most time,
         * but there is a tiny gap between tasks are completed and
         * removed from memory.
         * We assume tasks only in memory may be incomplete status,
         * in fact, it is also possible to appear on the backend tasks
         * when the database status is inconsistent.
         */
        if (task != null) {
            E.checkArgument(task.completed(),
                            "Can't delete incomplete task '%s' in status %s" +
                            ", Please try to cancel the task first",
                            id, task.status());
            this.remove(id);
        }
        return this.call(() -> {
            HugeVertex vertex = null;
            Iterator<Vertex> vertices = this.tx().queryVertices(id);
            try {
                if (vertices.hasNext()) {
                    vertex = (HugeVertex) vertices.next();
                    assert !vertices.hasNext();
                } else {
                    return null;
                }
            } finally {
                CloseableIterator.closeIterator(vertices);
            }
            HugeTask<V> result = HugeTask.fromVertex(vertex);
            E.checkState(result.completed(),
                         "Can't delete incomplete task '%s' in status %s",
                         id, result.status());
            this.tx().removeVertex(vertex);
            return result;
        });
    }

    public <V> HugeTask<V> waitUntilTaskCompleted(Id id, long seconds)
                                                  throws TimeoutException {
        long passes = seconds * 1000 / QUERY_INTERVAL;
        HugeTask<V> task = null;
        for (long pass = 0;; pass++) {
            try {
                task = this.task(id);
            } catch (NotFoundException e) {
                if (task != null && task.completed()) {
                    assert task.id().asLong() < 0L : task.id();
                    return task;
                }
                throw e;
            }
            if (task.completed()) {
                return task;
            }
            if (pass >= passes) {
                break;
            }
            try {
                Thread.sleep(QUERY_INTERVAL);
            } catch (InterruptedException ignored) {
                // Ignore InterruptedException
            }
        }
        throw new TimeoutException(String.format(
                  "Task '%s' was not completed in %s seconds", id, seconds));
    }

    public void waitUntilAllTasksCompleted(long seconds)
                                           throws TimeoutException {
        long passes = seconds * 1000 / QUERY_INTERVAL;
        int taskSize = 0;
        for (long pass = 0;; pass++) {
            taskSize = this.pendingTasks();
            if (taskSize == 0) {
                return;
            }
            if (pass >= passes) {
                break;
            }
            try {
                Thread.sleep(QUERY_INTERVAL);
            } catch (InterruptedException ignored) {
                // Ignore InterruptedException
            }
        }
        throw new TimeoutException(String.format(
                  "There are still %s incomplete tasks after %s seconds",
                  taskSize, seconds));
    }

    private <V> Iterator<HugeTask<V>> queryTask(String key, Object value,
                                                long limit, String page) {
        return this.queryTask(ImmutableMap.of(key, value), limit, page);
    }

    private <V> Iterator<HugeTask<V>> queryTask(Map<String, Object> conditions,
                                                long limit, String page) {
        return this.call(() -> {
            ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
            if (page != null) {
                query.page(page);
            }
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
            Iterator<HugeTask<V>> tasks =
                    new MapperIterator<>(vertices, HugeTask::fromVertex);
            // Convert iterator to list to avoid across thread tx accessed
            return new ListIterator<>(tasks);
        });
    }

    private <V> Iterator<HugeTask<V>> queryTask(List<Id> ids) {
        return this.call(() -> {
            Object[] idArray = ids.toArray(new Id[ids.size()]);
            Iterator<Vertex> vertices = this.tx().queryVertices(idArray);
            Iterator<HugeTask<V>> tasks =
                    new MapperIterator<>(vertices, HugeTask::fromVertex);
            // Convert iterator to list to avoid across thread tx accessed
            return new ListIterator<>(tasks);
        });
    }

    private <V> V call(Runnable runnable) {
        return this.call(Executors.callable(runnable, null));
    }

    private <V> V call(Callable<V> callable) {
        // Ensure all db operations are executed in dbExecutor thread(s)
        try {
            return this.dbExecutor.submit(callable).get();
        } catch (Throwable e) {
            throw new HugeException("Failed to update/query TaskStore: %s",
                                    e, e.toString());
        }
    }

    private static class TaskTransaction extends GraphTransaction {

        public static final String TASK = P.TASK;

        public TaskTransaction(HugeGraph graph, BackendStore store) {
            super(graph, store);
            this.autoCommit(true);
        }

        public HugeVertex constructVertex(HugeTask<?> task) {
            if (this.graph().schemaTransaction().getVertexLabel(TASK) == null) {
                throw new HugeException("Schema is missing for task(%s) '%s'",
                                        task.id(), task.name());
            }
            return this.constructVertex(false, task.asArray());
        }

        public boolean indexValueChanged(Vertex oldV, HugeVertex newV) {
            if (!oldV.value(P.STATUS).equals(newV.value(P.STATUS))) {
                return true;
            }
            return false;
        }

        private void deleteIndex(HugeVertex vertex) {
            // Delete the old record if exist
            Iterator<Vertex> old = this.queryVertices(vertex.id());
            try {
                if (old.hasNext()) {
                    HugeVertex oldV = (HugeVertex) old.next();
                    assert !old.hasNext();
                    if (this.indexValueChanged(oldV, vertex)) {
                        // Only delete vertex index if index value changed
                        this.indexTransaction().updateVertexIndex(oldV, true);
                    }
                }
            } finally {
                CloseableIterator.closeIterator(old);
            }
        }

        protected void initSchema() {
            HugeGraph graph = this.graph();
            VertexLabel label = graph.schemaTransaction().getVertexLabel(TASK);
            if (label != null) {
                return;
            }

            String[] properties = this.initProperties();

            // Create vertex label '~task'
            label = graph.schema().vertexLabel(TASK)
                         .properties(properties)
                         .useCustomizeNumberId()
                         .nullableKeys(P.DESCRIPTION, P.UPDATE,
                                       P.INPUT, P.RESULT, P.DEPENDENCIES)
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
            props.add(createPropertyKey(P.DEPENDENCIES, DataType.LONG,
                                        Cardinality.SET));

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

    // TODO: move to common module
    private static class ListIterator<V> extends WrappedIterator<V> {

        private final Iterator<V> origin;
        private final Iterator<V> iterator;

        public ListIterator(Iterator<V> origin) {
            this.origin = origin;
            @SuppressWarnings("unchecked")
            List<V> results = IteratorUtils.toList(origin);
            this.iterator = results.iterator();
        }

        @Override
        protected boolean fetch() {
            assert this.current == none();
            if (!this.iterator.hasNext()) {
                return false;
            }
            this.current = this.iterator.next();
            return true;
        }

        @Override
        protected Iterator<?> originIterator() {
            return this.origin;
        }
    }
}
