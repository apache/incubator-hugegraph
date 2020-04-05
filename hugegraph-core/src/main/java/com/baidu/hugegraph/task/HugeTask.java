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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.FutureTask;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.exception.LimitExceedException;
import com.baidu.hugegraph.type.define.SerialEnum;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.Log;

public class HugeTask<V> extends FutureTask<V> {

    private static final Logger LOG = Log.logger(HugeTask.class);

    private static final int MAX_PROPERTY_LENGTH = BytesBuffer.STRING_LEN_MAX;

    private final TaskCallable<V> callable;

    private String type;
    private String name;
    private final Id id;
    private final Id parent;
    private Set<Id> dependencies;
    private String description;
    private Date create;
    private volatile TaskStatus status;
    private volatile int progress;
    private volatile Date update;
    private volatile int retries;
    private volatile String input;
    private volatile String result;

    public HugeTask(Id id, Id parent, String callable, String input) {
        this(id, parent, TaskCallable.fromClass(callable));
        this.input = input;
    }

    public HugeTask(Id id, Id parent, TaskCallable<V> callable) {
        super(callable);

        E.checkArgumentNotNull(id, "Task id can't be null");
        E.checkArgument(id.number(), "Invalid task id type, it must be number");

        assert callable != null;
        this.callable = callable;
        this.type = null;
        this.name = null;
        this.id = id;
        this.parent = parent;
        this.dependencies = null;
        this.description = null;
        this.status = TaskStatus.NEW;
        this.progress = 0;
        this.create = new Date();
        this.update = null;
        this.retries = 0;
        this.input = null;
        this.result = null;
    }

    public Id id() {
        return this.id;
    }

    public Id parent() {
        return this.parent;
    }

    public Set<Id> dependencies() {
        return Collections.unmodifiableSet(this.dependencies);
    }

    public void depends(Id id) {
        E.checkState(this.status == TaskStatus.NEW,
                     "Can't add dependency in status '%s'", this.status);
        if (this.dependencies == null) {
            this.dependencies = InsertionOrderUtil.newSet();
        }
        this.dependencies.add(id);
    }

    public TaskStatus status() {
        return this.status;
    }

    public void type(String type) {
        this.type = type;
    }

    public String type() {
        return this.type;
    }

    public void name(String name) {
        this.name = name;
    }

    public String name() {
        return this.name;
    }

    public void description(String description) {
        this.description = description;
    }

    public String description() {
        return this.description;
    }

    public void progress(int progress) {
        this.progress = progress;
    }

    public int progress() {
        return this.progress;
    }

    public void createTime(Date create) {
        this.create = create;
    }

    public Date createTime() {
        return this.create;
    }

    public void updateTime(Date update) {
        this.update = update;
    }

    public Date updateTime() {
        return this.update;
    }

    public void retry() {
        ++this.retries;
    }

    public int retries() {
        return this.retries;
    }

    public void input(String input) {
        checkPropertySize(input, P.unhide(P.INPUT));
        this.input = input;
    }

    public String input() {
        return this.input;
    }

    public String result() {
        return this.result;
    }

    public boolean completed() {
        return TaskStatus.COMPLETED_STATUSES.contains(this.status);
    }

    public boolean cancelled() {
        return this.status == TaskStatus.CANCELLED || this.isCancelled();
    }

    @Override
    public String toString() {
        return String.format("HugeTask(%s)%s", this.id, this.asMap());
    }

    @Override
    public void run() {
        if (this.cancelled()) {
            // Scheduled task is running after cancelled
            return;
        }
        try {
            assert this.status.code() < TaskStatus.RUNNING.code() : this.status;
            if (this.checkDependenciesSuccess()) {
                this.status(TaskStatus.RUNNING);
                super.run();
            }
        } catch (Throwable e) {
            this.setException(e);
        } finally {
            LOG.debug("Task is finished {}", this);
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        // NOTE: Gremlin sleep() can't be interrupted by default
        // https://mrhaki.blogspot.com/2016/10/groovy-goodness-interrupted-sleeping.html
        boolean cancelled = super.cancel(mayInterruptIfRunning);
        if (!cancelled) {
            return cancelled;
        }

        try {
            if (this.status(TaskStatus.CANCELLED)) {
                // Callback for saving status to store
                this.callable.cancelled();
            } else {
                // Maybe the worker is still running then set status SUCCESS
                cancelled = false;
            }
        } catch (Throwable e) {
            LOG.error("An exception occurred when calling cancelled()", e);
        }
        return cancelled;
    }

    public boolean fail(Throwable e) {
        E.checkNotNull(e, "exception");
        if (!(this.cancelled() && HugeException.isInterrupted(e))) {
            LOG.warn("An exception occurred when running task: {}",
                     this.id(), e);
            // Update status to FAILED if exception occurred(not interrupted)
            if (this.status(TaskStatus.FAILED)) {
                this.result = e.toString();
                return true;
            }
        }
        return false;
    }

    public void failSave(Throwable e) {
        if (!this.fail(e)) {
            // Can't update status, just set result to error message
            this.result = e.toString();
        }
    }

    @Override
    protected void done() {
        try {
            // Callback for saving status to store
            this.callable.done();
        } catch (Throwable e) {
            LOG.error("An exception occurred when calling done()", e);
        } finally {
            this.callable.scheduler().remove(this.id);
        }
    }

    @Override
    protected void set(V v) {
        String result = JsonUtil.toJson(v);
        checkPropertySize(result, P.unhide(P.RESULT));
        if (this.status(TaskStatus.SUCCESS) && v != null) {
            this.result = result;
        }
        // Will call done() and may cause to save to store
        super.set(v);
    }

    @Override
    protected void setException(Throwable e) {
        this.fail(e);
        super.setException(e);
    }

    protected boolean checkDependenciesSuccess() {
        if (this.dependencies == null || this.dependencies.isEmpty()) {
            return true;
        }
        for (Id dependency : this.dependencies) {
            HugeTask<?> task = this.callable.scheduler().task(dependency);
            if (!task.completed()) {
                // Dependent task not completed, re-schedule self
                this.callable.scheduler().schedule(this);
                return false;
            } else if (task.status() == TaskStatus.CANCELLED) {
                this.status(TaskStatus.CANCELLED);
                this.result = String.format(
                              "Cancelled due to dependent task '%s' cancelled",
                              dependency);
                this.done();
                return false;
            } else if (task.status() == TaskStatus.FAILED) {
                this.status(TaskStatus.FAILED);
                this.result = String.format(
                              "Failed due to dependent task '%s' failed",
                              dependency);
                this.done();
                return false;
            }
        }
        return true;
    }

    protected TaskCallable<V> callable() {
        E.checkNotNull(this.callable, "callable");
        return this.callable;
    }

    protected synchronized boolean status(TaskStatus status) {
        E.checkNotNull(status, "status");
        if (status.code() > TaskStatus.NEW.code()) {
            E.checkState(this.type != null, "Task type can't be null");
            E.checkState(this.name != null, "Task name can't be null");
        }
        if (!this.completed()) {
            this.status = status;
            return true;
        }
        return false;
    }

    protected void checkProperties() {
        checkPropertySize(this.input, P.unhide(P.INPUT));
        checkPropertySize(this.result, P.unhide(P.RESULT));
    }

    protected void property(String key, Object value) {
        E.checkNotNull(key, "property key");
        switch (key) {
            case P.TYPE:
                this.type = (String) value;
                break;
            case P.NAME:
                this.name = (String) value;
                break;
            case P.CALLABLE:
                // pass
                break;
            case P.STATUS:
                this.status = SerialEnum.fromCode(TaskStatus.class,
                                                  (byte) value);
                break;
            case P.PROGRESS:
                this.progress = (int) value;
                break;
            case P.CREATE:
                this.create = (Date) value;
                break;
            case P.RETRIES:
                this.retries = (int) value;
                break;
            case P.DESCRIPTION:
                this.description = (String) value;
                break;
            case P.UPDATE:
                this.update = (Date) value;
                break;
            case P.DEPENDENCIES:
                @SuppressWarnings("unchecked")
                Set<Long> values = (Set<Long>) value;
                this.dependencies = values.stream().map(IdGenerator::of)
                                                   .collect(toOrderSet());
                break;
            case P.INPUT:
                this.input = (String) value;
                break;
            case P.RESULT:
                this.result = (String) value;
                break;
            default:
                throw new AssertionError("Unsupported key: " + key);
        }
    }

    protected Object[] asArray() {
        E.checkState(this.type != null, "Task type can't be null");
        E.checkState(this.name != null, "Task name can't be null");

        List<Object> list = new ArrayList<>(28);

        list.add(T.label);
        list.add(P.TASK);

        list.add(T.id);
        list.add(this.id);

        list.add(P.TYPE);
        list.add(this.type);

        list.add(P.NAME);
        list.add(this.name);

        list.add(P.CALLABLE);
        list.add(this.callable.getClass().getName());

        list.add(P.STATUS);
        list.add(this.status.code());

        list.add(P.PROGRESS);
        list.add(this.progress);

        list.add(P.CREATE);
        list.add(this.create);

        list.add(P.RETRIES);
        list.add(this.retries);

        if (this.description != null) {
            list.add(P.DESCRIPTION);
            list.add(this.description);
        }

        if (this.update != null) {
            list.add(P.UPDATE);
            list.add(this.update);
        }

        if (this.dependencies != null) {
            list.add(P.DEPENDENCIES);
            list.add(this.dependencies.stream().map(Id::asLong)
                                               .collect(toOrderSet()));
        }

        if (this.input != null) {
            list.add(P.INPUT);
            list.add(this.input);
        }

        if (this.result != null) {
            list.add(P.RESULT);
            list.add(this.result);
        }

        return list.toArray();
    }

    public Map<String, Object> asMap() {
        return this.asMap(true);
    }

    public Map<String, Object> asMap(boolean withDetails) {
        E.checkState(this.type != null, "Task type can't be null");
        E.checkState(this.name != null, "Task name can't be null");

        Map<String, Object> map = new HashMap<>();

        map.put(Hidden.unHide(P.ID), this.id);

        map.put(Hidden.unHide(P.TYPE), this.type);
        map.put(Hidden.unHide(P.NAME), this.name);
        map.put(Hidden.unHide(P.STATUS), this.status.string());
        map.put(Hidden.unHide(P.PROGRESS), this.progress);
        map.put(Hidden.unHide(P.CREATE), this.create);
        map.put(Hidden.unHide(P.RETRIES), this.retries);

        if (this.description != null) {
            map.put(Hidden.unHide(P.DESCRIPTION), this.description);
        }
        if (this.update != null) {
            map.put(Hidden.unHide(P.UPDATE), this.update);
        }
        if (this.dependencies != null) {
            Set<Long> value = this.dependencies.stream().map(Id::asLong)
                                                        .collect(toOrderSet());
            map.put(Hidden.unHide(P.DEPENDENCIES), value);
        }

        if (withDetails) {
            map.put(Hidden.unHide(P.CALLABLE),
                    this.callable.getClass().getName());
            if (this.input != null) {
                map.put(Hidden.unHide(P.INPUT), this.input);
            }
            if (this.result != null) {
                map.put(Hidden.unHide(P.RESULT), this.result);
            }
        }

        return map;
    }

    public static <V> HugeTask<V> fromVertex(Vertex vertex) {
        String callableName = vertex.value(P.CALLABLE);
        TaskCallable<V> callable;
        try {
            callable = TaskCallable.fromClass(callableName);
        } catch (Exception e) {
            callable = TaskCallable.empty(e);
        }

        HugeTask<V> task = new HugeTask<>((Id) vertex.id(), null, callable);
        for (Iterator<VertexProperty<Object>> iter = vertex.properties();
             iter.hasNext();) {
            VertexProperty<Object> prop = iter.next();
            task.property(prop.key(), prop.value());
        }
        return task;
    }

    private static <V> Collector<V, ?, Set<V>> toOrderSet() {
        return Collectors.toCollection(InsertionOrderUtil::newSet);
    }

    private static void checkPropertySize(String property, String name) {
        if (property != null && property.length() > MAX_PROPERTY_LENGTH) {
            throw new LimitExceedException(
                      "Task %s size %s exceeded limit %s bytes",
                      name, property.length(), MAX_PROPERTY_LENGTH);
        }
    }

    public static final class P {

        public static final String TASK = Hidden.hide("task");

        public static final String ID = T.id.getAccessor();
        public static final String LABEL = T.label.getAccessor();

        public static final String TYPE = "~task_type";
        public static final String NAME = "~task_name";
        public static final String CALLABLE = "~task_callable";
        public static final String DESCRIPTION = "~task_description";
        public static final String STATUS = "~task_status";
        public static final String PROGRESS = "~task_progress";
        public static final String CREATE = "~task_create";
        public static final String UPDATE = "~task_update";
        public static final String RETRIES = "~task_retries";
        public static final String INPUT = "~task_input";
        public static final String RESULT = "~task_result";
        public static final String DEPENDENCIES = "~task_dependencies";

        //public static final String PARENT = hide("parent");
        //public static final String CHILDREN = hide("children");

        public static String hide(String key) {
            return Hidden.hide("task" + "_" + key);
        }

        public static String unhide(String key) {
            final String prefix = Hidden.hide("task" + "_");
            if (key.startsWith(prefix)) {
                return key.substring(prefix.length());
            }
            return key;
        }
    }
}
