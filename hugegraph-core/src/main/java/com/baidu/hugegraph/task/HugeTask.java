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

import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.slf4j.Logger;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.type.define.SerialEnum;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.ImmutableSet;

public class HugeTask<V> extends FutureTask<V> {

    private static final Logger LOG = Log.logger(HugeTask.class);
    private static final Set<Status> COMPLETED_STATUSES =
            ImmutableSet.of(Status.SUCCESS, Status.CANCELLED, Status.FAILED);

    private final HugeTaskCallable<V> callable;

    private String type;
    private String name;
    private final Id id;
    private final Id parent;
    private List<Id> children;
    private String description;
    private Date create;
    private volatile Status status;
    private volatile int progress;
    private volatile Date update;
    private volatile int retries;
    private volatile String input;
    private volatile String result;

    public HugeTask(Id id, Id parent, String callable, String input) {
        this(id, parent, HugeTaskCallable.fromClass(callable));
        this.input = input;
    }

    public HugeTask(Id id, Id parent, HugeTaskCallable<V> callable) {
        super(callable);

        E.checkArgumentNotNull(id, "Task id can't be null");
        E.checkArgument(id.number(), "Invalid task id type, it must be number");

        assert callable != null;
        this.callable = callable;
        this.type = null;
        this.name = null;
        this.id = id;
        this.parent = parent;
        this.children = null;
        this.description = null;
        this.status = Status.NEW;
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

    public List<Id> children() {
        return Collections.unmodifiableList(this.children);
    }

    public void child(Id id) {
        if (this.children == null) {
            this.children = new ArrayList<>();
        }
        this.children.add(id);
    }

    public Status status() {
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
        this.input = input;
    }

    public String input() {
        return this.input;
    }

    public String result() {
        return this.result;
    }

    public boolean completed() {
        return COMPLETED_STATUSES.contains(this.status);
    }

    @Override
    public String toString() {
        return String.format("HugeTask(%s)%s", this.id, this.asMap());
    }

    @Override
    public void run() {
        assert this.status.code() < Status.RUNNING.code();
        this.status(Status.RUNNING);
        super.run();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        try {
            return super.cancel(mayInterruptIfRunning);
        } finally {
            this.status(Status.CANCELLED);
        }
    }

    @Override
    protected void done() {
        try {
            this.callable.done();
        } catch (Throwable e) {
            LOG.error("An exception occurred when calling done()", e);
        } finally {
            this.callable.scheduler().remove(this.id);
        }
    }

    @Override
    protected void set(V v) {
        this.status(Status.SUCCESS);
        if (v != null) {
            this.result = v.toString();
        }
        super.set(v);
    }

    @Override
    protected void setException(Throwable e) {
        if (!(this.status == Status.CANCELLED &&
              e instanceof InterruptedException)) {
            LOG.warn("An exception occurred when running task: {}",
                     this.id(), e);
            // Update status to FAILED if exception occurred(not interrupted)
            this.status(Status.FAILED);
            this.result = e.toString();
        }
        super.setException(e);
    }

    protected HugeTaskCallable<V> callable() {
        return this.callable;
    }

    protected void status(Status status) {
        this.status = status;
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
            case P.DESCRIPTION:
                this.description = (String) value;
                break;
            case P.STATUS:
                this.status(SerialEnum.fromCode(Status.class, (byte) value));
                break;
            case P.PROGRESS:
                this.progress = (int) value;
                break;
            case P.CREATE:
                this.create = (Date) value;
                break;
            case P.UPDATE:
                this.update = (Date) value;
                break;
            case P.RETRIES:
                this.retries = (int) value;
                break;
            case P.INPUT:
                this.input = (String) value;
                break;
            case P.RESULT:
                this.result = (String) value;
                break;
            case P.CALLABLE:
                // pass
                break;
            default:
                throw new AssertionError("Unsupported key: " + key);
        }
    }

    protected Object[] asArray() {
        E.checkState(this.type != null, "Task type can't be null");
        E.checkState(this.name != null, "Task name can't be null");

        List<Object> list = new ArrayList<>(24);

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
        E.checkState(this.type != null, "Task type can't be null");
        E.checkState(this.name != null, "Task name can't be null");

        Map<String, Object> map = new HashMap<>();

        map.put(Hidden.unHide(P.ID), this.id);

        map.put(Hidden.unHide(P.TYPE), this.type);
        map.put(Hidden.unHide(P.NAME), this.name);
        map.put(Hidden.unHide(P.CALLABLE), this.callable.getClass().getName());
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
        if (this.input != null) {
            map.put(Hidden.unHide(P.INPUT), this.input);
        }
        if (this.result != null) {
            map.put(Hidden.unHide(P.RESULT), this.result);
        }

        return map;
    }

    public static <V> HugeTask<V> fromVertex(Vertex vertex) {
        String callableName = vertex.value(P.CALLABLE);
        HugeTaskCallable<V> callable;
        try {
            callable = HugeTaskCallable.fromClass(callableName);
        } catch (Exception e) {
            callable = HugeTaskCallable.empty(e);
        }

        HugeTask<V> task = new HugeTask<>((Id) vertex.id(), null, callable);
        for (Iterator<VertexProperty<Object>> itor = vertex.properties();
             itor.hasNext();) {
            VertexProperty<Object> prop = itor.next();
            task.property(prop.key(), prop.value());
        }
        return task;
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
