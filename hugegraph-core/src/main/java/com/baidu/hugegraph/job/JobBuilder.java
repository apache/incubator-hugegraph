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

package com.baidu.hugegraph.job;

import java.util.Set;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.task.HugeTask;
import com.baidu.hugegraph.task.TaskScheduler;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;

public class JobBuilder<T> {

    private final HugeGraph graph;

    private String name;
    private String input;
    private Job<T> job;
    private Set<Id> dependencies;

    public static <T> JobBuilder<T> of(final HugeGraph graph) {
        return new JobBuilder<>(graph);
    }

    public JobBuilder(final HugeGraph graph) {
        this.graph = graph;
    }

    public JobBuilder<T> name(String name) {
        this.name = name;
        return this;
    }

    public JobBuilder<T> input(String input) {
        this.input = input;
        return this;
    }

    public JobBuilder<T> job(Job<T> job) {
        this.job = job;
        return this;
    }

    public JobBuilder<T> dependencies(Set<Id> dependencies) {
        this.dependencies = dependencies;
        return this;
    }

    public HugeTask<T> schedule() {
        E.checkArgumentNotNull(this.name, "Job name can't be null");
        E.checkArgumentNotNull(this.job, "Job can't be null");

        HugeTask<T> task = new HugeTask<>(this.genTaskId(), null, this.job);
        task.type(this.job.type());
        task.name(this.name);
        if (this.input != null) {
            task.input(this.input);
        }
        if (this.dependencies != null && !this.dependencies.isEmpty()) {
            for (Id depend : this.dependencies) {
                task.depends(depend);
            }
        }

        TaskScheduler scheduler = this.graph.taskScheduler();
        scheduler.schedule(task);
        scheduler.save(task);

        return task;
    }

    private Id genTaskId() {
        return this.graph.schemaTransaction().store().nextId(HugeType.TASK);
    }
}
