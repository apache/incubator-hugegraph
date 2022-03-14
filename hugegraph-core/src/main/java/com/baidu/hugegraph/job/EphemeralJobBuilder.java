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

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.task.HugeTask;
import com.baidu.hugegraph.task.TaskManager;
import com.baidu.hugegraph.task.TaskScheduler;
import com.baidu.hugegraph.util.E;
import com.google.common.base.Strings;

public class EphemeralJobBuilder<V> {

    private final HugeGraph graph;

    private String name;
    private String input;
    private String context;
    private EphemeralJob<V> job;

    // Use negative task id for ephemeral task
    private static int ephemeralTaskId = -1;

    public static <T> EphemeralJobBuilder<T> of(final HugeGraph graph) {
        return new EphemeralJobBuilder<>(graph);
    }

    public EphemeralJobBuilder(final HugeGraph graph) {
        this.graph = graph;
    }

    public EphemeralJobBuilder<V> name(String name) {
        this.name = name;
        return this;
    }

    public EphemeralJobBuilder<V> input(String input) {
        this.input = input;
        return this;
    }

    public EphemeralJobBuilder<V> job(EphemeralJob<V> job) {
        this.job = job;
        return this;
    }

    public EphemeralJobBuilder<V> context(String context) {
        this.context = Strings.isNullOrEmpty(context) 
            ? TaskManager.getContext(true)
            : context;
        return this;
    }

    public HugeTask<V> schedule() {
        E.checkArgumentNotNull(this.name, "Job name can't be null");
        E.checkArgumentNotNull(this.job, "Job can't be null");

        HugeTask<V> task = new HugeTask<>(this.genTaskId(), null, this.job);
        task.type(this.job.type());
        task.name(this.name);
        task.context(this.context);
        if (this.input != null) {
            task.input(this.input);
        }

        TaskScheduler scheduler = this.graph.taskScheduler();
        scheduler.schedule(task);

        return task;
    }

    private Id genTaskId() {
        if (ephemeralTaskId >= 0) {
            ephemeralTaskId = -1;
        }
        return IdGenerator.of(ephemeralTaskId--);
    }
}
