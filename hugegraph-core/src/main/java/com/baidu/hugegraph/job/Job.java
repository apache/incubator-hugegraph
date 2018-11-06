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

import java.util.Date;

import com.baidu.hugegraph.task.HugeTask;
import com.baidu.hugegraph.task.TaskCallable;

public abstract class Job<T> extends TaskCallable<T> {

    public abstract String type();

    public abstract T execute() throws Exception;

    @Override
    public T call() throws Exception {
        this.save();
        return this.execute();
    }

    @Override
    protected void done() {
        this.save();
    }

    @Override
    protected void cancelled() {
        this.save();
    }

    private void save() {
        HugeTask<T> task = this.task();
        task.updateTime(new Date());
        this.scheduler().save(task);
    }
}
