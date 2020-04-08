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

import org.slf4j.Logger;

import com.baidu.hugegraph.task.HugeTask;
import com.baidu.hugegraph.task.TaskCallable;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public abstract class Job<T> extends TaskCallable<T> {

    private static final Logger LOG = Log.logger(HugeTask.class);

    private static final String ERROR_MAX_LEN = "Failed to commit changes: " +
                                                "The max length of bytes is";

    private volatile long lastSaveTime = System.currentTimeMillis();
    private volatile long saveInterval = 1000 * 30;

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

    public void setMinSaveInterval(long seconds) {
        E.checkArgument(seconds > 0,
                        "Must set interval > 0, bug got '%s'", seconds);
        this.saveInterval = seconds * 1000L;
    }

    public void updateProgress(int progress) {
        HugeTask<T> task = this.task();
        task.progress(progress);

        long elapse = System.currentTimeMillis() - this.lastSaveTime;
        if (elapse > this.saveInterval) {
            this.save();
            this.lastSaveTime = System.currentTimeMillis();
        }
    }

    public int progress() {
        HugeTask<T> task = this.task();
        return task.progress();
    }

    private void save() {
        HugeTask<T> task = this.task();
        task.updateTime(new Date());
        try {
            this.scheduler().save(task);
        } catch (Throwable e) {
            if (task.completed()) {
                /*
                 * Failed to save task and the status is stable(can't be update)
                 * just log the task, and try again.
                 */
                LOG.error("Failed to save task with error \"{}\": {}", e, task);
                if (e.getMessage().contains(ERROR_MAX_LEN)) {
                    task.failSave(e);
                    this.scheduler().save(task);
                    return;
                }
            }
            throw e;
        }
    }
}
