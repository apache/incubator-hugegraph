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

package com.baidu.hugegraph.example;

import java.util.Iterator;

import org.apache.commons.collections.IteratorUtils;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.task.HugeTask;
import com.baidu.hugegraph.task.TaskStatus;
import com.baidu.hugegraph.task.TaskCallable;
import com.baidu.hugegraph.task.TaskManager;
import com.baidu.hugegraph.task.TaskScheduler;
import com.baidu.hugegraph.util.Log;

public class TaskExample {

    private static final Logger LOG = Log.logger(TaskExample.class);

    public static void main(String[] args) throws InterruptedException {
        LOG.info("TaskExample start!");

        HugeGraph graph = ExampleUtil.loadGraph();

        Id id = IdGenerator.of(8);
        String callable = "com.baidu.hugegraph.example.TaskExample$TestTask";
        HugeTask<?> task = new HugeTask<>(id, null, callable, "test-parameter");
        task.type("type-1");
        task.name("test-task");

        TaskScheduler scheduler = TaskManager.instance().getScheduler(graph);
        scheduler.schedule(task);
        scheduler.save(task);
        Iterator<HugeTask<Object>> iter;
        iter = scheduler.findTask(TaskStatus.RUNNING, -1);
        System.out.println(">>>> running task: " + IteratorUtils.toList(iter));

        Thread.sleep(TestTask.UNIT * 33);
        task.cancel(true);
        Thread.sleep(TestTask.UNIT * 1);
        scheduler.save(task);

        // Find task not finished(actually it should be RUNNING)
        iter = scheduler.findTask(TaskStatus.CANCELLED, -1);
        assert iter.hasNext();
        task = iter.next();

        System.out.println(">>>> task may be interrupted");

        Thread.sleep(TestTask.UNIT * 10);
        System.out.println(">>>> restore task...");
        scheduler.restore(task);
        Thread.sleep(TestTask.UNIT * 80);
        scheduler.save(task);

        iter = scheduler.findTask(TaskStatus.SUCCESS, -1);
        assert iter.hasNext();

        graph.close();

        HugeGraph.shutdown(30L);
    }

    public static class TestTask extends TaskCallable<Integer> {

        public static final int UNIT = 100;

        public volatile boolean run = true;

        @Override
        public Integer call() throws Exception {
            System.out.println(">>>> runing task with parameter: " +
                               this.task().input());
            for (int i = this.task().progress(); i <= 100 && this.run; i++) {
                System.out.println(">>>> progress " + i);
                this.task().progress(i);
                this.scheduler().save(this.task());
                Thread.sleep(UNIT);
            }
            return 18;
        }
    }
}
