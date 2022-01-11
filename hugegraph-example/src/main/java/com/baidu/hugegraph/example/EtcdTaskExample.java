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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.job.Job;
import com.baidu.hugegraph.logger.HugeGraphLogger;
import com.baidu.hugegraph.meta.MetaManager;
import com.baidu.hugegraph.task.HugeTask;
import com.baidu.hugegraph.task.TaskCallable;
import com.baidu.hugegraph.task.TaskScheduler;
import com.baidu.hugegraph.task.TaskStatus;
import com.baidu.hugegraph.util.Log;


public class EtcdTaskExample {

    private static final HugeGraphLogger LOGGER
        = Log.getLogger(EtcdTaskExample.class);
    private static final MetaManager metaManager = MetaManager.instance();

    public static void main(String[] args) throws Exception {
        LOGGER.logCustomDebug("EtcdTask Example Start {}", "Scorpiour", 0);
        String caFile = null;
        String clientCaFile = null;
        String clientKeyFile = null;

        List<String> endPoints = Arrays.asList("http://127.0.0.1:2379");

        metaManager.connect("hg", MetaManager.MetaDriverType.ETCD, caFile, clientCaFile, clientKeyFile, endPoints);
        

        HugeGraph graph = ExampleUtil.loadGraph();

        testTask(graph);
        Thread.sleep(30 * 1000L);
        graph.close();

        // Stop daemon thread
        HugeFactory.shutdown(5L);
    }

    public static void testTask(HugeGraph graph) throws InterruptedException {
        TaskScheduler scheduler = graph.taskScheduler();
        
        scheduler.flushAllTask();
        
        int start = 25;
        for (int i = start ; i < start + 5; i++) {

            int nid = i; //Math.abs(rand.nextInt())  % 10 + 1;
            int input = 10; //Math.abs(rand.nextInt()) % 5 + 1;
            
            Id id = IdGenerator.of(nid);
            String callable = "com.baidu.hugegraph.example.EtcdTaskExample$TestTask";
            HugeTask<?> task = new HugeTask<>(id, null, callable, "test-parameter");
            task.type("type-1");
            task.name("test-task");
            task.input(String.valueOf(input));

            
            scheduler.schedule(task);
            Thread.sleep(10);
        }

        
        // scheduler.flushAllTask();
        

        Iterator<HugeTask<Object>> iter;
        iter = scheduler.tasks(TaskStatus.SUCCESS, -1, null);
        while(iter.hasNext()) {
            HugeTask<?> task = iter.next();
            System.out.println(String.format("===========> success task %s ", task.id().asString()));
            scheduler.delete(task.id());
        }

        
        

        /*

        Thread.sleep(TestTask.UNIT * 33);
        task.cancel(true);
        Thread.sleep(TestTask.UNIT * 1);
        scheduler.save(task);

        
        // Find task not finished(actually it should be RUNNING)
        iter = scheduler.tasks(TaskStatus.CANCELLED, -1, null);
        assert iter.hasNext();
        task = iter.next();

        System.out.println(">>>> task may be interrupted");

        Thread.sleep(TestTask.UNIT * 10);
        System.out.println(">>>> restore task...");
        Whitebox.setInternalState(task, "status", TaskStatus.RUNNING);
        scheduler.restoreTasks();
        Thread.sleep(TestTask.UNIT * 80);
        scheduler.save(task);

        iter = scheduler.tasks(TaskStatus.SUCCESS, -1, null);
        assert iter.hasNext();
        task = iter.next();
        assert task.status() == TaskStatus.SUCCESS;
        assert task.retries() == 1;
        */
    }

    /**
     * The purposes of methods Job::execute() and TaskCallable<V>::call() are ambiguous. As what I can examined,
     * The execute() is called in the Abstract class TaskCallable<V>::call() somewhere like UserJob<V>, Combined wth save()
     * But there's no scenario of calling Job::execute()
     */
    public static class TestTask extends TaskCallable<Integer> implements Job<Integer> {

        public static final int UNIT = 100; // ms

        public volatile boolean run = true;

        @Override
        protected void done() {
            super.done();
            LOGGER.logCustomDebug(">>>> running task {} done()", "Scorpiour", this.task().id());
        }

        @Override
        protected void cancelled() {
            super.cancelled();
            LOGGER.logCustomDebug(">>>> running task {} cancelled()", "Scorpiour", this.task().id());
        }

        @Override
        public String type() {
            return "test-task";
        }

        @Override
        public Integer execute() throws Exception {
            System.out.println(">>>>====>>>> test task " + this.task().id().asString() + "  is running by " + Thread.currentThread().getId() + " " + Thread.currentThread().getName());
            int input = Integer.valueOf(this.task().input());
            for (int i = this.task().progress(); i <= input && this.run; i++) {

                System.out.println(">>>> progress " + i);
                this.task().progress(i);
                this.graph().taskScheduler().save(this.task());
                Thread.sleep(UNIT);
            }
            return 18;
        }

        @Override
        public Integer call() throws Exception {
            LOGGER.logCustomDebug(">>>> running task {} call()", "Scorpiour", this.task().id());
            return this.execute();

        }
    }
}
