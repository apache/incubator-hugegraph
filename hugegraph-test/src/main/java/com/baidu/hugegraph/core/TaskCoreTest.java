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

package com.baidu.hugegraph.core;

import java.util.Iterator;
import java.util.concurrent.TimeoutException;

import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.job.GremlinAPI.GremlinRequest;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.exception.LimitExceedException;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.job.EphemeralJob;
import com.baidu.hugegraph.job.EphemeralJobBuilder;
import com.baidu.hugegraph.job.GremlinJob;
import com.baidu.hugegraph.job.JobBuilder;
import com.baidu.hugegraph.task.HugeTask;
import com.baidu.hugegraph.task.TaskCallable;
import com.baidu.hugegraph.task.TaskScheduler;
import com.baidu.hugegraph.task.TaskStatus;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.Whitebox;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class TaskCoreTest extends BaseCoreTest {

    private static final int SLEEP_TIME = 200;

    @Before
    @Override
    public void setup() {
        super.setup();

        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        Iterator<HugeTask<Object>> iter = scheduler.findAllTask(-1, null);
        while (iter.hasNext()) {
            scheduler.deleteTask(iter.next().id());
        }
    }

    @Test
    public void testTask() throws TimeoutException {
        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        TaskCallable<Integer> callable = new TaskCallable<Integer>() {
            @Override
            public Integer call() throws Exception {
                Thread.sleep(SLEEP_TIME);
                return 125;
            }

            @Override
            protected void done() {
                scheduler.save(this.task());
            }
        };

        Id id = IdGenerator.of(88888);
        HugeTask<?> task = new HugeTask<>(id, null, callable);
        task.type("test");
        task.name("test-task");

        scheduler.schedule(task);
        Assert.assertEquals(id, task.id());
        Assert.assertFalse(task.completed());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            scheduler.deleteTask(id);
        }, e -> {
            Assert.assertContains("Can't delete incomplete task '88888'",
                                  e.getMessage());
        });

        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(id, task.id());
        Assert.assertEquals("test-task", task.name());
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());

        Assert.assertEquals("test-task", scheduler.task(id).name());
        Assert.assertEquals("test-task", scheduler.findTask(id).name());

        Iterator<HugeTask<Object>> iter = scheduler.tasks(ImmutableList.of(id));
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals("test-task", iter.next().name());
        Assert.assertFalse(iter.hasNext());

        iter = scheduler.findTask(TaskStatus.SUCCESS, 10, null);
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals("test-task", iter.next().name());
        Assert.assertFalse(iter.hasNext());

        iter = scheduler.findAllTask(10, null);
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals("test-task", iter.next().name());
        Assert.assertFalse(iter.hasNext());

        scheduler.deleteTask(id);
        iter = scheduler.findAllTask(10, null);
        Assert.assertFalse(iter.hasNext());
        Assert.assertThrows(NotFoundException.class, () -> {
            scheduler.task(id);
        });
    }

    @Test
    public void testTaskWithFailure() throws TimeoutException {
        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        TaskCallable<Integer> callable = new TaskCallable<Integer>() {
            @Override
            public Integer call() throws Exception {
                Thread.sleep(SLEEP_TIME);
                return 125;
            }
            @Override
            protected void done() {
                scheduler.save(this.task());
            }
        };

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new HugeTask<>(null, null, callable);
        }, e -> {
            Assert.assertContains("Task id can't be null", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Id id = IdGenerator.of("88888");
            new HugeTask<>(id, null, callable);
        }, e -> {
            Assert.assertContains("Invalid task id type, it must be number",
                                  e.getMessage());
        });

        Assert.assertThrows(NullPointerException.class, () -> {
            Id id = IdGenerator.of(88888);
            new HugeTask<>(id, null, null);
        });

        Assert.assertThrows(IllegalStateException.class, () -> {
            Id id = IdGenerator.of(88888);
            HugeTask<?> task2 = new HugeTask<>(id, null, callable);
            task2.name("test-task");
            scheduler.schedule(task2);
        }, e -> {
            Assert.assertContains("Task type can't be null", e.getMessage());
        });

        Assert.assertThrows(IllegalStateException.class, () -> {
            Id id = IdGenerator.of(88888);
            HugeTask<?> task2 = new HugeTask<>(id, null, callable);
            task2.type("test");
            scheduler.schedule(task2);
        }, e -> {
            Assert.assertContains("Task name can't be null", e.getMessage());
        });
    }

    @Test
    public void testEphemeralJob() throws TimeoutException {
        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        EphemeralJobBuilder<Object> builder = EphemeralJobBuilder.of(graph);
        builder.name("test-job-ephemeral")
               .job(new EphemeralJob<Object>() {
                    @Override
                    public String type() {
                        return "test";
                    }
                    @Override
                    public Object execute() throws Exception {
                        Thread.sleep(SLEEP_TIME);
                        return ImmutableMap.of("k1", 13579, "k2", "24680");
                    }
               });

        HugeTask<Object> task = builder.schedule();
        Assert.assertEquals("test-job-ephemeral", task.name());
        Assert.assertEquals("test", task.type());
        Assert.assertFalse(task.completed());

        HugeTask<?> task2 = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        Assert.assertEquals("{\"k1\":13579,\"k2\":\"24680\"}", task.result());

        Assert.assertEquals(TaskStatus.SUCCESS, task2.status());
        Assert.assertEquals("{\"k1\":13579,\"k2\":\"24680\"}", task2.result());

        Assert.assertThrows(NotFoundException.class, () -> {
            scheduler.waitUntilTaskCompleted(task.id(), 10);
        });
        Assert.assertThrows(NotFoundException.class, () -> {
            scheduler.task(task.id());
        });
    }

    @Test
    public void testGremlinJob() throws TimeoutException {
        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        GremlinRequest request = new GremlinRequest();
        request.gremlin("3 + 5");

        JobBuilder<Object> builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input(request.toJson())
               .job(new GremlinJob());

        HugeTask<Object> task = builder.schedule();
        Assert.assertEquals("test-job-gremlin", task.name());
        Assert.assertEquals("gremlin", task.type());
        Assert.assertFalse(task.completed());
        Assert.assertNull(task.result());

        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals("test-job-gremlin", task.name());
        Assert.assertEquals("gremlin", task.type());
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        Assert.assertEquals("8", task.result());

        task = scheduler.task(task.id());
        Assert.assertEquals("test-job-gremlin", task.name());
        Assert.assertEquals("gremlin", task.type());
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        Assert.assertEquals("8", task.result());
    }

    @Test
    public void testGremlinJobWithScript() throws TimeoutException {
        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        String script = "schema=graph.schema();"
                + "schema.propertyKey('name').asText().ifNotExist().create();"
                + "schema.propertyKey('age').asInt().ifNotExist().create();"
                + "schema.propertyKey('lang').asText().ifNotExist().create();"
                + "schema.propertyKey('date').asDate().ifNotExist().create();"
                + "schema.propertyKey('price').asInt().ifNotExist().create();"
                + "person1=schema.vertexLabel('person1').properties('name','age').ifNotExist().create();"
                + "person2=schema.vertexLabel('person2').properties('name','age').ifNotExist().create();"
                + "knows=schema.edgeLabel('knows').sourceLabel('person1').targetLabel('person2').properties('date').ifNotExist().create();"
                + "for(int i = 0; i < 1000; i++) {"
                + "  p1=graph.addVertex(T.label,'person1','name','p1-'+i,'age',29);"
                + "  p2=graph.addVertex(T.label,'person2','name','p2-'+i,'age',27);"
                + "  p1.addEdge('knows',p2,'date','2016-01-10');"
                + "}";

        HugeTask<Object> task = runGremlinJob(script);
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals("test-gremlin-job", task.name());
        Assert.assertEquals("gremlin", task.type());
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        Assert.assertEquals("[]", task.result());

        script = "g.V().count()";
        task = runGremlinJob(script);
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        Assert.assertEquals("[2000]", task.result());

        script = "g.V().hasLabel('person1').count()";
        task = runGremlinJob(script);
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        Assert.assertEquals("[1000]", task.result());

        script = "g.V().hasLabel('person2').count()";
        task = runGremlinJob(script);
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        Assert.assertEquals("[1000]", task.result());

        script = "g.E().count()";
        task = runGremlinJob(script);
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        Assert.assertEquals("[1000]", task.result());

        script = "g.E().hasLabel('knows').count()";
        task = runGremlinJob(script);
        task = scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.SUCCESS, task.status());
        Assert.assertEquals("[1000]", task.result());
    }

    @Test
    public void testGremlinJobWithFailure() throws TimeoutException {
        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        JobBuilder<Object> builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input("")
               .job(new GremlinJob());
        HugeTask<Object> task = builder.schedule();
        scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("Can't read json", task.result());

        builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .job(new GremlinJob());
        task = builder.schedule();
        scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("The input can't be null", task.result());

        builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input("{}")
               .job(new GremlinJob());
        task = builder.schedule();
        scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("Invalid gremlin value 'null'", task.result());

        builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input("{\"gremlin\":8}")
               .job(new GremlinJob());
        task = builder.schedule();
        scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("Invalid gremlin value '8'", task.result());

        builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input("{\"gremlin\":\"\"}")
               .job(new GremlinJob());
        task = builder.schedule();
        scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("Invalid bindings value 'null'", task.result());

        builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input("{\"gremlin\":\"\", \"bindings\":\"\"}")
               .job(new GremlinJob());
        task = builder.schedule();
        scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("Invalid bindings value ''", task.result());

        builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input("{\"gremlin\":\"\", \"bindings\":{}}")
               .job(new GremlinJob());
        task = builder.schedule();
        scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("Invalid language value 'null'", task.result());

        builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input("{\"gremlin\":\"\", \"bindings\":{}, \"language\":{}}")
               .job(new GremlinJob());
        task = builder.schedule();
        scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("Invalid language value '{}'", task.result());

        builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input("{\"gremlin\":\"\", \"bindings\":{}, \"language\":\"\"}")
               .job(new GremlinJob());
        task = builder.schedule();
        scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("Invalid aliases value 'null'", task.result());

        builder = JobBuilder.of(graph);
        builder.name("test-job-gremlin")
               .input("{\"gremlin\":\"\", \"bindings\":{}, " +
                      "\"language\":\"test\", \"aliases\":{}}")
               .job(new GremlinJob());
        task = builder.schedule();
        scheduler.waitUntilTaskCompleted(task.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task.status());
        Assert.assertContains("test is not an available GremlinScriptEngine",
                              task.result());
    }

    @Test
    public void testGremlinJobWithError() throws TimeoutException {
        HugeGraph graph = graph();

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            JobBuilder.of(graph)
                      .job(new GremlinJob())
                      .schedule();
        }, e -> {
            Assert.assertContains("Job name can't be null", e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            JobBuilder.of(graph)
                      .name("test-job-gremlin")
                      .schedule();
        }, e -> {
            Assert.assertContains("Job callable can't be null", e.getMessage());
        });

        // Test failure task with big input
        char[] chars = new char[65536];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = '8';
        }
        String bigInput = new String(chars);
        Assert.assertThrows(LimitExceedException.class, () -> {
            runGremlinJob(bigInput);
        }, e -> {
            Assert.assertContains("Task input size 65605 exceeded " +
                                  "limit 65535 bytes", e.getMessage());
        });
    }

    @Test
    public void testGremlinJobAndCancel() throws TimeoutException {
        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        HugeTask<Object> task = runGremlinJob("Thread.sleep(1000 * 10);");
        scheduler.cancel(task);

        Assert.assertEquals(TaskStatus.CANCELLED, task.status());
        Assert.assertTrue(task.result(), task.result() == null ||
                          task.result().endsWith("InterruptedException"));

        task = scheduler.findTask(task.id());
        Assert.assertEquals(TaskStatus.CANCELLED, task.status());
        Assert.assertEquals("test-gremlin-job", task.name());
        Assert.assertTrue(task.result(), task.result() == null ||
                          task.result().endsWith("InterruptedException"));

        // Cancel success task
        HugeTask<Object> task2 = runGremlinJob("1+2");
        scheduler.waitUntilTaskCompleted(task2.id(), 10);
        Assert.assertEquals(TaskStatus.SUCCESS, task2.status());
        scheduler.cancel(task2);
        Assert.assertEquals(TaskStatus.SUCCESS, task2.status());
        Assert.assertEquals("3", task2.result());

        // Cancel failure task with big results (job size exceeded limit)
        String bigList = "def l=[]; for (i in 1..10001) l.add(i); l;";
        HugeTask<Object> task3 = runGremlinJob(bigList);
        scheduler.waitUntilTaskCompleted(task3.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task3.status());
        scheduler.cancel(task3);
        Assert.assertEquals(TaskStatus.FAILED, task3.status());
        Assert.assertContains("LimitExceedException: Job results size 10001 " +
                              "has exceeded the max limit 10000",
                              task3.result());

        // Cancel failure task with big results (task exceeded limit 64k)
        String bigResults = "def big='123456789'; def l=[]; " +
                            "for (i in 1..9000) l.add(big); l;";
        HugeTask<Object> task4 = runGremlinJob(bigResults);
        scheduler.waitUntilTaskCompleted(task4.id(), 10);
        Assert.assertEquals(TaskStatus.FAILED, task4.status());
        scheduler.cancel(task4);
        Assert.assertEquals(TaskStatus.FAILED, task4.status());
        Assert.assertContains("LimitExceedException: Task result size 108001 " +
                              "exceeded limit 65535 bytes", task4.result());
    }

    @Test
    public void testGremlinJobAndRestore() throws Exception {
        HugeGraph graph = graph();
        TaskScheduler scheduler = graph.taskScheduler();

        String gremlin = "for(int i=gremlinJob.progress(); i<=10; i++) {" +
                         "  gremlinJob.updateProgress(i);" +
                         "  Thread.sleep(1000);" +
                         "}; 100;";
        HugeTask<Object> task = runGremlinJob(gremlin);
        Thread.sleep(1000 * 6);
        scheduler.cancel(task);

        Assert.assertEquals(TaskStatus.CANCELLED, task.status());
        Assert.assertTrue("progress=" + task.progress(),
                          0 < task.progress() && task.progress() < 10);
        Assert.assertEquals(0, task.retries());
        Assert.assertEquals(null, task.result());

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            scheduler.restore(task);
        }, e -> {
            Assert.assertContains("No need to restore completed task",
                                  e.getMessage());
        });

        HugeTask<Object> task2 = scheduler.task(task.id());
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            scheduler.restore(task2);
        }, e -> {
            Assert.assertContains("No need to restore completed task",
                                  e.getMessage());
        });
        Whitebox.setInternalState(task2, "status", TaskStatus.RUNNING);
        scheduler.restore(task2);

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            scheduler.restore(task2);
        }, e -> {
            Assert.assertContains("is already in the queue", e.getMessage());
        });

        scheduler.waitUntilTaskCompleted(task2.id(), 10);
        Assert.assertEquals(10, task2.progress());
        Assert.assertEquals(1, task2.retries());
        Assert.assertEquals("100", task2.result());
    }

    private HugeTask<Object> runGremlinJob(String gremlin) {
        HugeGraph graph = graph();

        GremlinRequest request = new GremlinRequest();
        request.gremlin(gremlin);

        JobBuilder<Object> builder = JobBuilder.of(graph);
        builder.name("test-gremlin-job")
               .input(request.toJson())
               .job(new GremlinJob());

        HugeTask<Object> task = builder.schedule();
        return task;
    }
}
