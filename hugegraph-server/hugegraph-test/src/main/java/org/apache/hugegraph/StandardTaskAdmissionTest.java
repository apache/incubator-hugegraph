/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.dist.RegisterUtil;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.job.SysJob;
import org.apache.hugegraph.masterelection.GlobalMasterInfo;
import org.apache.hugegraph.task.HugeTask;
import org.apache.hugegraph.task.StandardTaskScheduler;
import org.apache.hugegraph.task.TaskCallable;
import org.apache.hugegraph.task.TaskStatus;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.unit.FakeObjects;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;
import org.mockito.Mockito;

public class StandardTaskAdmissionTest {

    @Test
    public void testQueuedDefaultCancellationPreservesCallerTransaction() throws Exception {
        this.assertDefaultCancellationPreservesCallerTransaction(false);
    }

    @Test
    public void testRunningDefaultCancellationPreservesCallerTransaction() throws Exception {
        this.assertDefaultCancellationPreservesCallerTransaction(true);
    }

    private void assertDefaultCancellationPreservesCallerTransaction(boolean running) throws Exception {
        try (Fixture fixture = new Fixture("standard_caller_tx_" + running)) {
            CountDownLatch occupied = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            if (!running) {
                fixture.worker.submit(() -> {
                    occupied.countDown();
                    await(release);
                });
                Assert.assertTrue(occupied.await(10L, TimeUnit.SECONDS));
            }
            BlockingJob job = new BlockingJob();
            HugeTask<Object> task = fixture.task(job);
            try {
                fixture.scheduler.schedule(task);
                if (running) {
                    Assert.assertTrue(job.started.await(10L, TimeUnit.SECONDS));
                }
                fixture.graph.schema().vertexLabel("caller").useCustomizeStringId().create();
                fixture.graph.tx().commit();
                fixture.graph.addVertex(T.id, "pending", T.label, "caller");
                fixture.scheduler.cancel(task);
                Assert.assertTrue(task.isCancelled());
                Assert.assertTrue(fixture.graph.tx().isOpen());
                Assert.assertTrue(fixture.graph.vertices("pending").hasNext());
                fixture.graph.tx().commit();
                Assert.assertFalse(fixture.graph.tx().isOpen());
                Assert.assertTrue(fixture.database.submit(() -> {
                    try {
                        return fixture.graph.vertices("pending").hasNext();
                    } finally {
                        fixture.params.closeTx();
                    }
                }).get(10L, TimeUnit.SECONDS));
                if (running) {
                    Assert.assertEquals(1, fixture.scheduler.pendingTasks());
                }
            } finally {
                release.countDown();
                job.release.countDown();
            }
            fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
            Assert.assertEquals(0, fixture.scheduler.pendingTasks());
            ThreadLocal<?> owners = Whitebox.getInternalState(fixture.graph.tx(), "transactions");
            Assert.assertNull(fixture.worker.submit(owners::get).get(10L, TimeUnit.SECONDS));
            Assert.assertEquals(running ? 0L : 1L, job.started.getCount());
        }
    }

    @Test
    public void testCancelledWorkerFencesAdmissionUntilActualExit() throws Exception {
        try (Fixture fixture = new Fixture("standard_cancel_worker")) {
            BlockingJob job = new BlockingJob();
            HugeTask<Object> task = fixture.task(job);
            fixture.scheduler.schedule(task);
            Assert.assertTrue(job.started.await(10L, TimeUnit.SECONDS));
            fixture.scheduler.cancel(task);
            Assert.assertTrue(task.isCancelled());
            Assert.assertEquals(1, fixture.scheduler.pendingTasks());
            Assert.assertFalse(fixture.scheduler.close());
            HugeTask<Object> late = fixture.task(new EmptyJob());
            Assert.assertThrows(IllegalStateException.class, () -> fixture.scheduler.schedule(late));
            Assert.assertThrows(IllegalStateException.class, () -> {
                fixture.scheduler.restoreTasks();
            }, error -> Assert.assertContains("closing", error.getMessage()));
            Assert.assertEquals(TaskStatus.NEW, late.status());
            job.release.countDown();
            fixture.scheduler.waitUntilAllTasksCompleted(10L);
            Assert.assertEquals(TaskStatus.CANCELLED, fixture.scheduler.findTask(task.id()).status());
            Assert.assertTrue(fixture.scheduler.close());
        }
    }

    @Test
    public void testDispatchFutureCancellationKeepsRunningOwner() throws Exception {
        try (Fixture fixture = new Fixture("standard_cancel_future")) {
            BlockingJob job = new BlockingJob();
            HugeTask<Object> task = fixture.task(job);
            Future<?> dispatch = fixture.scheduler.schedule(task);
            Assert.assertTrue(job.started.await(10L, TimeUnit.SECONDS));
            Assert.assertTrue(dispatch.cancel(false));
            Assert.assertFalse(task.isCancelled());
            Assert.assertFalse(fixture.scheduler.close());
            job.release.countDown();
            fixture.scheduler.waitUntilAllTasksCompleted(10L);
            Assert.assertEquals(TaskStatus.SUCCESS, fixture.scheduler.findTask(task.id()).status());
            Assert.assertTrue(fixture.scheduler.close());
        }
    }

    @Test
    public void testCancellationCallbackRetainsItsTransactionAfterWorkerExits() throws Exception {
        try (Fixture fixture = new Fixture("standard_cancel_callback")) {
            fixture.graph.schema().vertexLabel("person").useCustomizeStringId().create();
            CountDownLatch callback = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            BlockingJob job = new BlockingJob() {
                @Override
                protected void cancelled() {
                    this.graph().addVertex(T.id, "callback", T.label, "person");
                    callback.countDown();
                    await(release);
                    this.graph().tx().commit();
                    super.cancelled();
                }
            };
            HugeTask<Object> task = fixture.task(job);
            fixture.scheduler.schedule(task);
            Assert.assertTrue(job.started.await(10L, TimeUnit.SECONDS));
            ExecutorService caller = Executors.newSingleThreadExecutor();
            try {
                Future<?> cancellation = caller.submit(() -> fixture.scheduler.cancel(task));
                Assert.assertTrue(callback.await(10L, TimeUnit.SECONDS));
                job.release.countDown();
                fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
                Assert.assertEquals(1, fixture.scheduler.pendingTasks());
                Assert.assertFalse(fixture.scheduler.close());
                release.countDown();
                cancellation.get(10L, TimeUnit.SECONDS);
                fixture.scheduler.waitUntilAllTasksCompleted(10L);
                Assert.assertTrue(fixture.graph.vertices("callback").hasNext());
                Assert.assertEquals(TaskStatus.CANCELLED, fixture.scheduler.findTask(task.id()).status());
            } finally {
                release.countDown();
                caller.shutdownNow();
                Assert.assertTrue(caller.awaitTermination(10L, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    public void testQueuedCancellationAndSubmissionRejectionReleaseOwners() throws Exception {
        try (Fixture fixture = new Fixture("standard_cancel_queued")) {
            CountDownLatch occupied = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            fixture.worker.submit(() -> {
                occupied.countDown();
                release.await();
                return null;
            });
            try {
                Assert.assertTrue(occupied.await(10L, TimeUnit.SECONDS));
                BlockingJob job = new BlockingJob();
                Future<?> queued = fixture.scheduler.schedule(fixture.task(job));
                Assert.assertTrue(queued.cancel(false));
                Assert.assertEquals(0, fixture.scheduler.pendingTasks());
                release.countDown();
                fixture.worker.submit(() -> null).get(10L, TimeUnit.SECONDS);
                Assert.assertEquals(1L, job.started.getCount());
                fixture.worker.shutdown();
                Assert.assertTrue(fixture.worker.awaitTermination(10L, TimeUnit.SECONDS));
                Assert.assertThrows(RejectedExecutionException.class,
                                    () -> fixture.scheduler.schedule(fixture.task(new EmptyJob())));
                Assert.assertEquals(0, fixture.scheduler.pendingTasks());
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    public void testDependencyRetryDoesNotReleaseEarlierCleanupOwner() throws Exception {
        try (Fixture fixture = new Fixture("standard_dependency_owner")) {
            CountDownLatch closing = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            AtomicBoolean first = new AtomicBoolean(true);
            Mockito.doAnswer(invocation -> {
                if (Thread.currentThread().getName().equals("standard-test-worker") &&
                    first.compareAndSet(true, false)) {
                    closing.countDown();
                    Assert.assertTrue(release.await(10L, TimeUnit.SECONDS));
                }
                return invocation.callRealMethod();
            }).when(fixture.params).closeTx();
            HugeTask<Object> dependency = fixture.task(new EmptyJob());
            fixture.scheduler.save(dependency);
            HugeTask<Object> task = fixture.task(new EmptyJob());
            task.depends(dependency.id());
            fixture.scheduler.schedule(task);
            try {
                Assert.assertTrue(closing.await(10L, TimeUnit.SECONDS));
                fixture.scheduler.cancel(task);
                Assert.assertEquals(1, fixture.scheduler.pendingTasks());
                Assert.assertFalse(fixture.scheduler.close());
            } finally {
                release.countDown();
            }
            fixture.scheduler.waitUntilAllTasksCompleted(10L);
            Assert.assertEquals(TaskStatus.CANCELLED, fixture.scheduler.findTask(task.id()).status());
        }
    }

    @Test
    public void testCloseFencesDependencyResubmission() throws Exception {
        try (Fixture fixture = new Fixture("standard_dependency_fence")) {
            CountDownLatch occupied = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            fixture.worker.submit(() -> {
                occupied.countDown();
                release.await();
                return null;
            });
            try {
                Assert.assertTrue(occupied.await(10L, TimeUnit.SECONDS));
                HugeTask<Object> dependency = fixture.task(new EmptyJob());
                fixture.scheduler.save(dependency);
                HugeTask<Object> task = fixture.task(new EmptyJob());
                task.depends(dependency.id());
                Future<?> dispatch = fixture.scheduler.schedule(task);
                Assert.assertFalse(fixture.scheduler.close());
                release.countDown();
                dispatch.get(10L, TimeUnit.SECONDS);
                fixture.scheduler.waitUntilAllTasksCompleted(10L);
                Assert.assertEquals(TaskStatus.FAILED, fixture.scheduler.findTask(task.id()).status());
                Assert.assertContains("is closing", task.result());
                Assert.assertTrue(fixture.scheduler.close());
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    public void testCloseFailureRemainsRetryable() throws Exception {
        try (Fixture fixture = new Fixture("standard_close_failure")) {
            Mockito.doThrow(new IllegalStateException("close failed")).doCallRealMethod()
                   .when(fixture.params).closeTx();
            Assert.assertThrows(RuntimeException.class, fixture.scheduler::close);
            HugeTask<Object> late = fixture.task(new EmptyJob());
            Assert.assertThrows(IllegalStateException.class, () -> fixture.scheduler.schedule(late));
            Assert.assertTrue(fixture.scheduler.close());
        }
    }

    private static void await(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static class Fixture implements AutoCloseable {

        private final HugeGraph graph;
        private final HugeGraphParams params;
        private final ExecutorService worker;
        private final ExecutorService database;
        private final StandardTaskScheduler scheduler;
        private final List<BlockingJob> jobs = new ArrayList<>();
        private long nextId = 9998000L;

        private Fixture(String name) {
            RegisterUtil.registerBackends();
            HugeConfig config = FakeObjects.newConfig();
            config.setProperty("backend", "memory");
            config.setProperty("serializer", "text");
            config.setProperty("store", name);
            this.graph = HugeFactory.open(config);
            this.graph.initBackend();
            this.graph.serverStarted(GlobalMasterInfo.master("standard-owner-test"));
            this.params = Mockito.spy((HugeGraphParams) Whitebox.getInternalState(this.graph, "params"));
            this.worker = Executors.newSingleThreadExecutor(runnable ->
                    new Thread(runnable, "standard-test-worker"));
            this.database = Executors.newSingleThreadExecutor();
            this.scheduler = new StandardTaskScheduler(this.params, this.worker, this.database);
        }

        private HugeTask<Object> task(TaskCallable<Object> job) {
            if (job instanceof BlockingJob) {
                this.jobs.add((BlockingJob) job);
            }
            HugeTask<Object> task = new HugeTask<>(IdGenerator.of(this.nextId++), null, job);
            task.type("test");
            task.name("standard-owner");
            return task;
        }

        @Override
        public void close() throws Exception {
            for (BlockingJob job : this.jobs) {
                job.release.countDown();
            }
            this.scheduler.close();
            this.worker.shutdown();
            Assert.assertTrue(this.worker.awaitTermination(10L, TimeUnit.SECONDS));
            Assert.assertTrue(this.scheduler.close());
            this.database.shutdownNow();
            Assert.assertTrue(this.database.awaitTermination(10L, TimeUnit.SECONDS));
            this.graph.close();
            HugeFactory.remove(this.graph);
        }
    }

    public static class EmptyJob extends SysJob<Object> {

        @Override
        public String type() {
            return "test";
        }

        @Override
        public Object execute() {
            return null;
        }
    }

    public static class BlockingJob extends SysJob<Object> {

        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        @Override
        public String type() {
            return "test";
        }

        @Override
        public Object execute() {
            this.graph().vertices().hasNext();
            this.started.countDown();
            await(this.release);
            return null;
        }
    }
}
