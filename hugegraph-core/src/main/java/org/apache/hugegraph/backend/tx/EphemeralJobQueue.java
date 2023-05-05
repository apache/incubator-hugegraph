/*
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

package org.apache.hugegraph.backend.tx;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.job.EphemeralJob;
import org.apache.hugegraph.job.EphemeralJobBuilder;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

public class EphemeralJobQueue {

    private static final Logger LOG = Log.logger(EphemeralJobQueue.class);

    private static final int CAPACITY = 2000;

    private final BlockingQueue<EphemeralJob<?>> pendingQueue;

    private final AtomicReference<State> state;

    private final HugeGraph graph;

    private enum State {
        INIT,
        EXECUTE,
    }

    public EphemeralJobQueue(HugeGraph graph) {
        this.state = new AtomicReference<>(State.INIT);
        this.graph = graph;
        this.pendingQueue = new ArrayBlockingQueue<>(CAPACITY);
    }

    public void add(EphemeralJob<?> job) {
        if (job == null) {
            return;
        }

        if (!this.pendingQueue.offer(job)) {
            LOG.warn("The pending queue of EphemeralJobQueue is full, {} job " +
                     "will be ignored", job.type());
            this.reScheduleIfNeeded();
            return;
        }

        this.reScheduleIfNeeded();
    }

    protected Queue<EphemeralJob<?>> queue() {
        return this.pendingQueue;
    }

    public void consumeComplete() {
        this.state.compareAndSet(State.EXECUTE, State.INIT);
    }

    public void reScheduleIfNeeded() {
        if (this.state.compareAndSet(State.INIT, State.EXECUTE)) {
            try {
                BatchEphemeralJob job = new BatchEphemeralJob(this);
                EphemeralJobBuilder.of(this.graph)
                                   .name("batch-ephemeral-job")
                                   .job(job)
                                   .schedule();
            } catch (Throwable e) {
                // Maybe if it fails, consider clearing all the data in the pendingQueue,
                // or start a scheduled retry task to retry until success.
                LOG.warn("Failed to schedule RemoveLeftIndexJob", e);
                this.pendingQueue.clear();
                this.state.compareAndSet(State.EXECUTE, State.INIT);
            }
        }
    }

    public boolean isEmpty() {
        return this.pendingQueue.isEmpty();
    }

    public static class BatchEphemeralJob extends EphemeralJob<Object> {

        private static final int PAGE_SIZE = 100;
        private static final String BATCH_EPHEMERAL_JOB = "batch-ephemeral-job";
        private static final int MAX_CONSUME_COUNT = EphemeralJobQueue.CAPACITY / 2;

        private WeakReference<EphemeralJobQueue> queueWeakReference;

        public BatchEphemeralJob(EphemeralJobQueue queue) {
            this.queueWeakReference = new WeakReference<>(queue);
        }

        @Override
        public String type() {
            return BATCH_EPHEMERAL_JOB;
        }

        @Override
        public Object execute() throws Exception {
            boolean stop = false;
            Object ret = null;
            int consumeCount = 0;
            InterruptedException interruptedException = null;
            EphemeralJobQueue queue;
            List<EphemeralJob<?>> batchJobs = new ArrayList<>();
            while (!stop) {
                if (interruptedException == null && Thread.currentThread().isInterrupted()) {
                    interruptedException = new InterruptedException();
                }

                queue = this.queueWeakReference.get();
                if (queue == null) {
                    stop = true;
                    continue;
                }

                if (queue.isEmpty() || consumeCount > MAX_CONSUME_COUNT ||
                    interruptedException != null) {
                    queue.consumeComplete();
                    stop = true;
                    if (!queue.isEmpty()) {
                        queue.reScheduleIfNeeded();
                    }
                    continue;
                }

                try {
                    while (!queue.isEmpty() && batchJobs.size() < PAGE_SIZE) {
                        EphemeralJob<?> job = queue.queue().poll();
                        batchJobs.add(job);
                        consumeCount++;
                    }

                    if (batchJobs.isEmpty()) {
                        continue;
                    }

                    ret = this.executeBatchJob(batchJobs, ret);

                } catch (InterruptedException e) {
                    interruptedException = e;
                } finally {
                    batchJobs.clear();
                }
            }

            if (interruptedException != null) {
                Thread.currentThread().interrupt();
                throw interruptedException;
            }

            return ret;
        }

        private Object executeBatchJob(List<EphemeralJob<?>> jobs, Object prev) throws Exception {
            GraphIndexTransaction graphTx = this.params().systemTransaction().indexTransaction();
            GraphIndexTransaction systemTx = this.params().graphTransaction().indexTransaction();
            Object ret = prev;
            for (EphemeralJob<?> job : jobs) {
                Object obj = job.call();
                if (job instanceof Reduce) {
                    ret = ((Reduce) job).reduce(ret, obj);
                }
            }

            graphTx.commit();
            systemTx.commit();

            return ret;
        }

        @Override
        public Object call() throws Exception {
            try {
                return super.call();
            } catch (Throwable e) {
                LOG.warn("Failed to execute BatchEphemeralJob", e);
                EphemeralJobQueue queue = this.queueWeakReference.get();
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                    if (queue != null) {
                        queue.queue().clear();
                    }
                    throw e;
                }

                if (queue != null && !queue.isEmpty()) {
                    queue.reScheduleIfNeeded();
                }
                throw e;
            }
        }
    }

    public interface Reduce<T> {
        T reduce(T t1,  T t2);
    }
}
