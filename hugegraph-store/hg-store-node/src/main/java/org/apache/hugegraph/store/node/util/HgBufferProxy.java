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

package org.apache.hugegraph.store.node.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.CheckForNull;

import lombok.extern.slf4j.Slf4j;

/**
 * 2022/3/15
 *
 * @version 0.2.0
 */
@Slf4j
public final class HgBufferProxy<T> {

    private final BlockingQueue<Supplier<T>> queue;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ReentrantLock lock = new ReentrantLock();
    private final Runnable applier;
    private final Executor executor;

    private HgBufferProxy(Executor executor, Runnable applier) {
        this.executor = executor;
        this.applier = applier;
        this.queue = new LinkedBlockingQueue<>();
    }

    public static HgBufferProxy of(Executor executor, Runnable applier) {
        HgAssert.isArgumentNotNull(applier, "applier");
        HgAssert.isArgumentNotNull(executor, "executor");
        return new HgBufferProxy(executor, applier);
    }

    public void send(T t) {
        if (t == null) {
            throw new IllegalArgumentException("the argument t is null");
        }

        if (this.closed.get()) {
            log.warn("the proxy has been closed");
            return;
        }

        this.lock.lock();
        try {
            this.queue.offer(() -> t);
        } finally {
            lock.unlock();
        }
    }

    /**
     * return an item from the chan
     *
     * @throws RuntimeException when fail to receive an item.
     */
    @CheckForNull
    public T receive(int timeoutSeconds) {
        return receive(timeoutSeconds, (timeout) -> {
            throw new RuntimeException("Timeout, max time: " + timeout + " seconds;");
        });
    }

    private void apply() {
        this.lock.lock();
        try {
            if (!this.closed.get()) {
                this.executor.execute(this.applier);
                Thread.yield();
            }
        } finally {
            this.lock.unlock();
        }

    }

    /**
     * return an item from the chan
     *
     * @return null when the chan has been closed
     * @throws RuntimeException
     */
    @CheckForNull
    public T receive(int timeoutSeconds, Consumer<Integer> timeoutCallBack) {
        Supplier<T> s = null;

        if (this.closed.get()) {
            s = this.queue.poll();
            return s != null ? s.get() : null;
        }

        if (this.queue.size() <= 1) {
            this.apply();
        }

        lock.lock();
        try {
            if (this.isClosed()) {
                s = this.queue.poll();
                return s != null ? s.get() : null;
            }
        } finally {
            lock.unlock();
        }

        try {
            s = this.queue.poll(timeoutSeconds, TimeUnit.SECONDS);
        } catch (Throwable t) {
            log.error("Failed to receive a item from chan. cause by: ", t);
            throw new RuntimeException(t);
        }

        if (s == null) {
            if (this.closed.get()) {
                s = this.queue.poll();
            } else {
                if (timeoutCallBack == null) {
                    throw new RuntimeException(
                            "Timeout, max time: " + timeoutSeconds + " seconds;");
                } else {
                    timeoutCallBack.accept(timeoutSeconds);
                }
            }
        }

        return s != null ? s.get() : null;

    }

    public boolean isClosed() {
        return this.closed.get();
    }

    /**
     * @throws RuntimeException when fail to close the chan
     */
    public void close() {
        if (this.closed.get()) {
            return;
        }
        lock.lock();
        this.closed.set(true);
        try {
            this.queue.offer(() -> null);
        } finally {
            lock.unlock();
        }

    }

}
