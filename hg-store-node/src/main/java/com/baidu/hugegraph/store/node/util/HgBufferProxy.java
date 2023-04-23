package com.baidu.hugegraph.store.node.util;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.CheckForNull;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author lynn.bond@hotmail.com on 2022/3/15
 * @version 0.1.0
 * @version 0.2.0
 */
@Slf4j
public final class HgBufferProxy<T> {
    private final BlockingQueue<Supplier<T>> queue;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ReentrantLock lock = new ReentrantLock();
    private final Runnable applier;
    private final Executor executor;

    public static HgBufferProxy of(Executor executor,Runnable applier) {
        HgAssert.isArgumentNotNull(applier, "applier");
        HgAssert.isArgumentNotNull(executor, "executor");
        return new HgBufferProxy(executor,applier);
    }

    private HgBufferProxy(Executor executor,Runnable applier) {
        this.executor=executor;
        this.applier = applier;
        this.queue = new LinkedBlockingQueue<>();
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
                    throw new RuntimeException("Timeout, max time: " + timeoutSeconds + " seconds;");
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
