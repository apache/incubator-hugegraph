package com.baidu.hugegraph.store.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author dominicwilliams
 * Updated by RoshanF on 12/28/2015.
 */
public class ManualResetEvent {

    private volatile CountDownLatch event;
    private static final Object mutex = new Object();

    /**
     * Initializes a new instance of the System.Threading.ManualResetEvent class
     * with a Boolean value indicating whether to set the initial state to signaled.
     * @param signalled true to set the initial state to signaled; false to set the initial state
     * to nonsignaled.
     */
    public ManualResetEvent(boolean signalled) {
        if (signalled) {
            event = new CountDownLatch(0);
        } else {
            event = new CountDownLatch(1);
        }
    }

    /**
     * Sets the state of the event to signaled, allowing one or more waiting threads to proceed.
     */
    public void set() {
        event.countDown();
    }

    /**
     * Sets the state of the event to nonsignaled, causing threads to block.
     */
    public void reset() {
        synchronized (mutex) {
            if (event.getCount() == 0) {
                event = new CountDownLatch(1);
            }
        }
    }

    /**
     * Blocks the current thread until the current wait handle receives a signal.
     * @throws InterruptedException
     */
    public void waitOne() throws InterruptedException {
        event.await();
    }

    /**
     * Blocks the current thread until the current wait handle receives a signal.
     * @param timeout the maximum time to wait
     * @param unit the time unit of the {@code timeout} argument
     * @return {@code true} if the count reached zero and {@code false}
     *         if the waiting time elapsed before the count reached zero
     * @throws InterruptedException if the current thread is interrupted
     *         while waiting
     */
    public boolean waitOne(int timeout, TimeUnit unit) throws InterruptedException {
        return event.await(timeout, unit);
    }

    /**
     * Check if the handle was signalled
     * @return Boolean state
     */
    public boolean isSignalled() {
        return event.getCount() == 0;
    }
}
