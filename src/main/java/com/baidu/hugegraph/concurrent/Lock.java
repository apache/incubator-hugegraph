package com.baidu.hugegraph.concurrent;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by zhangyi51 on 17/7/20.
 */
public class Lock {

    private String name;
    private AtomicReference<Thread> sign;

    public Lock(String name) {
        this.name = name;
        this.sign = new AtomicReference<>();
    }

    public boolean lock() {
        Thread current = Thread.currentThread();
        return this.sign.compareAndSet(null, current);
    }

    public void unlock() {
        Thread current = Thread.currentThread();
        if (!this.sign.compareAndSet(current, null)) {
            throw new RuntimeException(String.format(
                      "Thread '%s' try to unlock '%s' " +
                      "which is held by other thread now.",
                      current.getName(), this.name));
        }
    }

    public String name() {
        return this.name;
    }

    public void name(String name) {
        this.name = name;
    }
}
