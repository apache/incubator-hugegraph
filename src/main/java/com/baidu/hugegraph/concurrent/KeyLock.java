package com.baidu.hugegraph.concurrent;

import java.util.concurrent.locks.Lock;

import com.google.common.util.concurrent.Striped;

public class KeyLock {

    private Striped<Lock> locks;

    public KeyLock() {
        this(Runtime.getRuntime().availableProcessors() * 4);
    }

    public KeyLock(int size) {
        this.locks = Striped.lock(size);
    }

    public final void lock(Object key) {
        this.locks.get(key).lock();
    }

    public final void unlock(Object key) {
        this.locks.get(key).unlock();
    }
}
