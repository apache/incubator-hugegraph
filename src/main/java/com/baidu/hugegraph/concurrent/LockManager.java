package com.baidu.hugegraph.concurrent;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by zhangyi51 on 17/7/13.
 */
public class LockManager {

    private static final LockManager INSTANCE = new LockManager();

    public static LockManager instance() {
        return INSTANCE;
    }

    private Map<String, Lock> locksMap;

    private LockManager() {
        locksMap = new ConcurrentHashMap<>();
    }

    public boolean exists(String lockName) {
        return locksMap.containsKey(lockName);
    }

    public Lock create(String lockName) {
        if (locksMap.containsKey(lockName)) {
            throw new RuntimeException(String.format(
                      "Lock '%s' exists!", lockName));
        }
        Lock lock = new Lock(lockName);

        locksMap.put(lockName, lock);
        return lock;
    }

    public Lock get(String lockName) {
        return locksMap.get(lockName);
    }

    public class Lock {

        private String name;
        private AtomicReference<Thread> sign;

        public Lock(String name) {
            this.name = name;
            sign = new AtomicReference<>();
        }

        public boolean lock() {
            Thread current = Thread.currentThread();
            return sign.compareAndSet(null, current);
        }

        public void unlock() {
            Thread current = Thread.currentThread();
            if (!sign.compareAndSet(current, null)) {
                throw new RuntimeException(String.format("Thread '%s' try to " +
                          "unlock '%s' which is held by other thread now.",
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
}
