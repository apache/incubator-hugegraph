package com.baidu.hugegraph.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhangyi51 on 17/7/20.
 */
public class LockGroup {

    private static final Logger logger =
            LoggerFactory.getLogger(Lock.class);

    private String name;
    private Map<String, Lock> locksMap;

    public LockGroup(String lockGroup) {
        this.name = lockGroup;
        this.locksMap = new ConcurrentHashMap();
    }

    public boolean lock(String lockName) {
        if (!this.locksMap.containsKey(lockName)) {
            this.locksMap.putIfAbsent(lockName, new Lock(lockName));
        }
        return this.locksMap.get(lockName).lock();
    }

    public boolean lock(String lockName, int retries) {
        // The interval between retries is exponential growth(2^i)
        if (retries < 0 || retries > 10) {
            throw new RuntimeException("Lock retry times should in [0, 10]");
        }
        if (!this.locksMap.containsKey(lockName)) {
            this.locksMap.putIfAbsent(lockName, new Lock(lockName));
        }
        for (int i = 0;; i++) {
            if (this.locksMap.get(lockName).lock()) {
                return true;
            } else if (i >= retries) {
                break;
            }
            try {
                Thread.sleep(1000 * (1L << i));
            } catch (InterruptedException ignored) {
                logger.info("Thread sleep is interrupted.");
            }
        }
        return false;
    }

    public void unlock(String lockName) {
        if (!this.locksMap.containsKey(lockName)) {
            throw new RuntimeException(String.format(
                      "There is no lock '%s' in LockGroup '%s'",
                      lockName, this.name));
        }
        this.locksMap.get(lockName).unlock();
    }

    public String name() {
        return this.name;
    }

    public void name(String name) {
        this.name = name;
    }
}
