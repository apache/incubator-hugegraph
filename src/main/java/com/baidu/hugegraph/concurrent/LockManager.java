package com.baidu.hugegraph.concurrent;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Created by zhangyi51 on 17/7/13.
 */
public class LockManager {

    private static final LockManager INSTANCE = new LockManager();

    public static LockManager instance() {
        return INSTANCE;
    }

    private Map<String, LockGroup> lockGroupMap;

    private LockManager() {
        this.lockGroupMap = new ConcurrentHashMap<>();
    }

    public boolean exists(String lockGroup) {
        return this.lockGroupMap.containsKey(lockGroup);
    }

    public LockGroup create(String lockGroup) {
        if (this.lockGroupMap.containsKey(lockGroup)) {
            throw new RuntimeException(String.format(
                      "LockGroup '%s' already exists!", lockGroup));
        }
        LockGroup lockgroup = new LockGroup(lockGroup);

        this.lockGroupMap.put(lockGroup, lockgroup);
        return lockgroup;
    }

    public LockGroup get(String lockGroup) {
        if (!exists(lockGroup)) {
            throw new RuntimeException(String.format(
                      "Not exist LockGroup '%s'", lockGroup));
        }
        return this.lockGroupMap.get(lockGroup);
    }

    public void destroy(String lockGroup) {
        if (this.exists(lockGroup)) {
            this.lockGroupMap.remove(lockGroup);
        } else {
            throw new RuntimeException(String.format(
                      "Not exist LockGroup '%s'", lockGroup));
        }
    }
}
