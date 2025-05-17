package org.apache.hugegraph.pd.util;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class ExecutorUtil {

    private static Map<String, ThreadPoolExecutor> pools = new ConcurrentHashMap<>();

    public static ThreadPoolExecutor getThreadPoolExecutor(String name) {
        if (name == null) {
            return null;
        }
        return pools.get(name);
    }

    public static ThreadPoolExecutor createExecutor(String name, int coreThreads, int maxThreads,
                                                    int queueSize) {

        return createExecutor(name, coreThreads, maxThreads, queueSize, true);
    }

    public static ThreadPoolExecutor createExecutor(String name, int coreThreads, int maxThreads,
                                                    int queueSize, boolean daemon) {
        ThreadPoolExecutor res = pools.get(name);
        if (res != null) {
            return res;
        }
        synchronized (pools) {
            res = pools.get(name);
            if (res != null) {
                return res;
            }
            BlockingQueue queue;
            if (queueSize <= 0) {
                queue = new SynchronousQueue();
            } else {
                queue = new LinkedBlockingQueue<>(queueSize);
            }
            res = new ThreadPoolExecutor(coreThreads, maxThreads, 60L, TimeUnit.SECONDS, queue,
                                         new DefaultThreadFactory(name, daemon));
            pools.put(name, res);
        }
        return res;
    }
}
