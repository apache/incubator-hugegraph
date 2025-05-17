package org.apache.hugegraph.pd.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zhangyingjie
 * @date 2023/6/13
 **/
public class DefaultThreadFactory implements ThreadFactory {

    private final AtomicInteger number = new AtomicInteger(1);
    private final String namePrefix;
    private boolean daemon;

    public DefaultThreadFactory(String prefix, boolean daemon) {
        this.namePrefix = prefix + "-";
        this.daemon = daemon;
    }

    public DefaultThreadFactory(String prefix) {
        this(prefix, true);
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(null, r, namePrefix + number.getAndIncrement(), 0);
        t.setDaemon(daemon);
        t.setPriority(Thread.NORM_PRIORITY);
        return t;
    }
}
