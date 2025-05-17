package org.apache.hugegraph.pd.common;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zhangyingjie
 * @date 2023/4/25
 **/
public class DefaultThreadFactory implements ThreadFactory {

    private final AtomicInteger number = new AtomicInteger(1);
    private final String prefix;

    public DefaultThreadFactory(String prefix) {
        this.prefix = prefix + "-";
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, prefix + number.getAndIncrement());
        t.setDaemon(true);
        t.setPriority(Thread.NORM_PRIORITY);
        return t;
    }
}
