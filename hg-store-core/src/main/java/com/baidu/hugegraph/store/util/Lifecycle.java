package com.baidu.hugegraph.store.util;
/**
 * @author YanJinbing
 */
public interface Lifecycle<T> {

    /**
     * Initialize the service.
     *
     * @return true when successes.
     */
    boolean init(final T opts);

    /**
     * Dispose the resources for service.
     */
    void shutdown();
}
