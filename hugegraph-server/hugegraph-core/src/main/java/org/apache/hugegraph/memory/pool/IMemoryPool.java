package org.apache.hugegraph.memory.pool;

import java.util.Set;

import org.apache.hugegraph.memory.pool.impl.MemoryPoolStats;

public interface IMemoryPool {

    MemoryPoolStats getSnapShot();

    String getName();

    IMemoryPool getParentPool();

    Set<IMemoryPool> getChildrenPools();

    long allocate(long bytes);

    long free(long bytes);

    long requestMemory(long bytes);

    long reclaimMemory(long bytes, long maxWaitMs);

    // visitChildren，传递方法引用
}
