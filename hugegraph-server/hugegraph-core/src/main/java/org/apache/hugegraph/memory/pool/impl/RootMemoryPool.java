package org.apache.hugegraph.memory.pool.impl;

import org.apache.hugegraph.memory.pool.AbstractMemoryPool;
import org.apache.hugegraph.memory.pool.IMemoryPool;

public class RootMemoryPool extends AbstractMemoryPool {

    public RootMemoryPool(IMemoryPool parent, String name) {
        super(parent, name);
    }

    @Override
    public long reclaimMemory(long bytes, long maxWaitMs) {
        return 0;
    }

    @Override
    public long requestMemory(long bytes) {
        return 0;
    }

    @Override
    public long free(long bytes) {
        return 0;
    }

    @Override
    public long allocate(long bytes) {
        return 0;
    }
}
