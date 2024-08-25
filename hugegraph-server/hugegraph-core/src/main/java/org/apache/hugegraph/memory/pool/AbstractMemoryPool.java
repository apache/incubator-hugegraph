package org.apache.hugegraph.memory.pool;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.hugegraph.memory.pool.impl.MemoryPoolStats;

public abstract class AbstractMemoryPool implements IMemoryPool {
    private final IMemoryPool parent;
    private final Set<IMemoryPool> children = new CopyOnWriteArraySet<>();
    private final MemoryPoolStats stats;

    public AbstractMemoryPool(IMemoryPool parent, String memoryPoolName) {
        this.parent = parent;
        this.stats = new MemoryPoolStats(memoryPoolName);
    }

    @Override
    public MemoryPoolStats getSnapShot() {
        return stats;
    }

    @Override
    public IMemoryPool getParentPool() {
        return parent;
    }

    @Override
    public String getName() {
        return stats.getMemoryPoolName();
    }

    @Override
    public Set<IMemoryPool> getChildrenPools() {
        return children;
    }
}
