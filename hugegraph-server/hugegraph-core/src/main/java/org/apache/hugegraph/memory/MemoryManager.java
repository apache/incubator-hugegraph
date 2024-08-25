package org.apache.hugegraph.memory;

import org.apache.hugegraph.memory.pool.IMemoryPool;
import org.apache.hugegraph.memory.pool.impl.RootMemoryPool;

public class MemoryManager {

    private static final String ROOT_POOL_NAME = "RootQueryMemoryPool";
    private static final IMemoryPool ROOT = new RootMemoryPool(null, ROOT_POOL_NAME);

}
