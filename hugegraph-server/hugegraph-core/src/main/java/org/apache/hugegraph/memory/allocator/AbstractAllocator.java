package org.apache.hugegraph.memory.allocator;

public abstract class AbstractAllocator implements IAllocator {

    @Override
    public long tryToAllocateOnHeap(long size) {
        return 0;
    }

    @Override
    public long forceAllocateOnHeap(long size) {
        return 0;
    }
}
