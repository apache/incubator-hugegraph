package org.apache.hugegraph.memory.allocator;

public interface IAllocator {

    long tryToAllocateOnHeap(long size);

    long forceAllocateOnHeap(long size);

    long tryToAllocateOffHeap(long size);

    long forceAllocateOffHeap(long size);
}
