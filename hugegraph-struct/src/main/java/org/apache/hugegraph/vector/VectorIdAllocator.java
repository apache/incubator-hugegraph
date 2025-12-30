package org.apache.hugegraph.vector;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class VectorIdAllocator {
    AtomicInteger vectorId = new AtomicInteger(-1);;
    private final Queue<Integer> freeList = new ConcurrentLinkedQueue<>();

    public VectorIdAllocator(int maxVectorId) {
        this(maxVectorId, null);
    }

    public VectorIdAllocator(){
        this(-1, null);
    }

    public VectorIdAllocator(int maxVectorId, List<Integer> freeList) {
        vectorId = new AtomicInteger(maxVectorId);
        this.freeList.addAll(freeList);
    }

    int next() {
        if(!freeList.isEmpty()) {
            return freeList.remove();
        }
        return vectorId.incrementAndGet();
    }

    // store the free id
    void release(int id) {
        freeList.add(id);
    }

}
