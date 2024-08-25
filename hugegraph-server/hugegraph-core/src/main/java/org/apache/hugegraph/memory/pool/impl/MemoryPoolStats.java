package org.apache.hugegraph.memory.pool.impl;

public class MemoryPoolStats {

    private final String memoryPoolName;
    private long usedBytes;
    private long reservedBytes;
    private long cumulativeBytes;
    private long allocatedBytes;

    private long numShrinks;
    private long numExpands;
    private long numAborts;

    public MemoryPoolStats(String MemoryPoolName) {
        this.memoryPoolName = MemoryPoolName;
    }

    @Override
    public String toString() {
        return String.format("IMemoryPool-%s: {usedBytes[%d], reservedBytes[%d], " +
                             "cumulativeBytes[%d], allocatedBytes[%d], numShrinks[%d], " +
                             "numExpands[%d], numAborts[%d]}.", memoryPoolName, usedBytes,
                             reservedBytes,
                             cumulativeBytes, allocatedBytes, numShrinks, numExpands,
                             numAborts);
    }

    public String getMemoryPoolName() {
        return memoryPoolName;
    }

    public long getUsedBytes() {
        return usedBytes;
    }

    public void setUsedBytes(long usedBytes) {
        this.usedBytes = usedBytes;
    }

    public long getReservedBytes() {
        return reservedBytes;
    }

    public void setReservedBytes(long reservedBytes) {
        this.reservedBytes = reservedBytes;
    }

    public long getCumulativeBytes() {
        return cumulativeBytes;
    }

    public void setCumulativeBytes(long cumulativeBytes) {
        this.cumulativeBytes = cumulativeBytes;
    }

    public long getAllocatedBytes() {
        return allocatedBytes;
    }

    public void setAllocatedBytes(long allocatedBytes) {
        this.allocatedBytes = allocatedBytes;
    }

    public long getNumShrinks() {
        return numShrinks;
    }

    public void setNumShrinks(long numShrinks) {
        this.numShrinks = numShrinks;
    }

    public long getNumExpands() {
        return numExpands;
    }

    public void setNumExpands(long numExpands) {
        this.numExpands = numExpands;
    }

    public long getNumAborts() {
        return numAborts;
    }

    public void setNumAborts(long numAborts) {
        this.numAborts = numAborts;
    }
}
