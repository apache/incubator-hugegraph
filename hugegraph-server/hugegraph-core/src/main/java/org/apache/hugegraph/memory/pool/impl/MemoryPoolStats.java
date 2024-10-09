/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.memory.pool.impl;

public class MemoryPoolStats {

    private final String memoryPoolName;
    private long maxCapacity;
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
        return String.format("MemoryPool-%s: {maxCapacity[%d], usedBytes[%d], reservedBytes[%d]," +
                             "cumulativeBytes[%d], allocatedBytes[%d], numShrinks[%d], " +
                             "numExpands[%d], numAborts[%d]}.", memoryPoolName, maxCapacity,
                             usedBytes,
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

    public long getMaxCapacity() {
        return maxCapacity;
    }

    public void setMaxCapacity(long maxCapacity) {
        this.maxCapacity = maxCapacity;
    }
}
