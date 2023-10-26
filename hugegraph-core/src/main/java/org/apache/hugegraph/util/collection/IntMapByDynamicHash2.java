/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.util.collection;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import sun.misc.Unsafe;

public class IntMapByDynamicHash2 implements IntMap {

    private static final int DEFAULT_INITIAL_CAPACITY = 16;

    /**
     * The maximum capacity, used if a higher value is implicitly specified
     * by either of the constructors with arguments.
     * MUST be a power of two <= 1<<30.
     */
    private static final int MAXIMUM_CAPACITY = 1 << 30;

    private static final float LOAD_FACTOR = 0.75f;

    private static final int PARTITIONED_SIZE_THRESHOLD = 4096;

    private static final long NULL_VALUE = Long.MIN_VALUE;

    private static final int INT_NULL_VALUE = Integer.MIN_VALUE;

    /**
     * max number of probes to find an item
     */
    private static final int MAX_PROBES = 6;

    private volatile long[] table;

    /**
     * Partition counting to improve the concurrency performance of addToSize()
     */
    private int[] partitionedSize;

    private volatile ResizeContainer resizeContainer;

    private static final long RESIZING = Long.MIN_VALUE + 1;
    private static final long RESIZED = Long.MIN_VALUE + 2;

    private static final long RESIZE_SENTINEL = Long.MIN_VALUE + 3;

    /**
     * must be 2^n - 1
     */
    private static final int SIZE_BUCKETS = 7;


    private static final AtomicReferenceFieldUpdater<IntMapByDynamicHash2, long[]>
        TABLE_UPDATER =
        AtomicReferenceFieldUpdater.newUpdater(IntMapByDynamicHash2.class, long[].class,
                                               "table");

    private static final AtomicReferenceFieldUpdater<IntMapByDynamicHash2, ResizeContainer>
        RESIZE_CONTAINER_UPDATER =
        AtomicReferenceFieldUpdater.newUpdater(IntMapByDynamicHash2.class, ResizeContainer.class,
                                               "resizeContainer");


    /* ---------------- Table element access -------------- */
    private static long tableAt(long[] array, int index) {
        return UNSAFE.getLongVolatile(array,
                                      ((long) index << ENTRY_ARRAY_SHIFT) +
                                      ENTRY_ARRAY_BASE);
    }

    private static boolean casTableAt(long[] array, int index, long expected, long newValue) {
        return UNSAFE.compareAndSwapLong(
            array,
            ((long) index << ENTRY_ARRAY_SHIFT) + ENTRY_ARRAY_BASE,
            expected,
            newValue);
    }

    private static void setTableAt(long[] array, int index, long newValue) {
        UNSAFE.putLongVolatile(array, ((long) index << ENTRY_ARRAY_SHIFT) + ENTRY_ARRAY_BASE,
                               newValue);
    }

    private static int tableSizeFor(int c) {
        int n = c - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
    }

    @SuppressWarnings("UnusedDeclaration")
    private volatile int size; // updated via atomic field updater

    public IntMapByDynamicHash2() {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    public IntMapByDynamicHash2(int initialCapacity) {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("Illegal Initial Capacity: " + initialCapacity);
        }
        if (initialCapacity > MAXIMUM_CAPACITY) {
            initialCapacity = MAXIMUM_CAPACITY;
        }
        long size = (long) (1.0 + (long) initialCapacity / LOAD_FACTOR);
        int cap = (size >= (long) MAXIMUM_CAPACITY) ?
                  MAXIMUM_CAPACITY : tableSizeFor((int) size);
        if (cap >= PARTITIONED_SIZE_THRESHOLD) {
            /*
            we want 7 extra slots and 64 bytes for each
            slot. int is 4 bytes, so 64 bytes is 16 ints.
             */
            this.partitionedSize =
                new int[SIZE_BUCKETS * 16];
        }
        // The end index is for resizeContainer
        this.table = new long[cap + 1];
        Arrays.fill(table, NULL_VALUE);
    }

    @Override
    public boolean put(int key, int value) {
        int hash = this.hash(key);
        long[] currentArray = this.table;
        long o = IntMapByDynamicHash2.tableAt(currentArray, hash);
        if (o == NULL_VALUE) {
            long newEntry = combineInts(key, value);
            this.addToSize(1);
            if (IntMapByDynamicHash2.casTableAt(currentArray, hash, NULL_VALUE, newEntry)) {
                return true;
            }
            this.addToSize(-1);
        }

        this.slowPut(key, value, currentArray);
        return true;
    }

    private long slowPut(int key, int value, long[] currentTable) {
        int length;
        int index;
        long o;

        outer:
        while (true) {
            length = currentTable.length;
            index = this.hash(key, length);
            o = IntMapByDynamicHash2.tableAt(currentTable, index);

            if (o == RESIZED || o == RESIZING) {
                currentTable = this.helpWithResizeWhileCurrentIndex(currentTable, index);
            } else {
                long e = o;
                boolean found = false;
                boolean isNeedResize = false;
                int probes = 0;

                // Search for the key(open address)
                while (e != NULL_VALUE) {
                    if (e == RESIZED || e == RESIZING) {
                        currentTable = this.helpWithResizeWhileCurrentIndex(currentTable, index);
                        continue outer;
                    }
                    int candidate = extractKey(e);
                    if (candidate == key) {
                        found = true;
                        break;
                    }
                    if (++index == currentTable.length - 1) {
                        index = 0;
                    }
                    e = IntMapByDynamicHash2.tableAt(currentTable, index);
                    if (++probes > MAX_PROBES) {
                        isNeedResize = true;
                        break;
                    }
                }

                if (isNeedResize) {
                    this.resize(currentTable);
                    currentTable = this.table;
                    continue;
                }

                long newEntry = combineInts(key, value);
                if (found) {
                    // Key found, replace the entry
                    if (IntMapByDynamicHash2.casTableAt(currentTable, index, o, newEntry)) {
                        return e;
                    }
                } else {
                    // Key not found, add a new entry
                    if (IntMapByDynamicHash2.casTableAt(currentTable, index, o, newEntry)) {
                        this.incrementSizeAndPossiblyResize(currentTable, length);
                        return NULL_VALUE;
                    }
                }
            }
        }
    }

    @Override
    public int get(int key) {
        int index = this.hash(key);
        long[] currentTable = this.table;
        long o = IntMapByDynamicHash2.tableAt(currentTable, index);
        if (o == RESIZED || o == RESIZING) {
            return this.slowGet(key, currentTable);
        }
        long e = o;
        for (int i = 0; i < MAX_PROBES; i++) {
            if (e == NULL_VALUE) {
                return INT_NULL_VALUE;
            }
            if (e == RESIZED || e == RESIZING) {
                return this.slowGet(key, currentTable);
            }
            if (extractKey(e) == key) {
                return extractValue(e);
            }
            if (++index == currentTable.length - 1) {
                index = 0;
            }
            e = IntMapByDynamicHash2.tableAt(currentTable, index);
        }
        return INT_NULL_VALUE;
    }

    private int slowGet(int key, long[] currentTable) {
        outer:
        while (true) {
            int length = currentTable.length;
            int index = this.hash(key, length);
            long o = IntMapByDynamicHash2.tableAt(currentTable, index);
            if (o == RESIZED || o == RESIZING) {
                currentTable = this.helpWithResizeWhileCurrentIndex(currentTable, index);
            } else {
                long e = o;
                for (int i = 0; i < MAX_PROBES; i++) {
                    if (e == NULL_VALUE) {
                        return INT_NULL_VALUE;
                    }
                    if (e == RESIZED || e == RESIZING) {
                        currentTable = this.helpWithResizeWhileCurrentIndex(currentTable, index);
                        continue outer;
                    }
                    int candidate = extractKey(e);
                    if (candidate == key) {
                        return extractValue(e);
                    }
                    if (++index == currentTable.length - 1) {
                        index = 0;
                    }
                    e = IntMapByDynamicHash2.tableAt(currentTable, index);
                }
                return INT_NULL_VALUE;
            }
        }
    }

    @Override
    public boolean remove(int key) {
        int index = this.hash(key);
        long[] currentTable = this.table;
        long o = IntMapByDynamicHash2.tableAt(currentTable, index);
        if (o == RESIZED || o == RESIZING) {
            return this.slowRemove(key, currentTable) != NULL_VALUE;
        }

        long e = o;
        for (int i = 0; i < MAX_PROBES; i++) {
            if (e == NULL_VALUE) {
                return false;
            }
            if (e == RESIZED || e == RESIZING) {
                return this.slowRemove(key, currentTable) != NULL_VALUE;
            }
            if (extractKey(e) == key) {
                if (IntMapByDynamicHash2.casTableAt(currentTable, index, o, NULL_VALUE)) {
                    this.addToSize(-1);
                    return true;
                }
                return this.slowRemove(key, currentTable) != NULL_VALUE;
            }
            if (++index == currentTable.length - 1) {
                index = 0;
            }
            e = IntMapByDynamicHash2.tableAt(currentTable, index);
        }
        return false;
    }

    private long slowRemove(int key, long[] currentTable) {
        int length;
        int index;
        long o;

        outer:
        while (true) {
            length = currentTable.length;
            index = this.hash(key, length);
            o = IntMapByDynamicHash2.tableAt(currentTable, index);
            if (o == RESIZED || o == RESIZING) {
                currentTable = this.helpWithResizeWhileCurrentIndex(currentTable, index);
            } else {
                long e = o;
                long prev = NULL_VALUE;

                for (int i = 0; i < MAX_PROBES; i++) {
                    if (e == NULL_VALUE) {
                        return NULL_VALUE;
                    }
                    if (e == RESIZED || e == RESIZING) {
                        currentTable = this.helpWithResizeWhileCurrentIndex(currentTable, index);
                        continue outer;
                    }
                    if (extractKey(e) == key) {
                        if (IntMapByDynamicHash2.casTableAt(currentTable, index, o, NULL_VALUE)) {
                            this.addToSize(-1);
                            return extractValue(e);
                        }
                        // Key found, but CAS failed, restart the loop
                        break;
                    }
                    if (++index == length - 1) {
                        index = 0;
                    }
                    prev = e;
                    e = IntMapByDynamicHash2.tableAt(currentTable, index);
                }

                if (prev != NULL_VALUE) {
                    // Key not found
                    return NULL_VALUE;
                }
            }
        }
    }

    @Override
    public boolean containsKey(int key) {
        return this.getEntry(key) != NULL_VALUE;
    }

    @Override
    public IntIterator keys() {
        // TODO impl
        return null;
    }

    @Override
    public IntIterator values() {
        // TODO impl
        return null;
    }

    @Override
    public void clear() {
        long[] currentArray = this.table;
        ResizeContainer resizeContainer;
        do {
            resizeContainer = null;
            for (int i = 0; i < currentArray.length - 1; i++) {
                long o = IntMapByDynamicHash2.tableAt(currentArray, i);
                if (o == RESIZED || o == RESIZING) {
                    resizeContainer = this.resizeContainer;
                } else if (o != NULL_VALUE) {
                    if (IntMapByDynamicHash2.casTableAt(currentArray, i, o, NULL_VALUE)) {
                        this.addToSize(-1);
                    }
                }
            }
            if (resizeContainer != null) {
                if (resizeContainer.isNotDone()) {
                    this.helpWithResize(currentArray);
                    resizeContainer.waitForAllResizers();
                }
                currentArray = resizeContainer.nextArray;
            }
        } while (resizeContainer != null);
    }

    @Override
    public int size() {
        int localSize = this.size;
        if (this.partitionedSize != null) {
            for (int i = 0; i < SIZE_BUCKETS; i++) {
                localSize += this.partitionedSize[i << 4];
            }
        }
        return localSize;
    }

    @Override
    public boolean concurrent() {
        return true;
    }

    private int hash(int key) {
        return key & (table.length - 2);
    }

    private int hash(int key, int length) {
        return key & (length - 2);
    }

    private long getEntry(int key) {
        long[] currentTable = this.table;
        outer:
        while (true) {
            int length = currentTable.length;
            int index = this.hash(key, length);
            long o = IntMapByDynamicHash2.tableAt(currentTable, index);
            if (o == RESIZED || o == RESIZING) {
                currentTable = this.helpWithResizeWhileCurrentIndex(currentTable, index);
            } else {
                long e = o;
                for (int i = 0; i < MAX_PROBES; i++) {
                    if (e == NULL_VALUE) {
                        return NULL_VALUE;
                    }
                    if (e == RESIZED || e == RESIZING) {
                        currentTable = this.helpWithResizeWhileCurrentIndex(currentTable, index);
                        continue outer;
                    }
                    if (extractKey(e) == key) {
                        return e;
                    }
                    if (++index == length - 1) {
                        index = 0;
                    }
                    e = IntMapByDynamicHash2.tableAt(currentTable, index);
                }
                return NULL_VALUE;
            }
        }
    }

    private void addToSize(int value) {
        if (this.partitionedSize != null) {
            if (this.incrementPartitionedSize(value)) {
                return;
            }
        }
        this.incrementLocalSize(value);
    }

    private boolean incrementPartitionedSize(int value) {
        int h = (int) Thread.currentThread().getId();
        h ^= (h >>> 18) ^ (h >>> 12);
        h = (h ^ (h >>> 10)) & SIZE_BUCKETS;
        if (h != 0) {
            h = (h - 1) << 4;
            long address = ((long) h << INT_ARRAY_SHIFT) + INT_ARRAY_BASE;
            while (true) {
                int localSize = UNSAFE.getIntVolatile(this.partitionedSize, address);
                if (UNSAFE.compareAndSwapInt(this.partitionedSize, address, localSize,
                                             localSize + value)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void incrementLocalSize(int value) {
        while (true) {
            int localSize = this.size;
            if (UNSAFE.compareAndSwapInt(this, SIZE_OFFSET, localSize, localSize + value)) {
                break;
            }
        }
    }

    private void incrementSizeAndPossiblyResize(long[] currentArray, int length) {
        this.addToSize(1);

        int localSize = this.size();
        int threshold = (int) (length * LOAD_FACTOR); // threshold = length * 0.75
        if (localSize + 1 > threshold) {
            this.resize(currentArray);
        }
    }

    private long[] helpWithResizeWhileCurrentIndex(long[] currentArray, int index) {
        long[] newArray = this.helpWithResize(currentArray);
        int helpCount = 0;
        while (IntMapByDynamicHash2.tableAt(currentArray, index) != RESIZED) {
            helpCount++;
            newArray = this.helpWithResize(currentArray);
            if ((helpCount & 7) == 0) {
                Thread.yield();
            }
        }
        return newArray;
    }

    private void resize(long[] oldTable) {
        this.resize(oldTable, (oldTable.length - 1 << 1) + 1);
    }

    // newSize must be a power of 2 + 1
    @SuppressWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
    private void resize(long[] oldTable, int newSize) {
        int oldCapacity = oldTable.length;
        int end = oldCapacity - 1;
        long last = IntMapByDynamicHash2.tableAt(oldTable, end);
        if (this.size() < end && last == RESIZE_SENTINEL) {
            return;
        }
        if (oldCapacity >= MAXIMUM_CAPACITY) {
            throw new RuntimeException("max capacity of map exceeded");
        }
        ResizeContainer resizeContainer = null;
        // This ownResize records whether current thread need to perform the expansion operation of
        // the map by itself
        boolean ownResize = false;
        if (last == NULL_VALUE || last == RESIZE_SENTINEL) {
            // allocating a new array is too expensive to make this an atomic operation
            synchronized (oldTable) {
                if (IntMapByDynamicHash2.tableAt(oldTable, end) == NULL_VALUE) {
                    IntMapByDynamicHash2.setTableAt(oldTable, end, RESIZE_SENTINEL);
                    if (this.partitionedSize == null && newSize >= PARTITIONED_SIZE_THRESHOLD) {
                        this.partitionedSize = new int[SIZE_BUCKETS * 16];
                    }
                    long[] nextTable = new long[newSize];
                    Arrays.fill(nextTable, NULL_VALUE);
                    resizeContainer = new ResizeContainer(nextTable, oldTable.length - 1);
                    RESIZE_CONTAINER_UPDATER.set(this, resizeContainer);
                    ownResize = true;
                }
            }
        }
        if (ownResize) {
            this.transfer(oldTable, resizeContainer);

            long[] src = this.table;
            synchronized (oldTable) {
                long[] next = new long[resizeContainer.nextArray.length];
                System.arraycopy(resizeContainer.nextArray, 0, next, 0,
                                 resizeContainer.nextArray.length);
                TABLE_UPDATER.set(this, next);
                RESIZE_CONTAINER_UPDATER.set(this, null);
            }
            //while (!TABLE_UPDATER.compareAndSet(this, oldTable, resizeContainer.nextArray)) {
            //    /*
            //    we're in a double resize situation; we'll have to go help until it's our turn
            //    to set the table
            //     */
            //    if (src != oldTable) {
            //        this.helpWithResize(src);
            //    }
            //}
            //while (true) {
            //    if (RESIZE_CONTAINER_UPDATER.compareAndSet(this, resizeContainer, null)) {
            //        break;
            //    }
            //}
        } else {
            this.helpWithResize(oldTable);
        }
    }

    /**
     * Transfer all entries from src to dest tables
     */
    private void transfer(long[] src, ResizeContainer resizeContainer) {
        long[] dest = resizeContainer.nextArray;

        for (int j = 0; j < src.length - 1; ) {
            long o = IntMapByDynamicHash2.tableAt(src, j);
            if (o == NULL_VALUE) {
                if (IntMapByDynamicHash2.casTableAt(src, j, NULL_VALUE, RESIZED)) {
                    j++;
                }
            } else if (o == RESIZED || o == RESIZING) {
                /*
                 During the expansion process, other threads have already migrated the elements at
                 this location to the new array. This means that the elements in the current
                 position have already been processed and do not need to be migrated again.
                 */
                j = (j & ~(ResizeContainer.QUEUE_INCREMENT - 1)) + ResizeContainer.QUEUE_INCREMENT;
                /*
                 When there is only one thread for expansion, there is no concurrency issue
                 and there is no need to wait.
                 */
                if (resizeContainer.resizers.get() == 1) {
                    break;
                }
            } else {
                if (IntMapByDynamicHash2.casTableAt(src, j, o, RESIZING)) {
                    this.unconditionalCopy(dest, o);
                    IntMapByDynamicHash2.setTableAt(src, j, RESIZED);
                    j++;
                }
            }
        }
        resizeContainer.decrementResizerAndNotify();
        resizeContainer.waitForAllResizers();
    }

    /**
     * Enable the current thread to participate in the expansion
     */
    private long[] helpWithResize(long[] currentArray) {
        if (resizeContainer == null) {
            //System.out.println("============");
            //System.out.println(Arrays.toString(this.table));
            //System.out.println(Arrays.toString(currentArray));
            //System.out.println("************");
            //throw new RuntimeException("resizeContainer should not be null.");
            return this.table;
        }
        long[] newTable = resizeContainer.nextArray;
        if (resizeContainer.getQueuePosition() > ResizeContainer.QUEUE_INCREMENT) {
            resizeContainer.incrementResizer();
            this.reverseTransfer(currentArray, resizeContainer);
            resizeContainer.decrementResizerAndNotify();
        }
        return newTable;
    }

    private void reverseTransfer(long[] src, ResizeContainer resizeContainer) {
        long[] dest = resizeContainer.nextArray;
        while (resizeContainer.getQueuePosition() > 0) {
            int start = resizeContainer.subtractAndGetQueuePosition();
            int end = start + ResizeContainer.QUEUE_INCREMENT;
            if (end > 0) {
                if (start < 0) {
                    start = 0;
                }
                for (int j = end - 1; j >= start; ) {
                    long o = IntMapByDynamicHash2.tableAt(src, j);
                    if (o == NULL_VALUE) {
                        if (IntMapByDynamicHash2.casTableAt(src, j, NULL_VALUE, RESIZED)) {
                            j--;
                        }
                    } else if (o == RESIZED || o == RESIZING) {
                        resizeContainer.zeroOutQueuePosition();
                        return;
                    } else {
                        if (IntMapByDynamicHash2.casTableAt(src, j, o, RESIZING)) {
                            this.unconditionalCopy(dest, o);
                            IntMapByDynamicHash2.setTableAt(src, j, RESIZED);
                            j--;
                        }
                    }
                }
            }
        }
    }

    private void unconditionalCopy(long[] dest, long toCopyEntry) {
        long[] currentArray = dest;
        while (true) {
            int length = currentArray.length;
            int index = this.hash(extractKey(toCopyEntry), length);
            long o = IntMapByDynamicHash2.tableAt(currentArray, index);
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.resizeContainer.nextArray;
            } else {
                if (IntMapByDynamicHash2.casTableAt(currentArray, index, o, toCopyEntry)) {
                    return;
                }
            }
        }
    }

    private static final class ResizeContainer {
        private static final int QUEUE_INCREMENT =
            Math.min(1 << 10,
                     Integer.highestOneBit(Runtime.getRuntime().availableProcessors()) << 4);
        private final AtomicInteger resizers = new AtomicInteger(1);
        private final long[] nextArray;
        private final AtomicInteger queuePosition;

        private ResizeContainer(long[] nextArray, int oldSize) {
            this.nextArray = nextArray;
            this.queuePosition = new AtomicInteger(oldSize);
        }

        public void incrementResizer() {
            this.resizers.incrementAndGet();
        }

        public void decrementResizerAndNotify() {
            int remaining = this.resizers.decrementAndGet();
            if (remaining == 0) {
                synchronized (this) {
                    this.notifyAll();
                }
            }
        }

        public int getQueuePosition() {
            return this.queuePosition.get();
        }

        public int subtractAndGetQueuePosition() {
            return this.queuePosition.addAndGet(-QUEUE_INCREMENT);
        }

        public void waitForAllResizers() {
            if (this.resizers.get() > 0) {
                for (int i = 0; i < 16; i++) {
                    if (this.resizers.get() == 0) {
                        break;
                    }
                }
                for (int i = 0; i < 16; i++) {
                    if (this.resizers.get() == 0) {
                        break;
                    }
                    Thread.yield();
                }
            }
            if (this.resizers.get() > 0) {
                synchronized (this) {
                    while (this.resizers.get() > 0) {
                        try {
                            this.wait();
                        } catch (InterruptedException e) {
                            //ginore
                        }
                    }
                }
            }
        }

        public boolean isNotDone() {
            return this.resizers.get() > 0;
        }

        public void zeroOutQueuePosition() {
            this.queuePosition.set(0);
        }
    }

    public static long combineInts(int key, int value) {
        return ((long) key << 32) | (value & 0xFFFFFFFFL);
    }

    public static int extractKey(long combined) {
        return (int) (combined >> 32);
    }

    public static int extractValue(long combined) {
        return (int) (combined & 0xFFFFFFFFL);
    }

    /* ---------------- Unsafe mechanics -------------- */
    private static final Unsafe UNSAFE = IntSet.UNSAFE;
    private static final long ENTRY_ARRAY_BASE;
    private static final int ENTRY_ARRAY_SHIFT;
    private static final long INT_ARRAY_BASE;
    private static final int INT_ARRAY_SHIFT;
    private static final long SIZE_OFFSET;

    static {
        try {
            Class<?> tableClass = long[].class;
            ENTRY_ARRAY_BASE = UNSAFE.arrayBaseOffset(tableClass);
            int objectArrayScale = UNSAFE.arrayIndexScale(tableClass);
            if ((objectArrayScale & (objectArrayScale - 1)) != 0) {
                throw new AssertionError("data type scale not a power of two");
            }
            ENTRY_ARRAY_SHIFT = 31 - Integer.numberOfLeadingZeros(objectArrayScale);

            Class<?> intArrayClass = int[].class;
            INT_ARRAY_BASE = UNSAFE.arrayBaseOffset(intArrayClass);
            int intArrayScale = UNSAFE.arrayIndexScale(intArrayClass);
            if ((intArrayScale & (intArrayScale - 1)) != 0) {
                throw new AssertionError("data type scale not a power of two");
            }
            INT_ARRAY_SHIFT = 31 - Integer.numberOfLeadingZeros(intArrayScale);

            Class<?> mapClass = IntMapByDynamicHash2.class;
            SIZE_OFFSET = UNSAFE.objectFieldOffset(mapClass.getDeclaredField("size"));
        } catch (NoSuchFieldException | SecurityException e) {
            throw new AssertionError(e);
        }
    }
}
