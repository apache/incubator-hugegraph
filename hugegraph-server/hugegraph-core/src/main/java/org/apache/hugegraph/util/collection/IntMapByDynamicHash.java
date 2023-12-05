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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import sun.misc.Unsafe;

public class IntMapByDynamicHash implements IntMap {

    private static final int DEFAULT_INITIAL_CAPACITY = 16;

    /**
     * The maximum capacity, used if a higher value is implicitly specified
     * by either of the constructors with arguments.
     * MUST be a power of two <= 1<<30.
     */
    private static final int MAXIMUM_CAPACITY = 1 << 30;

    private static final float LOAD_FACTOR = 0.75f;

    private static final int PARTITIONED_SIZE_THRESHOLD = 4096;

    private static final int NULL_VALUE = Integer.MIN_VALUE;

    private static final AtomicReferenceFieldUpdater<IntMapByDynamicHash, Entry[]>
        TABLE_UPDATER =
        AtomicReferenceFieldUpdater.newUpdater(IntMapByDynamicHash.class, Entry[].class,
                                               "table");

    private volatile Entry[] table;

    /**
     * Partition counting to improve the concurrency performance of addToSize()
     */
    private int[] partitionedSize;

    private static final Entry RESIZING = new Entry(NULL_VALUE, NULL_VALUE, 1);
    private static final Entry RESIZED = new Entry(NULL_VALUE, NULL_VALUE, 2);

    private static final Entry RESIZE_SENTINEL = new Entry(NULL_VALUE, NULL_VALUE, 3);

    /**
     * must be 2^n - 1
     */
    private static final int SIZE_BUCKETS = 7;


    /* ---------------- Table element access -------------- */
    private static Object tableAt(Object[] array, int index) {
        return UNSAFE.getObjectVolatile(array,
                                        ((long) index << ENTRY_ARRAY_SHIFT) +
                                        ENTRY_ARRAY_BASE);
    }

    private static boolean casTableAt(Object[] array, int index, Object expected, Object newValue) {
        return UNSAFE.compareAndSwapObject(
            array,
            ((long) index << ENTRY_ARRAY_SHIFT) + ENTRY_ARRAY_BASE,
            expected,
            newValue);
    }

    private static void setTableAt(Object[] array, int index, Object newValue) {
        UNSAFE.putObjectVolatile(array, ((long) index << ENTRY_ARRAY_SHIFT) + ENTRY_ARRAY_BASE,
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

    public IntMapByDynamicHash() {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    public IntMapByDynamicHash(int initialCapacity) {
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
        this.table = new Entry[cap + 1];
    }

    @Override
    public boolean put(int key, int value) {
        int hash = this.hash(key);
        Entry[] currentArray = this.table;
        Entry o = (Entry) IntMapByDynamicHash.tableAt(currentArray, hash);
        if (o == null) {
            Entry newEntry = new Entry(key, value);
            this.addToSize(1);
            if (IntMapByDynamicHash.casTableAt(currentArray, hash, null, newEntry)) {
                return true;
            }
            this.addToSize(-1);
        }

        this.slowPut(key, value, currentArray);
        return true;
    }

    private int slowPut(int key, int value, Entry[] currentTable) {
        int length;
        int index;
        Entry o;

        while (true) {
            length = currentTable.length;
            index = this.hash(key, length);
            o = (Entry) IntMapByDynamicHash.tableAt(currentTable, index);

            if (o == RESIZED || o == RESIZING) {
                currentTable = this.helpWithResizeWhileCurrentIndex(currentTable, index);
            } else {
                Entry e = o;
                boolean found = false;

                // Search for the key in the chain
                while (e != null) {
                    int candidate = e.getKey();
                    if (candidate == key) {
                        found = true;
                        break;
                    }
                    e = e.getNext();
                }

                if (found) {
                    int oldVal = e.getValue();
                    // Key found, replace the entry
                    Entry newEntry =
                        new Entry(key, value, this.createReplacementChainForRemoval(o, e));
                    if (IntMapByDynamicHash.casTableAt(currentTable, index, o, newEntry)) {
                        return oldVal;
                    }
                } else {
                    // Key not found, add a new entry
                    Entry newEntry = new Entry(key, value, o);
                    if (IntMapByDynamicHash.casTableAt(currentTable, index, o, newEntry)) {
                        this.incrementSizeAndPossiblyResize(currentTable, length, o);
                        return NULL_VALUE;
                    }
                }
            }
        }
    }

    @Override
    public int get(int key) {
        int hash = this.hash(key);
        Entry[] currentArray = this.table;
        Entry o = (Entry) IntMapByDynamicHash.tableAt(currentArray, hash);
        if (o == RESIZED || o == RESIZING) {
            return this.slowGet(key, currentArray);
        }
        for (Entry e = o; e != null; e = e.getNext()) {
            int k;
            if ((k = e.getKey()) == key || key == k) {
                return e.value;
            }
        }
        return NULL_VALUE;
    }

    private int slowGet(int key, Entry[] currentArray) {
        while (true) {
            int length = currentArray.length;
            int hash = this.hash(key, length);
            Entry o = (Entry) IntMapByDynamicHash.tableAt(currentArray, hash);
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, hash);
            } else {
                Entry e = o;
                while (e != null) {
                    int candidate = e.getKey();
                    if (candidate == key) {
                        return e.getValue();
                    }
                    e = e.getNext();
                }
                return NULL_VALUE;
            }
        }
    }

    @Override
    public boolean remove(int key) {
        int hash = this.hash(key);
        Entry[] currentTable = this.table;
        Entry o = (Entry) IntMapByDynamicHash.tableAt(currentTable, hash);
        if (o == RESIZED || o == RESIZING) {
            return this.slowRemove(key, currentTable) != null;
        }

        Entry e = o;
        while (e != null) {
            int candidate = e.getKey();
            if (candidate == key) {
                Entry replacement = this.createReplacementChainForRemoval(o, e);
                if (IntMapByDynamicHash.casTableAt(currentTable, hash, o, replacement)) {
                    this.addToSize(-1);
                    return true;
                }
                return this.slowRemove(key, currentTable) != null;
            }
            e = e.getNext();
        }
        return false;
    }

    private Entry slowRemove(int key, Entry[] currentTable) {
        int length;
        int index;
        Entry o;

        while (true) {
            length = currentTable.length;
            index = this.hash(key, length);
            o = (Entry) IntMapByDynamicHash.tableAt(currentTable, index);
            if (o == RESIZED || o == RESIZING) {
                currentTable = this.helpWithResizeWhileCurrentIndex(currentTable, index);
            } else {
                Entry e = o;
                Entry prev = null;

                while (e != null) {
                    int candidate = e.getKey();
                    if (candidate == key) {
                        Entry replacement = this.createReplacementChainForRemoval(o, e);
                        if (IntMapByDynamicHash.casTableAt(currentTable, index, o, replacement)) {
                            this.addToSize(-1);
                            return e;
                        }
                        // Key found, but CAS failed, restart the loop
                        break;
                    }
                    prev = e;
                    e = e.getNext();
                }

                if (prev != null) {
                    // Key not found
                    return null;
                }
            }
        }
    }

    @Override
    public boolean containsKey(int key) {
        return this.getEntry(key) != null;
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
        Entry[] currentArray = this.table;
        ResizeContainer resizeContainer;
        do {
            resizeContainer = null;
            for (int i = 0; i < currentArray.length - 1; i++) {
                Entry o = (Entry) IntMapByDynamicHash.tableAt(currentArray, i);
                if (o == RESIZED || o == RESIZING) {
                    resizeContainer = (ResizeContainer) IntMapByDynamicHash.tableAt(currentArray,
                                                                                    currentArray.length -
                                                                                    1);
                } else if (o != null) {
                    Entry e = o;
                    if (IntMapByDynamicHash.casTableAt(currentArray, i, o, null)) {
                        int removedEntries = 0;
                        while (e != null) {
                            removedEntries++;
                            e = e.getNext();
                        }
                        this.addToSize(-removedEntries);
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

    private Entry getEntry(int key) {
        Entry[] currentArray = this.table;
        while (true) {
            int length = currentArray.length;
            int index = this.hash(key, length);
            Entry o = (Entry) IntMapByDynamicHash.tableAt(currentArray, index);
            if (o == RESIZED || o == RESIZING) {
                currentArray = this.helpWithResizeWhileCurrentIndex(currentArray, index);
            } else {
                Entry e = o;
                while (e != null) {
                    int candidate = e.getKey();
                    if (candidate == key) {
                        return e;
                    }
                    e = e.getNext();
                }
                return null;
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

    private Entry createReplacementChainForRemoval(Entry original,
                                                   Entry toRemove) {
        if (original == toRemove) {
            return original.getNext();
        }
        Entry replacement = null;
        Entry e = original;
        while (e != null) {
            if (e != toRemove) {
                replacement = new Entry(e.getKey(), e.getValue(), replacement);
            }
            e = e.getNext();
        }
        return replacement;
    }

    private void incrementSizeAndPossiblyResize(Entry[] currentArray, int length, Entry prev) {
        this.addToSize(1);
        if (prev != null) {
            int localSize = this.size();
            int threshold = (int) (length * LOAD_FACTOR); // threshold = length * 0.75
            if (localSize + 1 > threshold) {
                this.resize(currentArray);
            }
        }
    }

    private Entry[] helpWithResizeWhileCurrentIndex(Entry[] currentArray, int index) {
        Entry[] newArray = this.helpWithResize(currentArray);
        int helpCount = 0;
        while (IntMapByDynamicHash.tableAt(currentArray, index) != RESIZED) {
            helpCount++;
            newArray = this.helpWithResize(currentArray);
            if ((helpCount & 7) == 0) {
                Thread.yield();
            }
        }
        return newArray;
    }

    private void resize(Entry[] oldTable) {
        this.resize(oldTable, (oldTable.length - 1 << 1) + 1);
    }

    // newSize must be a power of 2 + 1
    @SuppressWarnings("JLM_JSR166_UTILCONCURRENT_MONITORENTER")
    private void resize(Entry[] oldTable, int newSize) {
        int oldCapacity = oldTable.length;
        int end = oldCapacity - 1;
        Entry last = (Entry) IntMapByDynamicHash.tableAt(oldTable, end);
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
        if (last == null || last == RESIZE_SENTINEL) {
            // allocating a new array is too expensive to make this an atomic operation
            synchronized (oldTable) {
                if (IntMapByDynamicHash.tableAt(oldTable, end) == null) {
                    IntMapByDynamicHash.setTableAt(oldTable, end, RESIZE_SENTINEL);
                    if (this.partitionedSize == null && newSize >= PARTITIONED_SIZE_THRESHOLD) {
                        this.partitionedSize = new int[SIZE_BUCKETS * 16];
                    }
                    resizeContainer = new ResizeContainer(new Entry[newSize], oldTable.length - 1);
                    IntMapByDynamicHash.setTableAt(oldTable, end, resizeContainer);
                    ownResize = true;
                }
            }
        }
        if (ownResize) {
            this.transfer(oldTable, resizeContainer);

            Entry[] src = this.table;
            while (!TABLE_UPDATER.compareAndSet(this, oldTable, resizeContainer.nextArray)) {
                /*
                we're in a double resize situation; we'll have to go help until it's our turn
                to set the table
                 */
                if (src != oldTable) {
                    this.helpWithResize(src);
                }
            }
        } else {
            this.helpWithResize(oldTable);
        }
    }

    /**
     * Transfer all entries from src to dest tables
     */
    private void transfer(Entry[] src, ResizeContainer resizeContainer) {
        Entry[] dest = resizeContainer.nextArray;

        for (int j = 0; j < src.length - 1; ) {
            Entry o = (Entry) IntMapByDynamicHash.tableAt(src, j);
            if (o == null) {
                if (IntMapByDynamicHash.casTableAt(src, j, null, RESIZED)) {
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
                Entry e = o;
                if (IntMapByDynamicHash.casTableAt(src, j, o, RESIZING)) {
                    while (e != null) {
                        this.unconditionalCopy(dest, e);
                        e = e.getNext();
                    }
                    IntMapByDynamicHash.setTableAt(src, j, RESIZED);
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
    private Entry[] helpWithResize(Entry[] currentArray) {
        ResizeContainer resizeContainer =
            (ResizeContainer) IntMapByDynamicHash.tableAt(currentArray,
                                                          currentArray.length - 1);
        Entry[] newTable = resizeContainer.nextArray;
        if (resizeContainer.getQueuePosition() > ResizeContainer.QUEUE_INCREMENT) {
            resizeContainer.incrementResizer();
            this.reverseTransfer(currentArray, resizeContainer);
            resizeContainer.decrementResizerAndNotify();
        }
        return newTable;
    }

    private void reverseTransfer(Entry[] src, ResizeContainer resizeContainer) {
        Entry[] dest = resizeContainer.nextArray;
        while (resizeContainer.getQueuePosition() > 0) {
            int start = resizeContainer.subtractAndGetQueuePosition();
            int end = start + ResizeContainer.QUEUE_INCREMENT;
            if (end > 0) {
                if (start < 0) {
                    start = 0;
                }
                for (int j = end - 1; j >= start; ) {
                    Entry o = (Entry) IntMapByDynamicHash.tableAt(src, j);
                    if (o == null) {
                        if (IntMapByDynamicHash.casTableAt(src, j, null, RESIZED)) {
                            j--;
                        }
                    } else if (o == RESIZED || o == RESIZING) {
                        resizeContainer.zeroOutQueuePosition();
                        return;
                    } else {
                        Entry e = o;
                        if (IntMapByDynamicHash.casTableAt(src, j, o, RESIZING)) {
                            while (e != null) {
                                this.unconditionalCopy(dest, e);
                                e = e.getNext();
                            }
                            IntMapByDynamicHash.setTableAt(src, j, RESIZED);
                            j--;
                        }
                    }
                }
            }
        }
    }

    private void unconditionalCopy(Entry[] dest, Entry toCopyEntry) {
        Entry[] currentArray = dest;
        while (true) {
            int length = currentArray.length;
            int index = this.hash(toCopyEntry.getKey(), length);
            Entry o = (Entry) IntMapByDynamicHash.tableAt(currentArray, index);
            if (o == RESIZED || o == RESIZING) {
                currentArray = ((ResizeContainer) IntMapByDynamicHash.tableAt(currentArray,
                                                                              length -
                                                                              1)).nextArray;
            } else {
                Entry newEntry;
                if (o == null) {
                    if (toCopyEntry.getNext() == null) {
                        newEntry = toCopyEntry; // no need to duplicate
                    } else {
                        newEntry = new Entry(toCopyEntry.getKey(), toCopyEntry.getValue());
                    }
                } else {
                    newEntry =
                        new Entry(toCopyEntry.getKey(), toCopyEntry.getValue(), o);
                }
                if (IntMapByDynamicHash.casTableAt(currentArray, index, o, newEntry)) {
                    return;
                }
            }
        }
    }

    private static final class ResizeContainer extends Entry {
        private static final int QUEUE_INCREMENT =
            Math.min(1 << 10,
                     Integer.highestOneBit(Runtime.getRuntime().availableProcessors()) << 4);
        private final AtomicInteger resizers = new AtomicInteger(1);
        private final Entry[] nextArray;
        private final AtomicInteger queuePosition;

        private ResizeContainer(Entry[] nextArray, int oldSize) {
            super(NULL_VALUE, NULL_VALUE, 4);
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

    private static class Entry {
        final int key;
        volatile int value;
        volatile Entry next;

        /**
         * 0 NORMAL
         * 1 RESIZING
         * 2 RESIZED
         * 3 RESIZE_SENTINEL
         * 4 ResizeContainer
         */
        final int state;

        public Entry(int key, int value, int state) {
            this.key = key;
            this.value = value;
            this.state = state;
        }

        public Entry(int key, int value) {
            this.key = key;
            this.value = value;
            this.next = null;
            this.state = 0;
        }

        public Entry(int key, int value, Entry next) {
            this.key = key;
            this.value = value;
            this.next = next;
            this.state = 0;
        }

        public int getKey() {
            return key;
        }

        public int getValue() {
            return value;
        }

        public Entry getNext() {
            return next;
        }

        @Override
        public String toString() {
            return this.key + "=" + this.value;
        }
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
            Class<?> tableClass = Entry[].class;
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

            Class<?> mapClass = IntMapByDynamicHash.class;
            SIZE_OFFSET = UNSAFE.objectFieldOffset(mapClass.getDeclaredField("size"));
        } catch (NoSuchFieldException | SecurityException e) {
            throw new AssertionError(e);
        }
    }
}
