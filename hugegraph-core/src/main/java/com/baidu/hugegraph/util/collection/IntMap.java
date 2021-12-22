/*
 * Copyright 2017 HugeGraph Authors
 *
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

package com.baidu.hugegraph.util.collection;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.eclipse.collections.api.map.primitive.MutableIntIntMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;

import com.baidu.hugegraph.iterator.ExtendableIterator;
import com.baidu.hugegraph.iterator.MapperIterator;
import com.baidu.hugegraph.util.E;

import io.netty.util.internal.shaded.org.jctools.util.UnsafeAccess;
import sun.misc.Unsafe;

public interface IntMap {

    public boolean put(int key, int value);
    public int get(int key);
    public boolean remove(int key);
    public boolean containsKey(int key);

    public Iterator<Integer> keys();
    public Iterator<Integer> values();

    public void clear();
    public int size();

    public boolean concurrent();

    public static final int NULL = Integer.MIN_VALUE;
    public static final int CPUS = Runtime.getRuntime().availableProcessors();

    /**
     * NOTE: IntMapBySegments(backend by IntMapByFixedAddr) is:
     * - slower 4x than IntMapByFixedAddr for 4 threads;
     * - faster 10x than ec IntIntHashMap-segment-lock for 4 threads;
     * - faster 20x than ec IntIntHashMap-global-lock for 4 threads;
     */
    public static final class IntMapBySegments implements IntMap {

        private final IntMap[] maps;
        private final long capacity;
        private final long unsignedSize;
        private final int segmentSize;
        private final int segmentShift;
        private final int segmentMask;
        private final Function<Integer, IntMap> creator;

        private static final Unsafe UNSAFE = IntMapByFixedAddr.UNSAFE;
        @SuppressWarnings("static-access")
        private static final int BASE_OFFSET = UNSAFE.ARRAY_OBJECT_BASE_OFFSET;
        @SuppressWarnings("static-access")
        private static final int SHIFT = 31 - Integer.numberOfLeadingZeros(
                                              UNSAFE.ARRAY_OBJECT_INDEX_SCALE);

        public IntMapBySegments(int capacity) {
            this(capacity, CPUS * 100, size -> new IntMapByFixedAddr(size));
        }

        public IntMapBySegments(int capacity, int segments,
                                Function<Integer, IntMap> creator) {
            E.checkArgument(segments >= 1,
                            "Invalid segments %s", segments);
            E.checkArgument(capacity >= segments,
                            "Invalid capacity %s, expect >= segments %s",
                            capacity, segments);

            this.maps = new IntMap[segments];
            // include signed and unsigned number
            this.unsignedSize = capacity;
            this.capacity = this.unsignedSize * 2L;
            this.segmentSize = IntMapByFixedAddr.segmentSize(
                               this.capacity, segments);
            this.segmentShift = Integer.numberOfTrailingZeros(this.segmentSize);
            /*
             * The mask is lower bits of each segment size, like
             * segmentSize=4096 (0x1000), segmentMask=4095 (0xfff),
             * NOTE: `-1 >>> 0` or `-1 >>> 32` is -1.
             */
            this.segmentMask = this.segmentShift == 0 ?
                               0 : -1 >>> (32 - this.segmentShift);
            this.creator = creator;
        }

        @Override
        public boolean put(int key, int value) {
            int innerKey = (int) ((key + this.unsignedSize) & this.segmentMask);
            return map(key).put(innerKey, value);
        }

        @Override
        public boolean remove(int key) {
            int innerKey = (int) ((key + this.unsignedSize) & this.segmentMask);
            return map(key).remove(innerKey);
        }

        @Override
        public int get(int key) {
            long ukey = key + this.unsignedSize;
            if (ukey >= this.capacity || ukey < 0L) {
                return NULL;
            }
            int innerKey = (int) (ukey & this.segmentMask);
            return map(key).get(innerKey);
        }

        @Override
        public boolean containsKey(int key) {
            long ukey = key + this.unsignedSize;
            if (ukey >= this.capacity || ukey < 0L) {
                return false;
            }
            int innerKey = (int) (ukey & this.segmentMask);
            return map(key).containsKey(innerKey);
        }

        @Override
        public void clear() {
            for (IntMap map : this.maps) {
                if (map != null) {
                    map.clear();
                }
            }
        }

        @Override
        public int size() {
            int size = 0;
            for (IntMap map : this.maps) {
                if (map != null) {
                    size += map.size();
                }
            }
            return size;
        }

        @Override
        public Iterator<Integer> keys() {
            ExtendableIterator<Integer> iters = new ExtendableIterator<>();
            for (int i = 0; i < this.maps.length; i++) {
                IntMap map = this.maps[i];
                if (map == null || map.size() == 0) {
                    continue;
                }
                int base = this.segmentSize * i;
                iters.extend(new MapperIterator<>(map.keys(), k -> {
                    return (int) (k + base - this.unsignedSize);
                }));
            }
            return iters;
        }

        @Override
        public Iterator<Integer> values() {
            ExtendableIterator<Integer> iters = new ExtendableIterator<>();
            for (IntMap map : this.maps) {
                if (map != null && map.size() > 0) {
                    iters.extend(map.values());
                }
            }
            return iters;
        }

        @Override
        public boolean concurrent() {
            return true;
        }

        private final IntMap map(int key) {
            long ukey = key + this.unsignedSize;
            if (ukey >= this.capacity || ukey < 0L) {
                E.checkArgument(false,
                                "The key %s is out of bound %s",
                                key, this.capacity);
            }

            long index = ukey >>> this.segmentShift;
            IntMap exist = this.maps[(int) index];
            if (exist != null) {
                return exist;
            }

            // volatile get this.maps[index]
            long offset = (index << SHIFT) + BASE_OFFSET;
            Object old = UNSAFE.getObjectVolatile(this.maps, offset);
            if (old != null) {
                return (IntMap) old;
            }

            // set this.maps[index] = new IntMap()
            IntMap map = this.creator.apply(this.segmentSize);
            while (true) {
                if (UNSAFE.compareAndSwapObject(this.maps, offset, null, map)) {
                    return map;
                }
                old = UNSAFE.getObjectVolatile(this.maps, offset);
                if (old != null) {
                    return (IntMap) old;
                }
            }
        }
    }

    /**
     * NOTE: IntMapByFixedAddr is:
     * - faster 3x than ec IntIntHashMap for single thread;
     * - faster 8x than ec IntIntHashMap for 4 threads, 4x operations
     *   with 0.5x cost;
     */
    public static final class IntMapByFixedAddr implements IntMap {

        private final int[] values;
        private final int capacity;
//        private final LongAdder size;
        private final AtomicInteger size;

        private final int indexBlocksNum;
        private final int indexBlockSize;
        private final int indexBlockSizeShift;
        private final IntSet.IntSetByFixedAddr4Unsigned indexBlocksSet;

        private static final sun.misc.Unsafe UNSAFE = UnsafeAccess.UNSAFE;
        @SuppressWarnings("static-access")
        private static final int BASE_OFFSET = UNSAFE.ARRAY_INT_BASE_OFFSET;
        @SuppressWarnings("static-access")
        private static final int SHIFT = 31 - Integer.numberOfLeadingZeros(
                                              UNSAFE.ARRAY_INT_INDEX_SCALE);

        public IntMapByFixedAddr(int capacity) {
            this.capacity = capacity;
            this.values = new int[capacity];
//            this.size = new LongAdder();
            this.size = new AtomicInteger();

            // each block at least >= 1kb
            int minBlockSize = 1 << 10;
            // 64k index blocks by default (indexBlocksSet will cost 8kb memory)
            int indexBlocksNum = 1 << 16;
            int indexBlockSize = IntMapByFixedAddr.segmentSize(
                                 capacity, indexBlocksNum);
            if (indexBlockSize < minBlockSize) {
                indexBlockSize = minBlockSize;
                indexBlocksNum = IntMapByFixedAddr.segmentSize(
                                 capacity, indexBlockSize);
            }
            this.indexBlocksNum = indexBlocksNum;
            this.indexBlockSize = IntMapByFixedAddr.segmentSize(
                                  capacity, this.indexBlocksNum);
            this.indexBlockSizeShift = Integer.numberOfTrailingZeros(
                                       this.indexBlockSize);
            this.indexBlocksSet = new IntSet.IntSetByFixedAddr4Unsigned(
                                  this.indexBlocksNum);

            this.clear();
        }

        @Override
        public boolean put(int key, int value) {
            assert value != NULL : "put value can't be " + NULL;
            if (value == NULL) {
                return false;
            }
            int offset = this.offset(key);
//            int oldV = UNSAFE.getAndSetInt(this.values, offset, value);
//            if (oldV == NULL) {
//                this.size.incrementAndGet();
//            }
//            return true;

//            int oldV = UNSAFE.getIntVolatile(this.values, offset);
//            if (oldV == NULL) {
//                this.size.incrementAndGet();
//            }
//            UNSAFE.putIntVolatile(this.values, offset, value);
//            return true;

//            while (true) {
//                int oldV = UNSAFE.getIntVolatile(this.values, offset);
//                int newV = value;
//                if (newV == oldV) {
//                    // the origin value is newV or other threads set to newV
//                    return false;
//                }
//                if (UNSAFE.compareAndSwapInt(this.values, offset, oldV, newV)) {
//                    if (oldV == NULL) {
//                        this.size.incrementAndGet();
////                        this.size.increment();
//                    }
//                    return true;
//                }
//            }

            int oldV = UNSAFE.getIntVolatile(this.values, offset);
            int newV = value;
            if (newV == oldV) {
                return true;
            }
            if (oldV != NULL) {
                UNSAFE.putIntVolatile(this.values, offset, newV);
            } else {
                if (UNSAFE.compareAndSwapInt(this.values, offset, oldV, newV)) {
                    this.size.incrementAndGet();
//                    this.size.increment();
                    this.indexBlocksSet.add(key >>> this.indexBlockSizeShift);
                }
            }
            return true;
        }

        public boolean putIfAbsent(int key, int value) {
            assert value != NULL;
            int offset = this.offset(key);

            int oldV = UNSAFE.getIntVolatile(this.values, offset);
            int newV = value;
            if (newV == oldV || oldV != NULL) {
                return false;
            }
            if (UNSAFE.compareAndSwapInt(this.values, offset, oldV, newV)) {
                assert oldV == NULL;
                this.size.incrementAndGet();
//                this.size.increment();
                this.indexBlocksSet.add(key >>> this.indexBlockSizeShift);
                return true;
            }
            return false;
        }

        @Override
        public int get(int key) {
            if (key >= this.capacity) {
                return NULL;
            }
            int offset = this.offset(key);
            int value = UNSAFE.getIntVolatile(this.values, offset);
            return value;
        }

        @Override
        public boolean containsKey(int key) {
            if (key >= this.capacity) {
                return false;
            }
            int offset = this.offset(key);
            int value = UNSAFE.getIntVolatile(this.values, offset);
            return value != NULL;
        }

        @Override
        public boolean remove(int key) {
            int offset = this.offset(key);

            while (true) {
                int oldV = UNSAFE.getIntVolatile(this.values, offset);
                int newV = NULL;
                if (newV == oldV) {
                    return false;
                }
                assert oldV != NULL;
                if (UNSAFE.compareAndSwapInt(this.values, offset, oldV, newV)) {
                    this.size.decrementAndGet();
//                    this.size.decrement();
                    return true;
                }
            }
        }

        @Override
        public void clear() {
            Arrays.fill(this.values, NULL);
            this.size.set(0);
//            this.size.reset();
            this.indexBlocksSet.clear();
        }

        @Override
        public int size() {
            return this.size.get();
//            return (int) this.size.sum();
        }

        @Override
        public Iterator<Integer> keys() {
            // NOTE: it's slow to scan KVs when a large number of empty slots
            return new KeyIterator();
        }

        @Override
        public Iterator<Integer> values() {
            // NOTE: it's slow to scan KVs when a large number of empty slots
            return new ValueIterator();
        }

        @Override
        public boolean concurrent() {
            return true;
        }

        private final int offset(int key) {
            if (key >= this.capacity || key < 0) {
                E.checkArgument(false, "The key %s is out of bound %s",
                                key, this.capacity);
            }
            // int key to int offset
            int index = key;
            // int offset to byte offset, and add the array base offset
            int offset = (index << SHIFT) + BASE_OFFSET;
            return offset;
        }

        private static final int segmentSize(long capacity, int segments) {
            long eachSize = capacity / segments;
            eachSize = IntIterator.size2PowerOf2Size((int) eachSize);
            /*
             * Supply total size
             * like capacity=20 and segments=19, then eachSize=1
             * should increase eachSize to eachSize * 2.
             */
            while (eachSize * segments < capacity) {
                eachSize <<= 1;
            }
            return (int) eachSize;
        }

        private final class KeyIterator implements Iterator<Integer> {

            private final int INVALID = -1;

            private int indexOfBlock = 0;
            private int indexInBlock = 0;

            private int current = this.INVALID;

            public KeyIterator() {
                this.indexOfBlock = indexBlocksSet.nextKey(this.indexOfBlock);
            }

            @Override
            public boolean hasNext() {
                if (this.current != this.INVALID) {
                    return true;
                }
                while (this.indexOfBlock < indexBlocksNum) {
                    while (this.indexInBlock < indexBlockSize) {
                        int index = this.indexOfBlock << indexBlockSizeShift;
                        index += this.indexInBlock++;
                        int value = get(index);
                        if (value != NULL) {
                            this.current = index;
                            return true;
                        }
                    }
                    this.indexOfBlock = indexBlocksSet.nextKey(
                                        this.indexOfBlock + 1);
                    this.indexInBlock = 0;
                }
                return false;
            }

            @Override
            public Integer next() {
                if (this.current == this.INVALID) {
                    if (!this.hasNext()) {
                        throw new NoSuchElementException();
                    }
                }
                Integer result = this.current;
                this.current = this.INVALID;
                return result;
            }
        }

        private final class ValueIterator implements Iterator<Integer> {

            private int indexOfBlock = 0;
            private int indexInBlock = 0;

            private int current = NULL;

            public ValueIterator() {
                this.indexOfBlock = indexBlocksSet.nextKey(this.indexOfBlock);
            }

            @Override
            public boolean hasNext() {
                if (this.current != NULL) {
                    return true;
                }
                while (this.indexOfBlock < indexBlocksNum) {
                    while (this.indexInBlock < indexBlockSize) {
                        int index = this.indexOfBlock << indexBlockSizeShift;
                        index += this.indexInBlock++;
                        int value = get(index);
                        if (value != NULL) {
                            this.current = value;
                            return true;
                        }
                    }
                    this.indexOfBlock = indexBlocksSet.nextKey(
                                        this.indexOfBlock + 1);
                    this.indexInBlock = 0;
                }
                return false;
            }

            @Override
            public Integer next() {
                if (this.current == NULL) {
                    if (!this.hasNext()) {
                        throw new NoSuchElementException();
                    }
                }
                Integer result = this.current;
                this.current = NULL;
                return result;
            }
        }
    }
    public static final class IntMapByEcSegment implements IntMap {

        private final MutableIntIntMap[] maps;
        private final int segmentMask;

        public IntMapByEcSegment(int segments) {
            segments = IntIterator.size2PowerOf2Size(segments);
            this.segmentMask = segments - 1;
            this.maps = new MutableIntIntMap[segments];
            for (int i = 0; i < segments; i++) {
                /*
                 * NOTE: asSynchronized() is:
                 * - about slower 3x for single thread;
                 * - about slower 5x for 4 threads, 4x operations with 20x cost;
                 * - about faster 2x than global-lock for 4 threads;
                 */
                this.maps[i] = new IntIntHashMap().asSynchronized();
            }
        }

        private MutableIntIntMap map(int key) {
            // NOTE '%' is slower 20% ~ 50% than '&': key % this.maps.length;
            int index = key & this.segmentMask;
            return this.maps[index];
        }

        @Override
        public boolean put(int key, int value) {
            map(key).put(key, value);
            return true;
        }

        @Override
        public int get(int key) {
            return map(key).get(key);
        }

        @Override
        public boolean containsKey(int key) {
            return map(key).containsKey(key);
        }

        @Override
        public boolean remove(int key) {
            map(key).remove(key);
            return true;
        }

        @Override
        public void clear() {
            for (MutableIntIntMap map : this.maps) {
                map.clear();
            }
        }

        @Override
        public int size() {
            int size = 0;
            for (MutableIntIntMap map : this.maps) {
                size += map.size();
            }
            return size;
        }

        @Override
        public Iterator<Integer> keys() {
            ExtendableIterator<Integer> iters = new ExtendableIterator<>();
            for (MutableIntIntMap map : this.maps) {
                iters.extend(new IntIterator(map.keySet().intIterator()));
            }
            return iters;
        }

        @Override
        public Iterator<Integer> values() {
            ExtendableIterator<Integer> iters = new ExtendableIterator<>();
            for (MutableIntIntMap map : this.maps) {
                iters.extend(new IntIterator(map.values().intIterator()));
            }
            return iters;
        }

        @Override
        public boolean concurrent() {
            return false;
        }
    }
}
