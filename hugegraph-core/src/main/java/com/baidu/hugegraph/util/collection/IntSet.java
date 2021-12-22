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
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.collections.api.collection.primitive.MutableIntCollection;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import com.baidu.hugegraph.util.E;

import io.netty.util.internal.shaded.org.jctools.util.UnsafeAccess;

public interface IntSet {

    public boolean add(int key);
    public boolean remove(int key);
    public boolean contains(int key);

    public void clear();
    public int size();

    public default boolean concurrent() {
        return true;
    }

    public static final class IntSetByEcSegment implements IntSet {

        private final MutableIntCollection[] sets;
        private final int segmentMask;

        public IntSetByEcSegment(int segments) {
            segments = IntIterator.size2PowerOf2Size(segments);
            this.segmentMask = segments - 1;
            this.sets = new MutableIntCollection[segments];
            for (int i = 0; i < segments; i++) {
                /*
                 * NOTE: asSynchronized() is:
                 * - about slower 2x for single thread;
                 * - about slower 4x for 4 threads, 4x operations with 16x cost;
                 * - about faster 20x than global-lock for 4 threads;
                 */
                this.sets[i] = new IntHashSet().asSynchronized();
//                this.sets[i] = new IntHashSet();
            }
        }

        private final MutableIntCollection set(int key) {
            // NOTE '%' is slower 20% ~ 50% than '&': key % this.sets.length;
            int index = key & this.segmentMask;
            return this.sets[index];
        }

        @Override
        public boolean add(int key) {
            return set(key).add(key);
        }

        @Override
        public boolean contains(int key) {
            return set(key).contains(key);
        }

        @Override
        public boolean remove(int key) {
            return set(key).remove(key);
        }

        @Override
        public void clear() {
            for (MutableIntCollection set : this.sets) {
                set.clear();
            }
        }

        @Override
        public int size() {
            int size = 0;
            for (MutableIntCollection set : this.sets) {
                size += set.size();
            }
            return size;
        }
    }

    /**
     * NOTE: IntSetByFixedAddr is:
     * - faster 3x than ec IntIntHashSet for single thread;
     * - faster 6x than ec IntIntHashSet for 4 threads, 4x operations
     *   with 0.67x cost;
     * - faster 20x than ec IntIntHashSet-segment-lock for 4 threads;
     * - faster 60x than ec IntIntHashSet-global-lock for 4 threads;
     */
    public static final class IntSetByFixedAddr implements IntSet {

        private final long[] bits;
        private final long numBits;
        private final long numBitsUnsigned;
//        private final LongAdder size;
        private final AtomicInteger size;

        private static final sun.misc.Unsafe UNSAFE = UnsafeAccess.UNSAFE;
        @SuppressWarnings("static-access")
        private static final int BASE_OFFSET = UNSAFE.ARRAY_LONG_BASE_OFFSET;
        @SuppressWarnings("static-access")
        private static final int SHIFT = 31 - Integer.numberOfLeadingZeros(
                                              UNSAFE.ARRAY_LONG_INDEX_SCALE);
        private static final int MOD64 = 0x3f;

        public IntSetByFixedAddr(int numBits) {
            this.numBitsUnsigned = numBits;
            this.numBits = numBits * 2L;
            this.bits = new long[bits2words(this.numBits)];;
//            this.size = new LongAdder();
            this.size = new AtomicInteger();
        }

        @Override
        public boolean add(int key) {
            long ukey = key + this.numBitsUnsigned;
            long offset = this.offset(key);
            long bit = ukey & MOD64;
            long bitmask = 1L << bit;

            while (true) {
                long oldV = UNSAFE.getLongVolatile(this.bits, offset);
                long newV = oldV | bitmask;
                if (newV == oldV) {
                    return false;
                }
                // this.bits[index] |= bitmask;
                if (UNSAFE.compareAndSwapLong(this.bits, offset, oldV, newV)) {
                    this.size.incrementAndGet();
//                    this.size.increment();
                    return true;
                }
            }
        }

        @Override
        public boolean contains(int key) {
//            int index = key >> 6; // div 64
//            if (index >= this.bits.length) {
//                return false;
//            }
//
//            int bit = key & MOD64; // mod 64
//            long bitmask = 1L << bit;
//            return (this.bits[index] & bitmask) != 0;
            long ukey = key + this.numBitsUnsigned;
            if (ukey >= this.numBits || ukey < 0L) {
                return false;
            }
            long offset = this.offset(key);
            long bit = ukey & MOD64;
            long bitmask = 1L << bit;

            long value = UNSAFE.getLongVolatile(this.bits, offset);
            return (value & bitmask) != 0L;
        }

        @Override
        public boolean remove(int key) {
//            int index = key >> 6; // div 64
//            if (index >= this.bits.length) {
//                return false;
//            }
//            int bit = key & MOD64; // mod 64
//            long bitmask = 1L << bit;
//            this.bits[index] &= ~bitmask;
//
//            this.length.decrement();
//            return true;
            long ukey = key + this.numBitsUnsigned;
            long offset = this.offset(key);
            long bit = ukey & MOD64;
            long bitmask = 1L << bit;

            while (true) {
                long oldV = UNSAFE.getLongVolatile(this.bits, offset);
                long newV = oldV & ~bitmask;
                if (newV == oldV) {
                    return false;
                }
                // this.bits[index] &= ~bitmask
                if (UNSAFE.compareAndSwapLong(this.bits, offset, oldV, newV)) {
                    this.size.decrementAndGet();
//                    this.size.decrement();
                    return true;
                }
            }
        }

        @Override
        public void clear() {
            Arrays.fill(this.bits, 0);
            this.size.set(0);
//            this.size.reset();
        }

        @Override
        public int size() {
            return this.size.get();
//            return (int) this.size.sum();
        }

        private final long offset(long key) {
            long ukey = key + this.numBitsUnsigned;
            if (ukey >= this.numBits || ukey < 0L) {
                E.checkArgument(false, "The key %s is out of bound %s",
                                key, this.numBits);
            }
            // bits to long offset
            long index = ukey >> 6;
            // long offset to byte offset, and add the array base offset
            long offset = (index << SHIFT) + BASE_OFFSET;
            return offset;
        }

        private static final int bits2words(long numBits) {
            return (int) ((numBits - 1) >>> 6) + 1;
        }
    }

    public static final class IntSetByFixedAddr4Unsigned implements IntSet {

        private final long[] bits;
        private final int numBits;
//        private final LongAdder size;
        private final AtomicInteger size;

        private static final sun.misc.Unsafe UNSAFE = UnsafeAccess.UNSAFE;
        @SuppressWarnings("static-access")
        private static final int BASE_OFFSET = UNSAFE.ARRAY_LONG_BASE_OFFSET;
        @SuppressWarnings("static-access")
        private static final int SHIFT = 31 - Integer.numberOfLeadingZeros(
                                              UNSAFE.ARRAY_LONG_INDEX_SCALE);
        private static final int MOD64 = 0x3f;

        public IntSetByFixedAddr4Unsigned(int numBits) {
            this.numBits = numBits;
            this.bits = new long[bits2words(numBits)];;
//            this.size = new LongAdder();
            this.size = new AtomicInteger();
        }

        @Override
        public boolean add(int key) {
            int offset = this.offset(key);
            int bit = key & MOD64;
            long bitmask = 1L << bit;

            while (true) {
                long oldV = UNSAFE.getLongVolatile(this.bits, offset);
                long newV = oldV | bitmask;
                if (newV == oldV) {
                    return false;
                }
                // this.bits[index] |= bitmask;
                if (UNSAFE.compareAndSwapLong(this.bits, offset, oldV, newV)) {
                    this.size.incrementAndGet();
//                    this.size.increment();
                    return true;
                }
            }
        }

        @Override
        public boolean contains(int key) {
//            int index = key >> 6; // div 64
//            if (index >= this.bits.length) {
//                return false;
//            }
//
//            int bit = key & MOD64; // mod 64
//            long bitmask = 1L << bit;
//            return (this.bits[index] & bitmask) != 0;
            if (key >= this.numBits || key < 0) {
                return false;
            }
            int offset = this.offset(key);
            int bit = key & MOD64;
            long bitmask = 1L << bit;

            long value = UNSAFE.getLongVolatile(this.bits, offset);
            return (value & bitmask) != 0L;
        }

        @Override
        public boolean remove(int key) {
//            int index = key >> 6; // div 64
//            if (index >= this.bits.length) {
//                return false;
//            }
//            int bit = key & MOD64; // mod 64
//            long bitmask = 1L << bit;
//            this.bits[index] &= ~bitmask;
//
//            this.length.decrement();
//            return true;
            int offset = this.offset(key);
            int bit = key & MOD64;
            long bitmask = 1L << bit;

            while (true) {
                long oldV = UNSAFE.getLongVolatile(this.bits, offset);
                long newV = oldV & ~bitmask;
                if (newV == oldV) {
                    return false;
                }
                // this.bits[index] &= ~bitmask
                if (UNSAFE.compareAndSwapLong(this.bits, offset, oldV, newV)) {
                    this.size.decrementAndGet();
//                    this.size.decrement();
                    return true;
                }
            }
        }

        @Override
        public void clear() {
            Arrays.fill(this.bits, 0);
            this.size.set(0);
//            this.size.reset();
        }

        @Override
        public int size() {
            return this.size.get();
//            return (int) this.size.sum();
        }

        public int nextKey(int key) {
            if (key < 0) {
                key = 0;
            }
            if (key >= this.numBits) {
                return key;
            }

            int offset = this.offset(key);
            int startBit = key & MOD64;
            int bitsEachLong = 64;
            int bytesEachLong = 8;
            key -= startBit;

            // check the first long
            long value = UNSAFE.getLongVolatile(this.bits, offset);
            if (value != 0L) {
                for (int bit = startBit; bit < bitsEachLong; bit++) {
                    long bitmask = 1L << bit;
                    if ((value & bitmask) != 0L) {
                        return key + bit;
                    }
                }
            }
            offset += bytesEachLong;
            key += bitsEachLong;

            // check the remaining
            while (key < this.numBits) {
                value = UNSAFE.getLongVolatile(this.bits, offset);
                if (value != 0L) {
                    for (int bit = 0; bit < bitsEachLong; bit++) {
                        long bitmask = 1L << bit;
                        if ((value & bitmask) != 0L) {
                            return key + bit;
                        }
                    }
                }
                offset += bytesEachLong;
                key += bitsEachLong;
            }
            return key;
        }

        private final int offset(int key) {
            if (key >= this.numBits || key < 0) {
                E.checkArgument(false, "The key %s is out of bound %s",
                                key, this.numBits);
            }
            // bits to long offset
            int index = key >> 6;
            // long offset to byte offset, and add the array base offset
            int offset = (index << SHIFT) + BASE_OFFSET;
            return offset;
        }

        private static final int bits2words(long numBits) {
            return (int) ((numBits - 1) >>> 6) + 1;
        }
    }

    public static final class IntSetByFixedAddrByHppc implements IntSet {

        private final com.carrotsearch.hppc.BitSet bits;

        public IntSetByFixedAddrByHppc(int numBits) {
            this.bits = new com.carrotsearch.hppc.BitSet(numBits);
        }

        @Override
        public boolean add(int key) {
            this.bits.set(key);
            return true;
        }

        @Override
        public boolean remove(int key) {
            this.bits.clear(key);
            return true;
        }

        @Override
        public boolean contains(int key) {
            return this.bits.get(key);
        }

        @Override
        public void clear() {
            this.bits.clear();
        }

        @Override
        public int size() {
            return (int) this.bits.size();
        }
    }
}
