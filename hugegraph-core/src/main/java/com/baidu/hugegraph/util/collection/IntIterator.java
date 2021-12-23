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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public interface IntIterator {

    public final static int[] EMPTY = new int[0];

    public boolean hasNext();

    public int next();

    public default Iterator<Integer> asIterator() {
        return new Iterator<Integer>() {

            @Override
            public boolean hasNext() {
                return IntIterator.this.hasNext();
            }

            @Override
            public Integer next() {
                return IntIterator.this.next();
            }
        };
    }

    public static IntIterator wrap(
                  org.eclipse.collections.api.iterator.IntIterator iter) {
        return new EcIntIterator(iter);
    }

    public static IntIterator wrap(int[] values) {
        return new ArrayIntIterator(values);
    }

    public final class ArrayIntIterator implements IntIterator {

        private final int[] array;
        private int index;

        public ArrayIntIterator(int[] array) {
            this.array = array;
            this.index = 0;
        }

        @Override
        public int next() {
            return this.array[this.index++];
        }

        @Override
        public boolean hasNext() {
            return this.index < this.array.length;
        }
    }

    public final class EcIntIterator implements IntIterator {

        private final org.eclipse.collections.api.iterator.IntIterator iterator;

        public EcIntIterator(org.eclipse.collections.api.iterator.IntIterator
                             iterator) {
            this.iterator = iterator;
        }

        @Override
        public int next() {
            return this.iterator.next();
        }

        @Override
        public boolean hasNext() {
            return this.iterator.hasNext();
        }
    }

    public final class IntIterators implements IntIterator {

        private final List<IntIterator> iters;
        private int currentIndex;
        private IntIterator currentIter;

        public IntIterators(int size) {
            this.iters = new ArrayList<>(size);
            this.currentIndex = 0;
            this.currentIter = null;
        }

        public void extend(IntIterator iter) {
            this.iters.add(iter);
        }

        @Override
        public boolean hasNext() {
            if (this.currentIter == null || !this.currentIter.hasNext()) {
                IntIterator iter = null;
                do {
                    if (this.currentIndex >= this.iters.size()) {
                        return false;
                    }
                    iter = this.iters.get(this.currentIndex++);
                } while (!iter.hasNext());
                assert iter.hasNext();
                this.currentIter = iter;
            }
            return true;
        }

        @Override
        public int next() {
            if (this.currentIter == null) {
                if (!this.hasNext()) {
                    throw new NoSuchElementException();
                }
            }
            return this.currentIter.next();
        }
    }

    public final class MapperInt2IntIterator implements IntIterator {

        private final IntIterator originIter;
        private final IntMapper intMapper;

        public MapperInt2IntIterator(IntIterator iter, IntMapper intMapper) {
            this.originIter = iter;
            this.intMapper = intMapper;
        }

        @Override
        public boolean hasNext() {
            return this.originIter.hasNext();
        }

        @Override
        public int next() {
            return intMapper.map(this.originIter.next());
        }

        public interface IntMapper {

            public int map(int key);
        }
    }

    public final class MapperInt2ObjectIterator<T> implements Iterator<T> {

        private final IntIterator originIter;
        private final IntMapper<T> intMapper;

        public MapperInt2ObjectIterator(IntIterator iter,
                                        IntMapper<T> intMapper) {
            this.originIter = iter;
            this.intMapper = intMapper;
        }

        @Override
        public boolean hasNext() {
            return this.originIter.hasNext();
        }

        @Override
        public T next() {
            return intMapper.map(this.originIter.next());
        }

        public interface IntMapper<T> {

            public T map(int key);
        }
    }
}
