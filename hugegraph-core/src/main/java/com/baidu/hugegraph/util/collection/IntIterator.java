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

import java.util.Iterator;

public interface IntIterator extends Iterator<Integer> {

    public final static int[] EMPTY = new int[0];

    public int nextInt();

    public static IntIterator wrap(
                  org.eclipse.collections.api.iterator.IntIterator iter) {
        return new EcIntIterator(iter);
    }

    public static IntIterator wrap(int[] values) {
        return new ArrayIntIterator(values);
    }

    public static final class ArrayIntIterator implements IntIterator {

        private final int[] array;
        private int index;

        public ArrayIntIterator(int[] array) {
            this.array = array;
            this.index = 0;
        }

        @Override
        public int nextInt() {
            return this.array[this.index++];
        }

        @Override
        public Integer next() {
            return this.nextInt();
        }

        @Override
        public boolean hasNext() {
            return this.index < this.array.length;
        }
    }

    public static final class EcIntIterator implements IntIterator {

        private final org.eclipse.collections.api.iterator.IntIterator iterator;

        public EcIntIterator(org.eclipse.collections.api.iterator.IntIterator
                             iterator) {
            this.iterator = iterator;
        }

        @Override
        public int nextInt() {
            return this.iterator.next();
        }

        @Override
        public Integer next() {
            return this.nextInt();
        }

        @Override
        public boolean hasNext() {
            return this.iterator.hasNext();
        }
    }

    public static int sizeToPowerOf2Size(int size) {
        if (size < 1) {
            size = 1;
        }

        int n = size - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        size = n + 1;

        return size;
    }
}
