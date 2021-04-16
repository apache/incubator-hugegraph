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

package com.baidu.hugegraph.traversal.algorithm.records;

import org.eclipse.collections.api.iterator.IntIterator;

import com.baidu.hugegraph.util.collection.Int2IntsMap;

public class IntArrayRecord implements Record {

    private final Int2IntsMap layer;

    public IntArrayRecord() {
        this.layer = new Int2IntsMap();
    }

    @Override
    public IntIterator keys() {
        return this.layer.keyIterator();
    }

    @Override
    public boolean containsKey(int key) {
        return this.layer.containsKey(key);
    }

    @Override
    public IntIterator get(int key) {
        return new IntArrayIterator(this.layer.get(key));
    }

    @Override
    public void addPath(int node, int parent) {
        this.layer.add(node, parent);
    }

    @Override
    public int size() {
        return this.layer.size();
    }

    public class IntArrayIterator implements IntIterator {

        private final int[] array;
        private int index;

        public IntArrayIterator(int[] array) {
            this.array = array;
            this.index = 0;
        }

        @Override
        public int next() {
            return this.array[index++];
        }

        @Override
        public boolean hasNext() {
            return this.index < this.array.length;
        }
    }
}
