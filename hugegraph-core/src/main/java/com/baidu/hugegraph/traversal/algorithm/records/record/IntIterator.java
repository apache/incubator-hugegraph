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

package com.baidu.hugegraph.traversal.algorithm.records.record;

import java.util.Iterator;

public class IntIterator implements Iterator<Integer> {

    private final static int[] EMPTY = new int[0];
    private final org.eclipse.collections.api.iterator.IntIterator iterator;
    private final int[] array;
    private int index;

    public IntIterator(int[] array) {
        this.iterator = null;
        this.array = array;
        this.index = 0;
    }

    public IntIterator(org.eclipse.collections.api.iterator.IntIterator iter) {
        this.iterator = iter;
        this.array = EMPTY;
        this.index = 0;
    }

    @Override
    public Integer next() {
        if (this.iterator != null) {
            return this.iterator.next();
        }
        return this.array[this.index++];
    }

    @Override
    public boolean hasNext() {
        if (this.iterator != null) {
            return this.iterator.hasNext();
        }
        return this.index < this.array.length;
    }
}
