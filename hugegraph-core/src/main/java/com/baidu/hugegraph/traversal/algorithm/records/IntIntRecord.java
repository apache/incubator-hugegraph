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
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;

public class IntIntRecord implements Record {

    private IntIntHashMap layer;

    public IntIntRecord() {
        this.layer = new IntIntHashMap();
    }

    @Override
    public IntIterator keys() {
        return this.layer.keySet().intIterator();
    }

    @Override
    public boolean containsKey(int key) {
        return this.layer.containsKey(key);
    }

    @Override
    public IntIterator get(int key) {
        return null;
    }

    @Override
    public void addPath(int node, int parent) {
        this.layer.put(node, parent);
    }

    @Override
    public int size() {
        return this.layer.size();
    }

    public IntIntHashMap layer() {
        return this.layer;
    }
}
