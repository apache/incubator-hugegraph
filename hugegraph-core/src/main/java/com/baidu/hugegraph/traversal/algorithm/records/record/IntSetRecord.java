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

import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

public class IntSetRecord implements Record {

    private final IntObjectHashMap<IntHashSet> layer;

    public IntSetRecord() {
        this.layer = new IntObjectHashMap<>();
    }

    @Override
    public IntIterator keys() {
        return new IntIterator(this.layer.keySet().intIterator());
    }

    @Override
    public boolean containsKey(int key) {
        return this.layer.containsKey(key);
    }

    @Override
    public IntIterator get(int key) {
        return new IntIterator(this.layer.get(key).intIterator());
    }

    @Override
    public void addPath(int node, int parent) {
        if (this.layer.containsKey(node)) {
            this.layer.get(node).add(parent);
        } else {
            this.layer.put(node, IntHashSet.newSetWith(parent));
        }
    }

    @Override
    public int size() {
        return this.layer.size();
    }
}
