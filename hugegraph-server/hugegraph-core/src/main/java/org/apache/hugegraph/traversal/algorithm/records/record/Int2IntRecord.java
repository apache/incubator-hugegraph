/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.traversal.algorithm.records.record;

import org.apache.hugegraph.util.collection.CollectionFactory;
import org.apache.hugegraph.util.collection.IntIterator;
import org.apache.hugegraph.util.collection.IntMap;

public class Int2IntRecord implements Record {

    private final IntMap layer;

    public Int2IntRecord() {
        this.layer = CollectionFactory.newIntMap();
    }

    @Override
    public IntIterator keys() {
        return this.layer.keys();
    }

    @Override
    public boolean containsKey(int node) {
        return this.layer.containsKey(node);
    }

    @Override
    public IntIterator get(int node) {
        int value = this.layer.get(node);
        if (value == IntMap.NULL_VALUE) {
            return IntIterator.EMPTY;
        }
        return IntIterator.wrap(value);
    }

    @Override
    public void addPath(int node, int parent) {
        this.layer.put(node, parent);
    }

    @Override
    public int size() {
        return this.layer.size();
    }

    @Override
    public boolean concurrent() {
        return this.layer.concurrent();
    }

    public IntMap layer() {
        return this.layer;
    }

    @Override
    public String toString() {
        return this.layer.toString();
    }
}
