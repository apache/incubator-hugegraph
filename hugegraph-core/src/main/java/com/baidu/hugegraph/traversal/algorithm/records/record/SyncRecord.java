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

import com.baidu.hugegraph.util.collection.IntIterator;

public class SyncRecord implements Record {

    private final Object lock;
    private final Record record;

    public SyncRecord(Record record) {
        this(record, null);
    }

    public SyncRecord(Record record, Object newLock) {
        if (record == null) {
            throw new IllegalArgumentException(
                      "Cannot create a SyncRecord on a null record");
        } else {
            this.record = record;
            this.lock = newLock == null ? this : newLock;
        }
    }

    @Override
    public Iterator<Integer> keys() {
        /*
         * Another threads call addPath() will change IntIterator inner array,
         * but in kout/kneighbor scenario it's ok because keys() and addPath()
         * won't be called simultaneously on same Record.
         */
        synchronized (this.lock) {
            return this.record.keys();
        }
    }

    @Override
    public boolean containsKey(int key) {
        synchronized (this.lock) {
            return this.record.containsKey(key);
        }
    }

    @Override
    public IntIterator get(int key) {
        synchronized (this.lock) {
            return this.record.get(key);
        }
    }

    @Override
    public void addPath(int node, int parent) {
        synchronized (this.lock) {
            this.record.addPath(node, parent);
        }
    }

    @Override
    public int size() {
        synchronized (this.lock) {
            return this.record.size();
        }
    }

    @Override
    public boolean concurrent() {
        return true;
    }
}
