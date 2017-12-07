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

package com.baidu.hugegraph.iterator;

import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;

public class ExtendableIterator<T> implements Iterator<T> {

    private final Deque<Iterator<T>> itors;

    public ExtendableIterator() {
        this.itors = new ConcurrentLinkedDeque<Iterator<T>>();
    }

    public ExtendableIterator(Iterator<T> itor) {
        this();
        this.extend(itor);
    }

    public ExtendableIterator(Iterator<T> itor1, Iterator<T> itor2) {
        this();
        this.extend(itor1);
        this.extend(itor2);
    }

    @Override
    public boolean hasNext() {
        // this is true since we never hold empty iterators
        return !this.itors.isEmpty() && this.itors.peekLast().hasNext();
    }

    @Override
    public T next() {
        T next = this.itors.peekFirst().next();
        if (!this.itors.peekFirst().hasNext()) {
            this.itors.removeFirst();
        }
        return next;
    }

    public ExtendableIterator<T> extend(Iterator<T> itor) {
        if (itor != null && itor.hasNext()) {
            this.itors.addLast(itor);
        }
        return this;
    }
}
