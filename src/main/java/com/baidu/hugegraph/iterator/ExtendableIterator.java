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

import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedDeque;

public class ExtendableIterator<T>
       implements Iterator<T>, AutoCloseable, Metadatable {

    private final Deque<Iterator<T>> itors;
    private final List<Iterator<T>> removedItors;

    public ExtendableIterator() {
        this.itors = new ConcurrentLinkedDeque<Iterator<T>>();
        this.removedItors = new ArrayList<>();
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
        if (this.itors.isEmpty()) {
            return false;
        }

        Iterator<T> first = null;
        while ((first = this.itors.peekFirst()) != null && !first.hasNext()) {
            if (first == this.itors.peekLast() && this.itors.size() == 1) {
                // The last one
                return false;
            }
            this.removedItors.add(this.itors.removeFirst());
        }

        assert first != null && first.hasNext();
        return true;
    }

    @Override
    public T next() {
        if (this.itors.isEmpty()) {
            throw new NoSuchElementException();
        }
        return this.itors.peekFirst().next();
    }

    public ExtendableIterator<T> extend(Iterator<T> itor) {
        if (itor != null && itor.hasNext()) {
            this.itors.addLast(itor);
        }
        return this;
    }

    @Override
    public void close() throws Exception {
        for (Iterator<T> itor : this.removedItors) {
            if (itor instanceof AutoCloseable) {
                ((AutoCloseable) itor).close();
            }
        }
        for (Iterator<T> itor : this.itors) {
            if (itor instanceof AutoCloseable) {
                ((AutoCloseable) itor).close();
            }
        }
    }

    @Override
    public Object metadata(String meta, Object... args) {
        Iterator<T> iterator = this.itors.peekLast();
        if (iterator instanceof Metadatable) {
            return ((Metadatable) iterator).metadata(meta, args);
        }
        throw new IllegalStateException("Original iterator is not Metadatable");
    }
}
