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

import com.baidu.hugegraph.util.E;

public class ExtendableIterator<T> extends WrappedIterator<T> {

    private final Deque<Iterator<T>> itors;

    private Iterator<T> currentIterator;

    public ExtendableIterator() {
        this.itors = new ConcurrentLinkedDeque<>();
        this.currentIterator = null;
    }

    public ExtendableIterator(Iterator<T> iter) {
        this();
        this.extend(iter);
    }

    public ExtendableIterator(Iterator<T> itor1, Iterator<T> itor2) {
        this();
        this.extend(itor1);
        this.extend(itor2);
    }

    public ExtendableIterator<T> extend(Iterator<T> iter) {
        E.checkState(this.currentIterator == null,
                     "Can't extend iterator after iterating");
        if (iter != null) {
            this.itors.addLast(iter);
        }
        return this;
    }

    @Override
    public void close() throws Exception {
        for (Iterator<T> iter : this.itors) {
            if (iter instanceof AutoCloseable) {
                ((AutoCloseable) iter).close();
            }
        }
    }

    @Override
    protected Iterator<T> originIterator() {
        return this.currentIterator;
    }

    @Override
    protected boolean fetch() {
        assert this.current == none();
        if (this.itors.isEmpty()) {
            return false;
        }

        if (this.currentIterator != null && this.currentIterator.hasNext()) {
            this.current = this.currentIterator.next();
            return true;
        }

        Iterator<T> first = null;
        while ((first = this.itors.peekFirst()) != null && !first.hasNext()) {
            if (first == this.itors.peekLast() && this.itors.size() == 1) {
                this.currentIterator = first;
                // The last one
                return false;
            }
            close(this.itors.removeFirst());
        }

        assert first != null && first.hasNext();
        this.currentIterator = first;
        this.current = this.currentIterator.next();
        return true;
    }
}
