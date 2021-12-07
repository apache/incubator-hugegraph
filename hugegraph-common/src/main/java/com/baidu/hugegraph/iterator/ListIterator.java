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

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.baidu.hugegraph.util.InsertionOrderUtil;

public class ListIterator<T> extends WrappedIterator<T> {

    private final Iterator<T> originIterator;
    private final Iterator<T> resultsIterator;
    private final Collection<T> results;

    public ListIterator(long capacity, Iterator<T> origin) {
        List<T> results = InsertionOrderUtil.newList();
        while (origin.hasNext()) {
            if (capacity >= 0L && results.size() >= capacity) {
                throw new IllegalArgumentException(
                          "The iterator exceeded capacity " + capacity);
            }
            results.add(origin.next());
        }
        this.originIterator = origin;
        this.results = Collections.unmodifiableList(results);
        this.resultsIterator = this.results.iterator();
    }

    public ListIterator(Collection<T> origin) {
        this.originIterator = origin.iterator();
        this.results = origin instanceof List ?
                       Collections.unmodifiableList((List<T>) origin) :
                       Collections.unmodifiableCollection(origin);
        this.resultsIterator = this.results.iterator();
    }

    @Override
    public void remove() {
        this.resultsIterator.remove();
    }

    public Collection<T> list() {
        return this.results;
    }

    @Override
    protected boolean fetch() {
        assert this.current == none();
        if (!this.resultsIterator.hasNext()) {
            return false;
        }
        this.current = this.resultsIterator.next();
        return true;
    }

    @Override
    protected Iterator<T> originIterator() {
        return this.originIterator;
    }
}
