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

import java.util.Iterator;
import java.util.function.Function;

import com.baidu.hugegraph.util.E;

public class FlatMapperIterator<T, R> extends WrappedIterator<R> {

    private final Iterator<T> originIterator;
    private final Function<T, Iterator<R>> mapperCallback;

    protected Iterator<R> results;

    public FlatMapperIterator(Iterator<T> origin,
                              Function<T, Iterator<R>> mapper) {
        this.originIterator = origin;
        this.mapperCallback = mapper;
        this.results = null;
    }

    @Override
    protected Iterator<?> originIterator() {
        return this.originIterator;
    }

    @Override
    protected final boolean fetch() {
        if (this.results != null && this.fetchMapped()) {
            return true;
        }

        while (this.originIterator.hasNext()) {
            T next = this.originIterator.next();
            assert this.results == null;
            this.results = this.mapperCallback.apply(next);
            if (this.results != null && this.fetchMapped()) {
                return true;
            }
        }
        return false;
    }

    protected boolean fetchMapped() {
        E.checkNotNull(this.results, "mapper results");
        while (this.results.hasNext()) {
            R result = this.results.next();
            if (result != null) {
                assert this.current == none();
                this.current = result;
                return true;
            }
        }
        this.results = null;
        return false;
    }
}
