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

public class FlatMapperFilterIterator<T, R> extends FlatMapperIterator<T, R> {

    private final Function<R, Boolean> filterCallback;

    public FlatMapperFilterIterator(Iterator<T> origin,
                                    Function<T, Iterator<R>> mapper,
                                    Function<R, Boolean> filter) {
        super(origin, mapper);
        this.filterCallback = filter;
    }

    @Override
    protected final boolean fetchFromBatch() {
        E.checkNotNull(this.batchIterator, "mapper results");
        while (this.batchIterator.hasNext()) {
            R result = this.batchIterator.next();
            if (result != null && this.filterCallback.apply(result)) {
                assert this.current == none();
                this.current = result;
                return true;
            }
        }
        this.resetBatchIterator();
        return false;
    }
}
