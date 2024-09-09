/*
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

package org.apache.hugegraph.iterator;

import java.util.Iterator;
import java.util.function.Function;

import org.apache.hugegraph.util.E;

public class FlatMapperIterator<T, R> extends WrappedIterator<R> {

    private final Iterator<T> originIterator;
    private final Function<T, Iterator<R>> mapperCallback;

    protected Iterator<R> batchIterator;

    public FlatMapperIterator(Iterator<T> origin,
                              Function<T, Iterator<R>> mapper) {
        this.originIterator = origin;
        this.mapperCallback = mapper;
        this.batchIterator = null;
    }

    @Override
    public void close() throws Exception {
        this.resetBatchIterator();
        super.close();
    }

    @Override
    protected Iterator<T> originIterator() {
        return this.originIterator;
    }

    @Override
    protected final boolean fetch() {
        if (this.batchIterator != null && this.fetchFromBatch()) {
            return true;
        }

        while (this.originIterator.hasNext()) {
            T next = this.originIterator.next();
            assert this.batchIterator == null;
            // Do fetch
            this.batchIterator = this.mapperCallback.apply(next);
            if (this.batchIterator != null && this.fetchFromBatch()) {
                return true;
            }
        }
        return false;
    }

    protected boolean fetchFromBatch() {
        E.checkNotNull(this.batchIterator, "mapper results");
        while (this.batchIterator.hasNext()) {
            R result = this.batchIterator.next();
            if (result != null) {
                assert this.current == none();
                this.current = result;
                return true;
            }
        }
        this.resetBatchIterator();
        return false;
    }

    protected final void resetBatchIterator() {
        if (this.batchIterator == null) {
            return;
        }
        close(this.batchIterator);
        this.batchIterator = null;
    }
}
