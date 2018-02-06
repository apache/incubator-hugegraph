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

package com.baidu.hugegraph.backend.store;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.iterator.Metadatable;

public abstract class BackendEntryIterator<T>
                implements Iterator<BackendEntry>, AutoCloseable, Metadatable {

    protected final Query query;

    protected BackendEntry current;

    private long count;

    public BackendEntryIterator(Query query) {
        this.query = query;
        this.count = 0L;
        this.current = null;
    }

    @Override
    public boolean hasNext() {
        if (this.exceedLimit()) {
            return false;
        }

        if (this.current != null) {
            return true;
        }

        return this.fetch();
    }

    @Override
    public BackendEntry next() {
        // Stop if reach capacity
        this.checkCapacity();

        // Stop if reach limit
        if (this.exceedLimit()) {
            throw new NoSuchElementException();
        }

        if (this.current == null) {
            this.fetch();
        }

        BackendEntry current = this.current;
        if (current == null) {
            throw new NoSuchElementException();
        }

        this.current = null;
        this.count += this.sizeOf(current);
        return current;
    }

    @Override
    public Object metadata(String meta, Object... args) {
        if ("page".equals(meta)) {
            return this.pageState();
        }
        throw new NotSupportException("Invalid meta '%s'", meta);
    }

    protected final void checkCapacity() {
        // Stop if reach capacity
        if (this.query.capacity() != Query.NO_CAPACITY &&
            this.count > this.query.capacity()) {
            throw new BackendException(
                      "Too many records(must <=%s) for a query",
                      this.query.capacity());
        }
    }

    protected final boolean exceedLimit() {
        /*
         * TODO: if the query is separated with multi sub-queries(like query
         * id in [id1, id2, ...]), then each BackendEntryIterator is only
         * result(s) of one sub-query, so the query offset/limit is inaccurate.
         */

        // Stop if reach limit
        if (this.query.limit() != Query.NO_LIMIT &&
            this.count >= (this.offset() + this.query.limit())) {
            return true;
        }

        return false;
    }

    protected void skipOffset() {
        long offset = this.offset();

        // Skip offset
        while (this.count < offset && this.fetch()) {
            assert this.current != null;
            final long size = this.sizeOf(this.current);
            this.count += size;
            if (this.count > offset) {
                // Skip part of sub-items in an entry
                final long skip = size - (this.count - offset);
                this.count -= this.skip(this.current, skip);
                assert this.count == offset;
            } else {
                // Skip entry
                this.current = null;
            }
        }
    }

    protected long offset() {
        return this.query.offset();
    }

    protected long sizeOf(BackendEntry entry) {
        return 1;
    }

    protected long skip(BackendEntry entry, long skip) {
        assert this.sizeOf(entry) == 1;
        // Return the remained sub-items(items)
        return this.sizeOf(entry);
    }

    protected abstract boolean fetch();

    protected abstract String pageState();
}
