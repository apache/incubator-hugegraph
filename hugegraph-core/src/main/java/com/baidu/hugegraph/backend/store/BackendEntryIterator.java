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
import java.util.function.Function;

import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumn;
import com.baidu.hugegraph.util.E;

public class BackendEntryIterator implements Iterator<BackendEntry> {

    private final Iterator<BackendColumn> columns;
    private final Query query;

    private Function<BackendColumn, BackendEntry> entryCreater;
    private BackendEntry current;
    private BackendEntry next;

    private long count;

    public BackendEntryIterator(Iterator<BackendColumn> columns, Query query,
                                Function<BackendColumn, BackendEntry> entry) {
        E.checkNotNull(columns, "columns");
        E.checkNotNull(entry, "entry");

        this.columns = columns;
        this.entryCreater = entry;
        this.query = query;
        this.count = 0L;
        this.current = null;
        this.next = null;
    }

    @Override
    public boolean hasNext() {
        if (this.reachLimit()) {
            return false;
        }

        if (this.current != null) {
            return true;
        }

        return this.fetch();
    }

    @Override
    public BackendEntry next() {
        if (this.reachLimit()) {
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
        this.count++;
        return current;
    }

    protected final boolean reachLimit() {
        /*
         * TODO: if the query is separated with multi sub-queries(like query
         * id in [id1, id2, ...]), then each BackendEntryIterator is only
         * result(s) of one sub-query, so the query offset/limit is inaccurate.
         */

        // Skip offset
        while (this.count < this.query.offset() && this.fetch()) {
            assert this.current != null;
            this.current = null;
            this.count++;
        }

        // Stop if reach limit
        if (this.query.limit() != Query.NO_LIMIT &&
            this.count >= this.query.limit()) {
            return true;
        }
        return false;
    }

    protected final boolean fetch() {
        assert this.current == null;
        if (this.next != null) {
            this.current = this.next;
            this.next = null;
        }

        while (this.columns.hasNext()) {
            BackendColumn col = this.columns.next();
            if (this.current == null) {
                // The first time to read
                this.current = this.entryCreater.apply(col);
                assert this.current != null;
                this.current.columns(col);
            } else if (this.current.belongToMe(col)) {
                // Does the column belongs to the current entry
                this.current.columns(col);
            } else {
                // New entry
                assert this.next == null;
                this.next = this.entryCreater.apply(col);
                assert this.next != null;
                this.next.columns(col);
                return true;
            }
        }
        return this.current != null;
    }
}
