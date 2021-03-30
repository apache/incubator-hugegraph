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

package com.baidu.hugegraph.backend.store.cassandra;

import java.util.Iterator;
import java.util.function.BiFunction;

import com.baidu.hugegraph.backend.page.PageState;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntryIterator;
import com.baidu.hugegraph.util.E;
import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class CassandraEntryIterator extends BackendEntryIterator {

    private final ResultSet results;
    private final Iterator<Row> rows;
    private final BiFunction<BackendEntry, Row, BackendEntry> merger;

    private long remaining;
    private BackendEntry next;

    public CassandraEntryIterator(ResultSet results, Query query,
           BiFunction<BackendEntry, Row, BackendEntry> merger) {
        super(query);
        this.results = results;
        this.rows = results.iterator();
        this.remaining = results.getAvailableWithoutFetching();
        this.merger = merger;
        this.next = null;

        this.skipOffset();

        if (query.paging()) {
            assert query.offset() == 0L;
            assert query.limit() >= 0L || query.noLimit() : query.limit();
            // Check the number of available rows
            if (results.isFullyFetched()) {
                // All results fetched (maybe not enough for the entire page)
                E.checkState(this.remaining <= query.limit(),
                             "Unexpected fetched page size: %s",
                             this.remaining);
            } else {
                // Not fetched the entire page (ScyllaDB may go here #1340)
                this.remaining = query.limit();
            }
        }
    }

    @Override
    public void close() throws Exception {
        // pass
    }

    @Override
    protected final boolean fetch() {
        assert this.current == null;
        if (this.next != null) {
            this.current = this.next;
            this.next = null;
        }

        while (this.remaining > 0 && this.rows.hasNext()) {
            if (this.query.paging()) {
                // Limit page size(due to rows.hasNext() will fetch next page)
                this.remaining--;
            }
            Row row = this.rows.next();
            BackendEntry merged = this.merger.apply(this.current, row);
            if (this.current == null) {
                // The first time to read
                this.current = merged;
            } else if (merged == this.current) {
                // The next entry belongs to the current entry
                assert merged != null;
            } else {
                // New entry
                assert this.next == null;
                this.next = merged;
                break;
            }
        }
        return this.current != null;
    }

    @Override
    protected final long sizeOf(BackendEntry entry) {
        CassandraBackendEntry e = (CassandraBackendEntry) entry;
        int subRowsSize = e.subRows().size();
        return subRowsSize > 0 ? subRowsSize : 1L;
    }

    @Override
    protected final long skip(BackendEntry entry, long skip) {
        CassandraBackendEntry e = (CassandraBackendEntry) entry;
        E.checkState(e.subRows().size() > skip, "Invalid entry to skip");
        for (long i = 0; i < skip; i++) {
            e.subRows().remove(0);
        }
        return e.subRows().size();
    }

    @Override
    protected PageState pageState() {
        PagingState page = this.results.getExecutionInfo().getPagingState();
        if (page == null || this.results.isExhausted()) {
            return new PageState(PageState.EMPTY_BYTES, 0, (int) this.count());
        }
        byte[] position = page.toBytes();
        return new PageState(position, 0, (int) this.count());
    }
}
