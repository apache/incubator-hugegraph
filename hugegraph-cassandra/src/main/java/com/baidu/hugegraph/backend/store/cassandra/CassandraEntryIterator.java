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
import java.util.List;
import java.util.function.BiFunction;

import com.baidu.hugegraph.backend.page.PageState;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntryIterator;
import com.baidu.hugegraph.util.E;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class CassandraEntryIterator extends BackendEntryIterator {

    private final ResultSet results;
    private final Iterator<Row> rows;
    private final BiFunction<BackendEntry, Row, BackendEntry> merger;

    private int fetchdPageSize;
    private long expected;
    private BackendEntry next;

    public CassandraEntryIterator(ResultSet results, Query query,
           BiFunction<BackendEntry, Row, BackendEntry> merger) {
        super(query);
        this.results = results;
        this.rows = results.iterator();
        this.merger = merger;

        this.fetchdPageSize = results.getAvailableWithoutFetching();
        this.next = null;

        if (query.paging()) {
            assert query.offset() == 0L;
            assert query.limit() >= 0L || query.noLimit() : query.limit();
            // Skip page offset
            this.expected = PageState.fromString(query.page()).offset();
            this.skipPageOffset(query.page());
            // Check the number of available rows
            E.checkState(this.fetchdPageSize <= query.limit(),
                         "Unexpected fetched page size: %s",
                         this.fetchdPageSize);
            if (results.isFullyFetched()) {
                /*
                 * All results fetched
                 * NOTE: it may be enough or not enough for the entire page
                 */
                this.expected = this.fetchdPageSize;
            } else {
                /*
                 * Not fully fetched, that's fetchdPageSize == query.limit(),
                 *
                 * NOTE: but there may be fetchdPageSize < query.limit(), means
                 * not fetched the entire page (ScyllaDB may go here #1340),
                 * try to fetch next page later until got the expected count.
                 * Can simulate by: `select.setFetchSize(total - 1)`
                 */
                this.expected = query.total();
            }
        } else {
            this.expected = query.total();
            this.skipOffset();
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

        while (this.expected > 0L && this.rows.hasNext()) {
            // Limit expected count, due to rows.hasNext() will fetch next page
            this.expected--;
            Row row = this.rows.next();
            if (this.query.paging()) {
                // Update fetchdPageSize if auto fetch the next page
                if (this.expected > 0L && this.availableLocal() == 0) {
                    if (this.rows.hasNext()) {
                        this.fetchdPageSize = this.availableLocal();
                    }
                }
            }
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
        byte[] position;
        int offset = 0;
        int count = (int) this.count();
        assert this.fetched() == count;
        int extra = this.availableLocal();
        List<ExecutionInfo> infos = this.results.getAllExecutionInfo();
        if (extra > 0 && infos.size() >= 2) {
            /*
             * Go back to the previous page if there are still available
             * results fetched to local memory but not consumed, and set page
             * offset with consumed amount of results.
             *
             * Safely, we should get the remaining size of the current page by:
             *  `Whitebox.getInternalState(results, "currentPage").size()`
             * instead of
             *  `results.getAvailableWithoutFetching()`
             */
            ExecutionInfo previous = infos.get(infos.size() - 2);
            PagingState page = previous.getPagingState();
            position = page.toBytes();
            offset = this.fetchdPageSize - extra;
        } else {
            PagingState page = this.results.getExecutionInfo().getPagingState();
            if (page == null || this.expected > 0L) {
                // Call isExhausted() will lead to try to fetch the next page
                E.checkState(this.results.isExhausted(),
                             "Unexpected paging state with expected=%s, " +
                             "ensure consume all the fetched results before " +
                             "calling pageState()", this.expected);
                position = PageState.EMPTY_BYTES;
            } else {
                /*
                 * Exist page position which used to fetch the next page.
                 * Maybe it happens to the last page (that's the position is
                 * at the end of results and next page is empty)
                 */
                position = page.toBytes();
            }
        }

        return new PageState(position, offset, count);
    }

    private int availableLocal() {
        return this.results.getAvailableWithoutFetching();
    }
}
