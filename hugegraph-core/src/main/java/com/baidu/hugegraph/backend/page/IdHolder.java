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

package com.baidu.hugegraph.backend.page;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.iterator.Metadatable;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.LockUtil.Locks;

public interface IdHolder {

    public boolean paging();

    public Set<Id> all();

    public PageIds fetchNext(String page, long pageSize);

    public static class PagingIdHolder implements IdHolder {

        private final ConditionQuery query;
        private final Function<ConditionQuery, PageIds> fetcher;
        private boolean exhausted;

        /**
         * For paging situation
         * @param query         original query
         * @param fetcher    function to fetch one page ids
         */
        public PagingIdHolder(ConditionQuery query,
                              Function<ConditionQuery, PageIds> fetcher) {
            E.checkArgument(query.paging(),
                            "Query '%s' must include page info", query);
            this.query = query.copy();
            this.fetcher = fetcher;
            this.exhausted = false;
        }

        @Override
        public boolean paging() {
            return true;
        }

        @Override
        public PageIds fetchNext(String page, long pageSize) {
            if (this.exhausted) {
                return PageIds.EMPTY;
            }

            this.query.page(page);
            this.query.limit(pageSize);

            PageIds result = this.fetcher.apply(this.query);
            assert result != null;
            if (result.ids().size() != this.query.limit() || result.page() == null) {
                this.exhausted = true;
            }
            return result;
        }

        @Override
        public Set<Id> all() {
            throw new NotImplementedException("PagingIdHolder.all");
        }
    }

    public static class FixedIdHolder implements IdHolder {

        // Used by Joint Index
        private final Set<Id> ids;

        public FixedIdHolder(Set<Id> ids) {
            this.ids = ids;
        }

        @Override
        public boolean paging() {
            return false;
        }

        @Override
        public Set<Id> all() {
            return ids;
        }

        @Override
        public PageIds fetchNext(String page, long pageSize) {
            throw new NotImplementedException("FixedIdHolder.fetchNext");
        }
    }

    public static class BatchIdHolder implements IdHolder,
                                                 Iterator<IdHolder>,
                                                 Metadatable,
                                                 AutoCloseable {

        private final Locks locks;
        private final ConditionQuery query;
        private final Iterator<BackendEntry> entries;
        private final Function<Long, Set<Id>> fetcher;
        private long count;

        public BatchIdHolder(Locks locks, ConditionQuery query,
                             Iterator<BackendEntry> entries,
                             Function<Long, Set<Id>> fetcher) {
            this.locks = locks;
            this.query = query;
            this.entries = entries;
            this.fetcher = fetcher;
            this.count = 0L;
        }

        @Override
        public boolean paging() {
            return false;
        }

        @Override
        public boolean hasNext() {
            return this.entries.hasNext();
        }

        @Override
        public IdHolder next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return this;
        }

        @Override
        public PageIds fetchNext(String page, long batchSize) {
            E.checkArgument(page == null,
                            "Not support page parameter by BatchIdHolder");
            E.checkArgument(batchSize > 0L,
                            "Invalid batch size value: %s", batchSize);

            if (!this.query.nolimit()) {
                long remaining = this.remaining();
                if (remaining < batchSize) {
                    batchSize = remaining;
                }
            }
            Set<Id> ids = this.fetcher.apply(batchSize);
            this.count += ids.size();

            // If there is no data, the entries is not a Metadatable object
            if (ids.isEmpty()) {
                return PageIds.EMPTY;
            } else {
                return new PageIds(ids, PageState.EMPTY);
            }
        }

        @Override
        public Set<Id> all() {
            return this.fetcher.apply(this.remaining());
        }

        private long remaining() {
            if (this.query.nolimit()) {
                return Query.NO_LIMIT;
            } else {
                return this.query.total() - this.count;
            }
        }

        @Override
        public void close() {
            try {
                CloseableIterator.closeIterator(this.entries);
            } finally {
                this.locks.unlock();
            }
        }

        @Override
        public Object metadata(String meta, Object... args) {
            E.checkState(this.entries instanceof Metadatable,
                         "Invalid iterator for Metadatable: %s",
                         this.entries.getClass());
            return ((Metadatable) this.entries).metadata(meta, args);
        }
    }
}
