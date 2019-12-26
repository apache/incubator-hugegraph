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
import com.baidu.hugegraph.iterator.CIter;
import com.baidu.hugegraph.iterator.Metadatable;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.LockUtil.Locks;

public abstract class IdHolder {

    protected final Query query;
    protected boolean exhausted;

    public IdHolder(Query query) {
        E.checkNotNull(query, "query");;
        this.query = query;
        this.exhausted = false;
    }

    public Query query() {
        return this.query;
    }

    @Override
    public String toString() {
        return String.format("%s{%s}",
                             this.getClass().getSimpleName(), this.query);
    }

    public abstract boolean paging();

    public abstract Set<Id> all();

    public abstract PageIds fetchNext(String page, long pageSize);

    public static class FixedIdHolder extends IdHolder {

        private final Set<Id> ids;

        public FixedIdHolder(Query query, Set<Id> ids) {
            super(query);
            this.ids = ids;
        }

        @Override
        public boolean paging() {
            return false;
        }

        @Override
        public Set<Id> all() {
            return this.ids;
        }

        @Override
        public PageIds fetchNext(String page, long pageSize) {
            throw new NotImplementedException("FixedIdHolder.fetchNext");
        }
    }

    public static class PagingIdHolder extends IdHolder {

        private final Function<ConditionQuery, PageIds> fetcher;

        /**
         * For paging situation
         * @param query         original query
         * @param fetcher    function to fetch one page ids
         */
        public PagingIdHolder(ConditionQuery query,
                              Function<ConditionQuery, PageIds> fetcher) {
            super(query.copy());
            E.checkArgument(query.paging(),
                            "Query '%s' must include page info", query);
            this.fetcher = fetcher;
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

            PageIds result = this.fetcher.apply((ConditionQuery) this.query);
            assert result != null;
            if (result.ids().size() < pageSize || result.page() == null) {
                this.exhausted = true;
            }
            return result;
        }

        @Override
        public Set<Id> all() {
            throw new NotImplementedException("PagingIdHolder.all");
        }
    }

    public static class BatchIdHolder extends IdHolder
                                      implements CIter<IdHolder> {

        private final Locks locks;
        private final Iterator<BackendEntry> entries;
        private final Function<Long, Set<Id>> fetcher;
        private long count;

        public BatchIdHolder(Locks locks, ConditionQuery query,
                             Iterator<BackendEntry> entries,
                             Function<Long, Set<Id>> fetcher) {
            super(query);
            this.locks = locks;
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
            if (this.exhausted) {
                return false;
            }
            boolean hasNext= this.entries.hasNext();
            if (!hasNext) {
                this.close();
            }
            return hasNext;
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

            if (this.query.limit() != Query.NO_LIMIT) {
                long remaining = this.remaining();
                if (remaining < batchSize) {
                    batchSize = remaining;
                }
            }
            Set<Id> ids = this.fetcher.apply(batchSize);
            this.count += ids.size();
            if (ids.size() < batchSize) {
                this.close();
            }

            // If there is no data, the entries is not a Metadatable object
            if (ids.isEmpty()) {
                return PageIds.EMPTY;
            } else {
                return new PageIds(ids, PageState.EMPTY);
            }
        }

        @Override
        public Set<Id> all() {
            Set<Id> ids = this.fetcher.apply(this.remaining());
            this.close();
            return ids;
        }

        private long remaining() {
            if (this.query.limit() == Query.NO_LIMIT) {
                return Query.NO_LIMIT;
            } else {
                return this.query.limit() - this.count;
            }
        }

        @Override
        public void close() {
            if (this.exhausted) {
                return;
            }
            this.exhausted = true;
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
