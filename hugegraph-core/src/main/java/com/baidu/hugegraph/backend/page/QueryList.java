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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.iterator.FlatMapperIterator;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableSet;

public final class QueryList {

    private final HugeGraph graph;
    private final Query parent;
    // The size of each page fetched by the inner page
    private final Function<Query, Iterator<BackendEntry>> fetcher;
    private final List<QueryHolder> queries;

    public QueryList(HugeGraph graph, Query parent,
                     Function<Query, Iterator<BackendEntry>> fetcher) {
        this.graph = graph;
        this.parent = parent;
        this.fetcher = fetcher;
        this.queries = new ArrayList<>();
    }

    protected Query parent() {
        return this.parent;
    }

    protected Function<Query, Iterator<BackendEntry>> fetcher() {
        return this.fetcher;
    }

    public void add(List<IdHolder> holders) {
        if (!this.parent.paging()) {
            for (QueryHolder q : this.queries) {
                if (q instanceof IndexQuery) {
                    ((IndexQuery) q).holders.addAll(holders);
                    return;
                }
            }
        }
        this.queries.add(new IndexQuery(holders));
    }

    public void add(Query query) {
        // TODO: maybe need do deduplicate(for -> flatten)
        this.queries.add(new OptimizedQuery(query));
    }

    public int total() {
        int total = 0;
        for (QueryHolder q : this.queries) {
            total += q.total();
        }
        return total;
    }

    public boolean empty() {
        return this.queries.isEmpty();
    }

    public Iterator<BackendEntry> fetch() {
        assert !this.queries.isEmpty();
        if (this.parent.paging()) {
            int pageSize = this.graph.configuration()
                                     .get(CoreOptions.QUERY_PAGE_SIZE);
            return new PageEntryIterator(this, pageSize);
        } else {
            return this.fetchAll();
        }
    }

    protected Iterator<BackendEntry> fetchAll() {
        return new FlatMapperIterator<>(this.queries.iterator(), q -> {
            return q.iterator();
        });
    }

    protected PageIterator fetchNext(PageInfo pageInfo, long pageSize) {
        QueryHolder query = null;
        int offset = pageInfo.offset();
        int current = 0;
        for (QueryHolder q : this.queries) {
            if (current + q.total() > offset) {
                query = q;
                break;
            }
            current += q.total();
        }
        E.checkNotNull(query, "query");
        assert offset >= current;
        return query.iterator(offset - current, pageInfo.page(), pageSize);
    }

    @SuppressWarnings("unused")
    private static Set<Id> limit(Set<Id> ids, Query query) {
        long fromIndex = query.offset();
        E.checkArgument(fromIndex <= Integer.MAX_VALUE,
                        "Offset must be <= 0x7fffffff, but got '%s'",
                        fromIndex);

        if (query.offset() >= ids.size()) {
            return ImmutableSet.of();
        }
        if (query.limit() == Query.NO_LIMIT && query.offset() == 0) {
            return ids;
        }
        long toIndex = query.offset() + query.limit();
        if (query.limit() == Query.NO_LIMIT || toIndex > ids.size()) {
            toIndex = ids.size();
        }
        assert fromIndex < ids.size();
        assert toIndex <= ids.size();
        return CollectionUtil.subSet(ids, (int) fromIndex, (int) toIndex);
    }

    /**
     * A container that can generate queries
     */
    private interface QueryHolder {

        /**
         * For non-paging situation
         * @return          BackendEntry iterator
         */
        public Iterator<BackendEntry> iterator();

        /**
         * For paging situation
         * @param index     position IdHolder(Query)
         * @param page      set query page
         * @param pageSize  set query page size
         * @return          BackendEntry iterator with page
         */
        public PageIterator iterator(int index, String page, long pageSize);

        public int total();
    }

    /**
     * Generate queries from tx.optimizeQuery()
     */
    private class OptimizedQuery implements QueryHolder {

        private final Query query;

        public OptimizedQuery(Query query) {
            this.query = query;
        }

        @Override
        public Iterator<BackendEntry> iterator() {
            return fetcher().apply(this.query);
        }

        @Override
        public PageIterator iterator(int index, String page, long pageSize) {
            assert index == 0;
            Query query = this.query.copy();
            query.page(page);
            // Not set limit to pageSize due to PageEntryIterator.remaining
            if (this.query.limit() == Query.NO_LIMIT) {
                query.limit(pageSize);
            }
            Iterator<BackendEntry> iterator = fetcher().apply(query);
            // Must iterate all entries before get the next page
            List<BackendEntry> results = IteratorUtils.list(iterator);
            return new PageIterator(results.iterator(),
                                    PageInfo.page(iterator));
        }

        @Override
        public int total() {
            return 1;
        }
    }

    /**
     * Generate queries from tx.indexQuery()
     */
    private class IndexQuery implements QueryHolder {

        // Actual is an instance of IdHolderList
        private final List<IdHolder> holders;

        public IndexQuery(List<IdHolder> holders) {
            this.holders = holders;
        }

        @Override
        public Iterator<BackendEntry> iterator() {
            return new FlatMapperIterator<>(this.holders.iterator(), holder -> {
                if (holder.ids().isEmpty()) {
                    return null;
                }
                Set<Id> ids = holder.ids();
                if (parent().limit() != Query.NO_LIMIT &&
                    ids.size() > parent().limit()) {
                    /*
                     * Avoid too many ids in one time query,
                     * Assume it will get one result by each id
                     */
                    ids = CollectionUtil.subSet(ids, 0, (int) parent().limit());
                }
                IdQuery query = new IdQuery(parent(), ids);
                return fetcher().apply(query);
            });
        }

        @Override
        public PageIterator iterator(int index, String page, long pageSize) {
            IdHolder holder = this.holders.get(index);
            PageIds pageIds = holder.fetchNext(page, pageSize);
            if (pageIds.empty()) {
                return PageIterator.EMPTY;
            }
            IdQuery query = new IdQuery(parent(), pageIds.ids());
            return new PageIterator(fetcher().apply(query), pageIds.page());
        }

        @Override
        public int total() {
            return this.holders.size();
        }
    }

    public static class PageIterator {

        public static final PageIterator EMPTY = new PageIterator(
                                                 Collections.emptyIterator(),
                                                 PageInfo.PAGE_NONE);

        private final Iterator<BackendEntry> iterator;
        private final String page;

        public PageIterator(Iterator<BackendEntry> iterator, String page) {
            this.iterator = iterator;
            this.page = page;
        }

        public Iterator<BackendEntry> iterator() {
            return this.iterator;
        }

        public String page() {
            return this.page;
        }

        public long total() {
            return PageState.fromString(this.page).total();
        }
    }
}
