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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.page.IdHolder.BatchIdHolder;
import com.baidu.hugegraph.backend.page.IdHolder.FixedIdHolder;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;

public final class QueryList {

    private final HugeGraph graph;
    private final Query parent;
    // The size of each page fetched by the inner page
    private final Function<Query, QueryResults> fetcher;
    private final List<FlattenQuery> queries;

    public QueryList(HugeGraph graph, Query parent,
                     Function<Query, QueryResults> fetcher) {
        this.graph = graph;
        this.parent = parent;
        this.fetcher = fetcher;
        this.queries = new ArrayList<>();
    }

    protected Query parent() {
        return this.parent;
    }

    protected Function<Query, QueryResults> fetcher() {
        return this.fetcher;
    }

    public void add(IdHolderList holders, long batchSize) {
        // IdHolderList is results of one index query, the query is flattened
//        if (!this.parent.paging()) {
//            for (QueryHolder q : this.queries) {
//                if (q instanceof IndexQuery) {
//                    ((IndexQuery) q).merge(holders);
//                    return;
//                }
//            }
//        }
        this.queries.add(new IndexQuery(holders, batchSize));
    }

    public void add(Query query) {
        // TODO: maybe need do deduplicate(for -> flatten)
        this.queries.add(new OptimizedQuery(query));
    }

    public int total() {
        int total = 0;
        for (FlattenQuery q : this.queries) {
            total += q.total();
        }
        return total;
    }

    public boolean empty() {
        return this.queries.isEmpty();
    }

    public QueryResults fetch() {
        assert !this.queries.isEmpty();
        if (this.parent.paging()) {
            int pageSize = this.graph.configuration()
                                     .get(CoreOptions.QUERY_PAGE_SIZE);
            @SuppressWarnings("resource") // closed by QueryResults
            PageEntryIterator iterator = new PageEntryIterator(this, pageSize);
            /*
             * NOTE: PageEntryIterator query will change every fetch time.
             * TODO: sort results by input ids in each page.
             */
            return iterator.results();
        }

        // Fetch all results once
        return QueryResults.flatMap(this.queries.iterator(), q -> q.iterator());
    }

    protected PageResults fetchNext(PageInfo pageInfo, long pageSize) {
        FlattenQuery query = null;
        int offset = pageInfo.offset();
        int visited = 0;
        // Find the first FlattenQuery not visited
        for (FlattenQuery q : this.queries) {
            if (visited + q.total() > offset) {
                /*
                 * The first FlattenQuery not visited is found
                 * q.total() return holders size if it's IndexQuery else 1
                 */
                query = q;
                break;
            }
            visited += q.total();
        }
        E.checkNotNull(query, "query");
        assert offset >= visited;
        return query.iterator(offset - visited, pageInfo.page(), pageSize);
    }

    /**
     * A container that can generate queries
     */
    private interface FlattenQuery {

        /**
         * For non-paging situation
         * @return          BackendEntry iterator
         */
        public QueryResults iterator();

        /**
         * For paging situation
         * @param index     position IdHolder(Query)
         * @param page      set query page
         * @param pageSize  set query page size
         * @return          BackendEntry iterator with page
         */
        public PageResults iterator(int index, String page, long pageSize);

        public int total();
    }

    /**
     * Generate queries from tx.optimizeQuery()
     */
    private class OptimizedQuery implements FlattenQuery {

        private final Query query;

        public OptimizedQuery(Query query) {
            this.query = query;
        }

        @Override
        public QueryResults iterator() {
            // Iterate all
            return fetcher().apply(this.query);
        }

        @Override
        public PageResults iterator(int index, String page, long pageSize) {
            // Iterate by paging
            assert index == 0;
            Query query = this.query.copy();
            query.page(page);
            // Not set limit to pageSize due to PageEntryIterator.remaining
            if (this.query.nolimit()) {
                query.limit(pageSize);
            }

            QueryResults results = fetcher().apply(query);

            // Must iterate all entries before get the next page state
            QueryResults fetched = results.toList();
            PageState pageState = PageInfo.pageState(results.iterator());

            return new PageResults(fetched, pageState);
        }

        @Override
        public int total() {
            return 1;
        }
    }

    /**
     * Generate queries from tx.indexQuery()
     */
    private class IndexQuery implements FlattenQuery {

        // One IdHolder each sub-query
        private final IdHolderList holders;
        // Fetching ids size each time, default 100
        private final long batchSize;

        public IndexQuery(IdHolderList holders, long batchSize) {
            this.holders = holders;
            this.batchSize = batchSize;
        }

        @Override
        public QueryResults iterator() {
            // Iterate all
            if (this.holders.size() == 1) {
                return this.each(this.holders.get(0));
            }
            return QueryResults.flatMap(this.holders.iterator(), this::each);
        }

        private QueryResults each(IdHolder holder) {
            Query parent = parent();
            assert !holder.paging();

            // Iterate by all
            if (holder instanceof FixedIdHolder) {
                Set<Id> ids = holder.all();
                ids = parent.skipOffset(ids);
                if (ids.isEmpty()) {
                    return null;
                }

                IdQuery query = new IdQuery(parent, ids);
                return fetcher().apply(query);
            }

            // Iterate by batch
            assert holder instanceof BatchIdHolder;
            return QueryResults.flatMap((BatchIdHolder) holder, h -> {
                long remaining = parent.nolimit() ? Query.NO_LIMIT :
                                 parent.remaining();
                if (remaining > this.batchSize || remaining == Query.NO_LIMIT) {
                    /*
                     * Avoid too many ids in one time query,
                     * Assume it will get one result by each id
                     */
                    remaining = this.batchSize;
                }
                Set<Id> ids = h.fetchNext(null, remaining).ids();
                ids = parent.skipOffset(ids);
                if (ids.isEmpty()) {
                    return null;
                }

                IdQuery query = new IdQuery(parent, ids);
                return fetcher().apply(query);
            });
        }

        @Override
        public PageResults iterator(int index, String page, long pageSize) {
            // Iterate by paging
            E.checkArgument(0 <= index && index <= this.holders.size(),
                            "Invalid page index %s", index);
            IdHolder holder = this.holders.get(index);
            PageIds pageIds = holder.fetchNext(page, pageSize);
            if (pageIds.empty()) {
                return PageResults.EMPTY;
            }

            IdQuery query = new IdQuery(parent(), pageIds.ids());
            QueryResults results = fetcher().apply(query);

            return new PageResults(results, pageIds.pageState());
        }

        @Override
        public int total() {
            return this.holders.size();
        }
    }

    public static class PageResults {

        public static final PageResults EMPTY = new PageResults(
                                                QueryResults.empty(),
                                                PageState.EMPTY);

        private final QueryResults results;
        private final PageState pageState;

        public PageResults(QueryResults results, PageState pageState) {
            this.results = results;
            this.pageState = pageState;
        }

        public Iterator<BackendEntry> get() {
            return this.results.iterator();
        }

        public boolean hasNextPage() {
            return !Bytes.equals(this.pageState.position(),
                                 PageState.EMPTY_BYTES);
        }

        public Query query() {
            List<Query> queries = this.results.queries();
            E.checkState(queries.size() == 1,
                         "Expect queries size 1, but got: %s", queries);
            return queries.get(0);
        }

        public String page() {
            return this.pageState.toString();
        }

        public long total() {
            return this.pageState.total();
        }
    }
}
