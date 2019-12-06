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

package com.baidu.hugegraph.backend.query;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;

import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.iterator.FlatMapperIterator;
import com.baidu.hugegraph.iterator.MapperIterator;
import com.baidu.hugegraph.iterator.Metadatable;
import com.baidu.hugegraph.type.Idfiable;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;

public class QueryResults {

    private static final Iterator<?> EMPTY_ITERATOR = new EmptyIterator<>();

    private static final QueryResults EMPTY = new QueryResults(emptyIterator(),
                                                               Query.NONE);

    private final Iterator<BackendEntry> results;
    private final List<Query> queries;

    public QueryResults(Iterator<BackendEntry> results, Query query) {
        this(results);
        this.addQuery(query);
    }

    public QueryResults(Iterator<BackendEntry> results) {
        this.results = results;
        this.queries = InsertionOrderUtil.newList();
    }

    public void setQuery(Query query) {
        if (this.queries.size() > 0) {
            this.queries.clear();
        }
        this.addQuery(query);
    }

    private void addQuery(Query query) {
        E.checkNotNull(query, "query");
        this.queries.add(query);
    }

    private void addQueries(List<Query> queries) {
        for (Query query : queries) {
            this.addQuery(query);
        }
    }

    public Iterator<BackendEntry> iterator() {
        return this.results;
    }

    public List<BackendEntry> list() {
        return IteratorUtils.list(this.results);
    }

    public List<Query> queries() {
        return Collections.unmodifiableList(this.queries);
    }

    public <T extends Idfiable> Iterator<T> keepInputOrderIfNeeded(
                                            Iterator<T> origin) {
        if (!origin.hasNext()) {
            // None result found
            return origin;
        }
        Set<Id> ids;
        if (this.paging() || !this.mustSortByInputIds() ||
            (ids = this.queryIds()).size() <= 1) {
            /*
             * Return the original iterator if it's paging query or if the
             * query input is less than one id, or don't have to do sort.
             */
            return origin;
        }

        // Fill map with all elements
        Map<Id, T> results = new HashMap<>();
        fillMap(origin, results);

        return new MapperIterator<>(ids.iterator(), id -> {
            return results.get(id);
        });
    }

    private boolean mustSortByInputIds() {
        if (this.queries.size() == 1) {
            Query query = this.queries.get(0);
            if (query instanceof IdQuery) {
                return ((IdQuery) query).mustSortByInput();
            }
        }
        return true;
    }

    private boolean paging() {
        for (Query query : this.queries) {
            Query origin = query.originQuery();
            if (query.paging() || origin != null && origin.paging()) {
                return true;
            }
        }
        return false;
    }

    private Set<Id> queryIds() {
        if (this.queries.size() == 1) {
            return this.queries.get(0).ids();
        }

        Set<Id> ids = InsertionOrderUtil.newSet();
        for (Query query : this.queries) {
            ids.addAll(query.ids());
        }
        return ids;
    }

    public static <T extends Idfiable> void fillMap(Iterator<T> iterator,
                                                    Map<Id, T> map) {
        while (iterator.hasNext()) {
            T result = iterator.next();
            assert result.id() != null;
            map.put(result.id(), result);
        }
    }

    public static <T> QueryResults flatMap(Iterator<T> iterator,
                                           Function<T, QueryResults> func) {
        QueryResults[] qr = new QueryResults[1];
        qr[0] = new QueryResults(new FlatMapperIterator<>(iterator, i -> {
            QueryResults results = func.apply(i);
            if (results == null) {
                return null;
            }
            qr[0].addQueries(results.queries());
            return results.iterator();
        }));
        return qr[0];
    }

    public static QueryResults empty() {
        return EMPTY;
    }

    @SuppressWarnings("unchecked")
    public static <T> Iterator<T> emptyIterator() {
        return (Iterator<T>) EMPTY_ITERATOR;
    }

    private static class EmptyIterator<T> implements Iterator<T>, Metadatable {

        @Override
        public Object metadata(String meta, Object... args) {
            return null;
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public T next() {
            throw new NoSuchElementException();
        }
    }
}
