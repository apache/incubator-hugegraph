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

package com.baidu.hugegraph.backend.tx;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.iterator.Metadatable;
import com.baidu.hugegraph.util.E;

public class PagedEntryIterator extends PagedIterator<BackendEntry> {

    private final List<Query> queries;
    // The size of each page fetched by the inner page
    private final Function<Query, Iterator<BackendEntry>> fetcher;
    private Iterator<BackendEntry> results;

    public PagedEntryIterator(List<Query> queries, String page, long limit,
                              Function<Query, Iterator<BackendEntry>> fetcher) {
        super(limit, 0);
        E.checkNotNull(page, "page");
        this.queries = queries;
        this.fetcher = fetcher;
        this.results = null;

        this.parsePageState(page);
    }

    private void parsePageState(String pageInfo) {
        if (pageInfo.isEmpty()) {
            for (Query query : this.queries) {
                query.resetPage();
            }
        } else {
            PageState pageState = PageState.fromString(pageInfo);
            this.offset = pageState.offset();
            E.checkState(this.offset < this.queries.size(),
                         "Invalid page '%s' with an offset '%s' exceeds " +
                         "the size of Queries", pageInfo, this.offset);
            for (int i = offset; i < this.queries.size(); i++) {
                Query query = this.queries.get(i);
                if (i == offset) {
                    query.page(pageState.page());
                } else {
                    query.resetPage();
                }
            }
        }
    }

    @Override
    public boolean hasNext() {
        if (this.results != null && this.results.hasNext()) {
            return true;
        }
        if (this.remaining <= 0 || this.offset >= this.queries.size()) {
            return false;
        }

        if (this.results != null) {
            ++this.offset;
        }
        this.fetch();
        return this.results.hasNext();
    }

    private void fetch() {
        if (this.remaining <= 0 || this.offset >= this.queries.size()) {
            return;
        }

        Query query = this.queries.get(this.offset);
        this.results = this.fetcher.apply(query);
        if (!this.results.hasNext()) {
            ++this.offset;
            this.fetch();
        }
    }

    @Override
    public BackendEntry next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }
        BackendEntry entry = this.results.next();
        this.remaining--;
        return entry;
    }

    @Override
    public Object metadata(String meta, Object... args) {
        if ("page".equals(meta)) {
            Object page = ((Metadatable) this.results).metadata(meta, args);
            if (page == null) {
                if (++this.offset < this.queries.size()) {
                    page = "";
                } else {
                    return null;
                }
            }
            PageState pageState = new PageState(this.offset, (String) page);
            return pageState.toString();
        }
        throw new NotSupportException("Invalid meta '%s'", meta);
    }
}
