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

import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.iterator.Metadatable;
import com.baidu.hugegraph.util.E;

public class PageEntryIterator implements Iterator<BackendEntry>, Metadatable {

    private final QueryList queries;
    private final long pageSize;
    private QueryList.PageIterator results;
    private PageState pageState;
    private long remaining;

    public PageEntryIterator(QueryList queries, long pageSize) {
        this.queries = queries;
        this.pageSize = pageSize;
        this.results = QueryList.PageIterator.EMPTY;
        this.pageState = this.parsePageState();
        this.remaining = queries.parent().limit();
    }

    private PageState parsePageState() {
        String page = this.queries.parent().pageWithoutCheck();
        PageState pageState = PageState.fromString(page);
        E.checkState(pageState.offset() < this.queries.total(),
                     "Invalid page '%s' with an offset '%s' exceeds " +
                     "the size of IdHolderList", page, pageState.offset());
        return pageState;
    }

    @Override
    public boolean hasNext() {
        if (this.results.iterator().hasNext()) {
            return true;
        }
        return this.fetch();
    }

    private boolean fetch() {
        if ((this.remaining != Query.NO_LIMIT && this.remaining <= 0L) ||
            this.pageState.offset() >= this.queries.total()) {
            return false;
        }

        long pageSize = this.pageSize;
        if (this.remaining != Query.NO_LIMIT && this.remaining < pageSize) {
            pageSize = this.remaining;
        }
        this.results = this.queries.fetchNext(this.pageState, pageSize);
        assert this.results != null;

        if (this.results.iterator().hasNext()) {
            if (this.results.page() == null) {
                this.pageState.increase();
            } else {
                this.pageState.page(this.results.page());
            }
            return true;
        } else {
            this.pageState.increase();
            return this.fetch();
        }
    }

    @Override
    public BackendEntry next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }
        BackendEntry entry = this.results.iterator().next();
        if (this.remaining != Query.NO_LIMIT) {
            // Assume one result in each entry (just for index query)
            this.remaining--;
        }
        return entry;
    }

    @Override
    public Object metadata(String meta, Object... args) {
        if (PageState.PAGE.equals(meta)) {
            if (this.pageState.offset() >= this.queries.total()) {
                return null;
            }
            return this.pageState.toString();
        }
        throw new NotSupportException("Invalid meta '%s'", meta);
    }
}
