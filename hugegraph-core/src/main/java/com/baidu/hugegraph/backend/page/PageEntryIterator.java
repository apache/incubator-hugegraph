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
    private PageInfo pageInfo;
    private long remaining;

    public PageEntryIterator(QueryList queries, long pageSize) {
        this.queries = queries;
        this.pageSize = pageSize;
        this.results = QueryList.PageIterator.EMPTY;
        this.pageInfo = this.parsePageState();
        this.remaining = queries.parent().limit();
    }

    private PageInfo parsePageState() {
        String page = this.queries.parent().pageWithoutCheck();
        PageInfo pageInfo = PageInfo.fromString(page);
        E.checkState(pageInfo.offset() < this.queries.total(),
                     "Invalid page '%s' with an offset '%s' exceeds " +
                     "the size of IdHolderList", page, pageInfo.offset());
        return pageInfo;
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
            this.pageInfo.offset() >= this.queries.total()) {
            return false;
        }

        long pageSize = this.pageSize;
        if (this.remaining != Query.NO_LIMIT && this.remaining < pageSize) {
            pageSize = this.remaining;
        }
        this.results = this.queries.fetchNext(this.pageInfo, pageSize);
        assert this.results != null;

        if (this.results.iterator().hasNext()) {
            if (!this.results.hasNextPage()) {
                this.pageInfo.increase();
            } else {
                this.pageInfo.page(this.results.page());
            }
            this.remaining -= this.results.total();
            return true;
        } else {
            this.pageInfo.increase();
            return this.fetch();
        }
    }

    @Override
    public BackendEntry next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }
        return this.results.iterator().next();
    }

    @Override
    public Object metadata(String meta, Object... args) {
        if (PageInfo.PAGE.equals(meta)) {
            if (this.pageInfo.offset() >= this.queries.total()) {
                return null;
            }
            return this.pageInfo;
        }
        throw new NotSupportException("Invalid meta '%s'", meta);
    }
}
