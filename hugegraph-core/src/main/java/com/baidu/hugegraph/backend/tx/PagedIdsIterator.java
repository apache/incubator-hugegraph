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

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.util.E;

public class PagedIdsIterator extends PagedIterator<Set<Id>> {

    private final List<PagedIdHolder> holders;
    // The size of each page fetched by the inner page
    private final int pageSize;
    private Set<Id> results;

    @SuppressWarnings("unchecked")
    public PagedIdsIterator(IdHolderChain chain, String page,
                            int pageSize, long limit) {
        super(limit, 0);
        E.checkArgument(chain.paged(), "IdHolderChain must be paged");
        E.checkNotNull(page, "page");
        this.holders = (List<PagedIdHolder>) (Object) chain.idsHolders();
        this.pageSize = pageSize;
        this.results = null;

        this.parsePageState(page);
    }

    private void parsePageState(String pageInfo) {
        if (pageInfo.isEmpty()) {
            for (PagedIdHolder holder : this.holders) {
                holder.resetPage();
            }
        } else {
            PageState pageState = PageState.fromString(pageInfo);
            this.offset = pageState.offset();
            E.checkState(this.offset < this.holders.size(),
                         "Invalid page '%s' with an offset '%s' exceeds " +
                         "the size of IdHolderChain", pageInfo, this.offset);
            for (int i = this.offset; i < this.holders.size(); i++) {
                PagedIdHolder holder = this.holders.get(i);
                if (i == this.offset) {
                    holder.page(pageState.page());
                } else {
                    holder.resetPage();
                }
            }
        }
    }

    @Override
    public boolean hasNext() {
        if (this.results != null) {
            return true;
        }
        if (this.remaining <= 0 || this.offset >= this.holders.size()) {
            return false;
        }

        this.fetch();
        return this.results != null;
    }

    private void fetch() {
        if (this.remaining <= 0 || this.offset >= this.holders.size()) {
            return;
        }

        PagedIdHolder holder = this.holders.get(this.offset);
        if (this.remaining < this.pageSize) {
            holder.limit((int) this.remaining);
        } else {
            holder.limit(this.pageSize);
        }

        this.results = holder.fetchFromBackend();

        if (this.results != null && !this.results.isEmpty()) {
            this.remaining -= this.results.size();
            this.makeUpOnePageIfNeed();
        } else {
            this.offset++;
            this.fetch();
        }
    }

    /**
     * If the result is not enough pagesize, should try to make up a page.
     */
    private void makeUpOnePageIfNeed() {
        if (this.results.size() < this.pageSize && this.remaining > 0) {
            if (++this.offset >= this.holders.size()) {
                return;
            }
            int lackCount = (int) Math.min(this.pageSize - this.results.size(),
                                           this.remaining);
            PagedIdHolder holder = this.holders.get(this.offset);
            holder.limit(lackCount);
            Set<Id> compensated = holder.fetchFromBackend();
            if (compensated != null && !compensated.isEmpty()) {
                this.remaining -= compensated.size();
                this.results.addAll(compensated);
            }
            this.makeUpOnePageIfNeed();
        }
    }

    @Override
    public Set<Id> next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException();
        }
        Set<Id> results = this.results;
        this.results = null;
        return results;
    }

    @Override
    public Object metadata(String meta, Object... args) {
        if ("page".equals(meta)) {
            PagedIdHolder holder = this.holders.get(this.offset);
            Object page = holder.metadata(meta, args);
            if (page == null) {
                if (++this.offset < this.holders.size()) {
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
