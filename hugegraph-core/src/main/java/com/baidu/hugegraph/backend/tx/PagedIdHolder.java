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

import java.util.Set;
import java.util.function.Supplier;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.iterator.Metadatable;
import com.baidu.hugegraph.util.E;

public final class PagedIdHolder implements IdHolder, Metadatable {

    private final Query query;
    private final Supplier<PagedIds> idsFetcher;

    private int limit;
    private Set<Id> ids;
    private String page;
    private boolean finished;

    public PagedIdHolder(Query query, Supplier<PagedIds> idsFetcher) {
        E.checkArgument(query.paging(),
                        "Query '%s' must carry the page info in paged mode",
                        query);
        this.query = query;
        this.idsFetcher = idsFetcher;
        this.ids = null;
        this.page = query.page();
        this.finished = false;
    }

    public int limit() {
        return this.limit;
    }

    public void limit(int limit) {
        this.limit = limit;
    }

    public String page() {
        return this.page;
    }

    public void page(String page) {
        this.page = page;
    }

    public void resetPage() {
        this.page = "";
    }

    public Set<Id> fetchFromBackend() {
        if (this.finished) {
            return null;
        }

        this.query.page(this.page);
        this.query.limit(this.limit);

        PagedIds result = this.idsFetcher.get();
        this.ids = result.ids();
        this.page = result.page();

        assert this.ids != null;
        if (this.ids.size() != this.query.limit() || this.page == null) {
            this.finished = true;
        }
        return this.ids;
    }

    @Override
    public int size() {
        if (this.ids == null) {
            return 0;
        }
        return this.ids.size();
    }

    @Override
    public Object metadata(String meta, Object... args) {
        if ("page".equals(meta)) {
            return this.page;
        }
        throw new NotSupportException("Invalid meta '%s'", meta);
    }
}
