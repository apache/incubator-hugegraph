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

import java.util.Set;
import java.util.function.Supplier;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.util.E;

public final class IdHolder {

    private final Query query;
    private final Supplier<PageIds> idsFetcher;
    private boolean exhausted;

    private Set<Id> ids;

    /**
     * For non-paged situation
     */
    public IdHolder(Set<Id> ids) {
        this.query = null;
        this.idsFetcher = null;
        this.exhausted = false;
        this.ids = ids;
    }

    /**
     * For paged situation
     */
    public IdHolder(Query query, Supplier<PageIds> idsFetcher) {
        E.checkArgument(query.paging(),
                        "Query '%s' must carry the page info in paged mode",
                        query);
        this.query = query;
        this.idsFetcher = idsFetcher;
        this.exhausted = false;
        this.ids = null;
    }

    public void merge(Set<Id> ids) {
        this.ids.addAll(ids);
    }

    public Set<Id> ids() {
        E.checkNotNull(this.ids, "ids");
        return this.ids;
    }

    public int size() {
        if (this.ids == null) {
            return 0;
        }
        return this.ids.size();
    }

    public boolean paging() {
        return this.idsFetcher != null;
    }

    public PageIds fetchNext(String page, long pageSize) {
        if (this.exhausted) {
            return PageIds.EMPTY;
        }

        this.query.page(page);
        this.query.limit(pageSize);

        PageIds result = this.idsFetcher.get();

        assert result != null;
        this.ids = result.ids();
        if (this.ids.size() != this.query.limit() || result.page() == null) {
            this.exhausted = true;
        }
        return result;
    }
}
