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

import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.InsertionOrderUtil;

public class SortByCountIdHolderList extends IdHolderList {

    private static final long serialVersionUID = -7779357582250824558L;

    public SortByCountIdHolderList(boolean paging) {
        super(paging);
    }

    @Override
    public boolean add(IdHolder holder) {
        if (this.paging()) {
            return super.add(holder);
        }

        SortByCountIdHolder sortHolder = super.size() > 0 ?
                                         (SortByCountIdHolder) super.get(0) :
                                         new SortByCountIdHolder();
        sortHolder.merge(holder);
        return true;
    }

    private static class SortByCountIdHolder implements IdHolder {

        private final Map<Id, Integer> ids;

        public SortByCountIdHolder() {
            this.ids = InsertionOrderUtil.newMap();
        }

        public void merge(IdHolder holder) {
            for (Id id : holder.all()) {
                this.ids.compute(id, (k, v) -> v == null ? 1 : v + 1);
            }
        }

        @Override
        public boolean paging() {
            return false;
        }

        @Override
        public Set<Id> all() {
            return CollectionUtil.sortByValue(this.ids, false).keySet();
        }

        @Override
        public PageIds fetchNext(String page, long pageSize) {
            throw new NotImplementedException("SortByCountIdHolder.fetchNext");
        }
    }
}
