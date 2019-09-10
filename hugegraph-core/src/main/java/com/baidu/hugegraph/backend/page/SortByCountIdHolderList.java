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
import com.google.common.collect.ImmutableSet;

public class SortByCountIdHolderList extends IdHolderList {

    private static final long serialVersionUID = -3702668078311531645L;

    public SortByCountIdHolderList(boolean paging) {
        super(paging);
    }

    @Override
    public boolean add(IdHolder holder) {
        if (!this.paging()) {
            holder = new SortByCountIdHolder(holder);
        }
        return super.add(holder);
    }

    private static class SortByCountIdHolder extends IdHolder {

        private final Map<Id, Integer> ids;

        public SortByCountIdHolder(IdHolder holder) {
            super(ImmutableSet.of());
            this.ids = InsertionOrderUtil.newMap();
            this.merge(holder.ids());
        }

        @Override
        public void merge(Set<Id> ids) {
            for (Id id : ids) {
                this.ids.compute(id, (k, v) -> v == null ? 1 : v + 1);
            }
        }

        @Override
        public Set<Id> ids() {
            return CollectionUtil.sortByValue(this.ids, false).keySet();
        }

        @Override
        public int size() {
            return this.ids.size();
        }

        @Override
        public boolean paging() {
            return false;
        }

        @Override
        public PageIds fetchNext(String page, long pageSize) {
            throw new NotImplementedException("SortByCountIdHolder.fetchNext");
        }
    }
}
