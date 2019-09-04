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

package com.baidu.hugegraph.traversal.optimize;

import java.util.Iterator;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;

import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.iterator.Metadatable;

public interface QueryHolder extends HasContainerHolder, Metadatable {

    public static final String SYSPROP_PAGE = "~page";

    public static class PageElementIterator<E> implements Iterator<E>,
                                                          Metadatable {
        @Override
        public Object metadata(String meta, Object... args) {
            return null;
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public E next() {
            return null;
        }
    }

    public Iterator<?> lastTimeResults();

    @Override
    public default Object metadata(String meta, Object... args) {
        Iterator<?> results = this.lastTimeResults();
        if (results instanceof Metadatable) {
            return ((Metadatable) results).metadata(meta, args);
        }
        throw new IllegalStateException("Original results is not Metadatable");
    }

    public Query queryInfo();

    public default void orderBy(String key, Order order) {
        this.queryInfo().order(TraversalUtil.string2HugeKey(key),
                               TraversalUtil.convOrder(order));
    }

    public default long setRange(long start, long end) {
        this.queryInfo().range(start, end);
        return this.queryInfo().limit();
    }

    public default void setPage(String page) {
        this.queryInfo().page(page);
    }

    public default void setCount() {
        this.queryInfo().capacity(Query.NO_CAPACITY);
    }

    public default <Q extends Query> Q injectQueryInfo(Q query) {
        query.copyBasic(this.queryInfo());
        return query;
    }
}
