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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;

public class Query implements Cloneable {

    public static final long NO_LIMIT = Long.MAX_VALUE;

    public static final long NO_CAPACITY = -1L;
    public static final long DEFAULT_CAPACITY = 800000L; // HugeGraph-777

    private HugeType resultType;
    private Map<HugeKeys, Order> orders;
    private long offset;
    private long limit;
    private String page;
    private long capacity;
    private boolean showHidden;

    private Query originQuery;

    private static final ThreadLocal<Long> capacityContex = new ThreadLocal<>();

    public Query(HugeType resultType) {
        this(resultType, null);
    }

    public Query(HugeType resultType, Query originQuery) {
        this.resultType = resultType;
        this.originQuery = originQuery;

        this.orders = new LinkedHashMap<>();

        this.offset = 0L;
        this.limit = NO_LIMIT;
        this.page = null;

        this.capacity = defaultCapacity();

        this.showHidden = false;
    }

    public HugeType resultType() {
        return this.resultType;
    }

    public void resultType(HugeType resultType) {
        this.resultType = resultType;
    }

    public Query originQuery() {
        return this.originQuery;
    }

    protected void originQuery(Query originQuery) {
        this.originQuery = originQuery;
    }

    public Map<HugeKeys, Order> orders() {
        return Collections.unmodifiableMap(this.orders);
    }

    public void orders(Map<HugeKeys, Order> orders) {
        this.orders = new LinkedHashMap<>(orders);
    }

    public void order(HugeKeys key, Order order) {
        this.orders.put(key, order);
    }

    public long offset() {
        return this.offset;
    }

    public void offset(long offset) {
        E.checkArgument(offset >= 0L, "Invalid offset %s", offset);
        this.offset = offset;
    }

    public long total() {
        if (this.limit == NO_LIMIT) {
            return NO_LIMIT;
        } else {
            return this.offset + this.limit;
        }
    }

    public long limit() {
        return this.limit;
    }

    public void limit(long limit) {
        E.checkArgument(limit >= 0L, "Invalid limit %s", limit);
        this.limit = limit;
    }

    public boolean reachLimit(long count) {
        if (this.limit == NO_LIMIT) {
            return false;
        }
        return count >= (this.offset + this.limit);
    }

    /**
     * Set or update the offset and limit by a range [start, end)
     * NOTE: it will use the min range one: max start and min end
     * @param start the range start, include it
     * @param end   the range end, exclude it
     */
    public void range(long start, long end) {
        // Update offset
        long offset = this.offset();
        start = Math.max(start, offset);
        this.offset(start);

        // Update limit
        if (end != -1L) {
            if (this.limit() != Query.NO_LIMIT) {
                end = Math.min(end, offset + this.limit());
            } else {
                assert end < Query.NO_LIMIT;
            }
            E.checkArgument(end >= start,
                            "Invalid range: [%s, %s)", start, end);
            this.limit(end - start);
        } else {
            // Keep the origin limit
            assert this.limit() <= Query.NO_LIMIT;
        }
    }

    public String page() {
        if (this.page != null) {
            E.checkState(this.limit != Query.NO_LIMIT,
                         "Must set limit when using paging");
            E.checkState(this.limit != 0L,
                         "Can't set limit=0 when using paging");
            E.checkState(this.offset == 0L,
                         "Can't set offset when using paging, but got '%s'",
                         this.offset);
        }
        return this.page;
    }

    public void page(String page) {
        this.page = page;
    }

    public boolean paging() {
        return this.page != null;
    }

    public long capacity() {
        return this.capacity;
    }

    public void capacity(long capacity) {
        this.capacity = capacity;
    }

    public void checkCapacity(long count) {
        // Throw BackendException if reach capacity
        if (this.capacity != Query.NO_CAPACITY && count > this.capacity) {
            final int MAX_CHARS = 256;
            String query = this.toString();
            if (query.length() > MAX_CHARS) {
                query = query.substring(0, MAX_CHARS) + "...";
            }
            throw new BackendException(
                      "Too many records(must <=%s) for a query: %s",
                      this.capacity, query);
        }
    }

    public boolean showHidden() {
        return this.showHidden;
    }

    public void showHidden(boolean showHidden) {
        this.showHidden = showHidden;
    }

    public Set<Id> ids() {
        return ImmutableSet.of();
    }

    public Set<Condition> conditions() {
        return ImmutableSet.of();
    }

    public boolean empty() {
        return this.ids().isEmpty() && this.conditions().isEmpty();
    }

    public boolean test(HugeElement element) {
        return true;
    }

    public Query copy() {
        try {
            return (Query) this.clone();
        } catch (CloneNotSupportedException e) {
            throw new BackendException(e);
        }
    }

    @Override
    public boolean equals(Object object) {
        if (!(object instanceof Query)) {
            return false;
        }
        Query other = (Query) object;
        return this.resultType.equals(other.resultType) &&
               this.orders.equals(other.orders) &&
               this.offset == other.offset &&
               this.limit == other.limit &&
               ((this.page == null && other.page == null) ||
                this.page.equals(other.page)) &&
               this.ids().equals(other.ids()) &&
               this.conditions().equals(other.conditions());
    }

    @Override
    public int hashCode() {
        return this.resultType.hashCode() ^
               this.orders.hashCode() ^
               Long.hashCode(this.offset) ^
               Long.hashCode(this.limit) ^
               Objects.hashCode(this.page) ^
               this.ids().hashCode() ^
               this.conditions().hashCode();
    }

    @Override
    public String toString() {
        return String.format("Query for %s offset=%d, limit=%d, order by %s",
                             this.resultType,
                             this.offset,
                             this.limit,
                             this.orders.toString());
    }

    public static long defaultCapacity(long capacity) {
        Long old = capacityContex.get();
        capacityContex.set(capacity);
        return old != null ? old : DEFAULT_CAPACITY;
    }

    public static long defaultCapacity() {
        Long capacity = capacityContex.get();
        return capacity != null ? capacity : DEFAULT_CAPACITY;
    }

    public static enum Order {
        ASC,
        DESC;
    }
}
