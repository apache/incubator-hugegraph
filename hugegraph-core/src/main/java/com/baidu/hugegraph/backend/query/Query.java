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
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.exception.LimitExceedException;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.google.common.collect.ImmutableSet;

public class Query implements Cloneable {

    public static final long NO_LIMIT = Long.MAX_VALUE;

    public static final long COMMIT_BATCH = 500;

    public static final long NO_CAPACITY = -1L;
    public static final long DEFAULT_CAPACITY = 800000L; // HugeGraph-777

    private static final ThreadLocal<Long> capacityContex = new ThreadLocal<>();

    public static final Query NONE = new Query(HugeType.UNKNOWN);

    private HugeType resultType;
    private Map<HugeKeys, Order> orders;
    private long offset;
    private long actualOffset;
    private long limit;
    private String page;
    private long capacity;
    private boolean showHidden;
    private boolean showDeleting;

    private Query originQuery;

    public Query(HugeType resultType) {
        this(resultType, null);
    }

    public Query(HugeType resultType, Query originQuery) {
        this.resultType = resultType;
        this.originQuery = originQuery;

        this.orders = null;

        this.offset = 0L;
        this.actualOffset = 0L;
        this.limit = NO_LIMIT;
        this.page = null;

        this.capacity = defaultCapacity();

        this.showHidden = false;
        this.showDeleting = false;
    }

    public void copyBasic(Query query) {
        E.checkNotNull(query, "query");
        this.offset = query.offset();
        this.limit = query.limit();
        this.page = query.page();
        this.capacity = query.capacity();
        this.showHidden = query.showHidden();
        this.showDeleting = query.showDeleting();
        if (query.orders != null) {
            this.orders(query.orders);
        }
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
        return Collections.unmodifiableMap(this.getOrNewOrders());
    }

    public void orders(Map<HugeKeys, Order> orders) {
        this.orders = InsertionOrderUtil.newMap(orders);
    }

    public void order(HugeKeys key, Order order) {
        this.getOrNewOrders().put(key, order);
    }

    protected Map<HugeKeys, Order> getOrNewOrders() {
        if (this.orders != null) {
            return this.orders;
        }
        this.orders = InsertionOrderUtil.newMap();
        return this.orders;
    }

    public long offset() {
        return this.offset;
    }

    public void offset(long offset) {
        E.checkArgument(offset >= 0L, "Invalid offset %s", offset);
        this.offset = offset;
    }

    public long actualOffset() {
        if (this.originQuery != null) {
            return this.originQuery.actualOffset();
        }
        return this.actualOffset;
    }

    public long skipOffset(long offset) {
        if (this.originQuery != null) {
            return this.originQuery.skipOffset(offset);
        }

        this.actualOffset += offset;
        return this.actualOffset;
    }

    public <T> Set<T> skipOffset(Set<T> elems) {
        if (this.originQuery != null) {
            return this.originQuery.skipOffset(elems);
        }

        long fromIndex = this.offset() - this.actualOffset;
        E.checkArgument(fromIndex <= Integer.MAX_VALUE,
                        "Offset must be <= 0x7fffffff, but got '%s'",
                        fromIndex);

        this.actualOffset += elems.size();

        if (fromIndex >= elems.size()) {
            return ImmutableSet.of();
        }
        if (this.nolimit() && this.offset() == 0) {
            return elems;
        }
        long toIndex = this.total();
        if (this.nolimit() || toIndex > elems.size()) {
            toIndex = elems.size();
        }
        assert fromIndex < elems.size();
        assert toIndex <= elems.size();
        return CollectionUtil.subSet(elems, (int) fromIndex, (int) toIndex);
    }

    public long remainingOffset() {
        return this.offset() - this.actualOffset();
    }

    public long remaining() {
        return this.total() - this.actualOffset();
    }

    public long total() {
        if (this.limit == NO_LIMIT) {
            return NO_LIMIT;
        } else {
            return this.offset + this.limit;
        }
    }

    public long limit() {
        if (this.capacity != NO_CAPACITY) {
            E.checkArgument(this.limit == Query.NO_LIMIT ||
                            this.limit <= this.capacity,
                            "Invalid limit %s, must be <= capacity", this.limit);
        }
        return this.limit;
    }

    public void limit(long limit) {
        E.checkArgument(limit >= 0L, "Invalid limit %s", limit);
        this.limit = limit;
    }

    public boolean nolimit() {
        return this.limit() == NO_LIMIT;
    }

    public boolean reachLimit(long count) {
        long limit = this.limit();
        if (limit == NO_LIMIT) {
            return false;
        }
        return count >= (limit + this.offset());
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
            if (!this.nolimit()) {
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
            E.checkState(this.limit() != 0L,
                         "Can't set limit=0 when using paging");
            E.checkState(this.offset() == 0L,
                         "Can't set offset when using paging, but got '%s'",
                         this.offset());
        }
        return this.page;
    }

    public String pageWithoutCheck() {
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

    public boolean bigCapacity() {
        return this.capacity == NO_CAPACITY || this.capacity > DEFAULT_CAPACITY;
    }

    public void checkCapacity(long count) throws LimitExceedException {
        // Throw LimitExceedException if reach capacity
        if (this.capacity != Query.NO_CAPACITY && count > this.capacity) {
            final int MAX_CHARS = 256;
            String query = this.toString();
            if (query.length() > MAX_CHARS) {
                query = query.substring(0, MAX_CHARS) + "...";
            }
            throw new LimitExceedException(
                      "Too many records(must <= %s) for the query: %s",
                      this.capacity, query);
        }
    }

    public boolean showHidden() {
        return this.showHidden;
    }

    public void showHidden(boolean showHidden) {
        this.showHidden = showHidden;
    }

    public boolean showDeleting() {
        return this.showDeleting;
    }

    public void showDeleting(boolean showDeleting) {
        this.showDeleting = showDeleting;
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
               this.orders().equals(other.orders()) &&
               this.offset == other.offset &&
               this.limit == other.limit &&
               Objects.equals(this.page, other.page) &&
               this.ids().equals(other.ids()) &&
               this.conditions().equals(other.conditions());
    }

    @Override
    public int hashCode() {
        return this.resultType.hashCode() ^
               this.orders().hashCode() ^
               Long.hashCode(this.offset) ^
               Long.hashCode(this.limit) ^
               Objects.hashCode(this.page) ^
               this.ids().hashCode() ^
               this.conditions().hashCode();
    }

    @Override
    public String toString() {
        Map<String, Object> pairs = InsertionOrderUtil.newMap();
        if (this.page != null) {
            pairs.put("page", String.format("'%s'", this.page));
        }
        if (this.offset != 0) {
            pairs.put("offset", this.offset);
        }
        if (this.limit != NO_LIMIT) {
            pairs.put("limit", this.limit);
        }
        if (!this.orders().isEmpty()) {
            pairs.put("order by", this.orders());
        }

        StringBuilder sb = new StringBuilder(64);
        sb.append("Query for ").append(this.resultType);
        for (Map.Entry<String, Object> entry : pairs.entrySet()) {
            sb.append(' ').append(entry.getKey())
              .append(' ').append(entry.getValue()).append(',');
        }
        if (!pairs.isEmpty()) {
            // Delete last comma
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
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

    public final static void checkForceCapacity(long count)
                                                throws LimitExceedException {
        if (count > Query.DEFAULT_CAPACITY) {
            throw new LimitExceedException(
                      "Too many records(must <= %s) for one query",
                      Query.DEFAULT_CAPACITY);
        }
    }

    public static enum Order {
        ASC,
        DESC;
    }
}
