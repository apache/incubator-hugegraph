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
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableSet;

public class Query implements Cloneable {

    public static final long NO_LIMIT = Long.MAX_VALUE;

    public static final long NO_CAPACITY = -1L;
    public static final long DEFAULT_CAPACITY = 100000L; // HugeGraph-777

    private HugeType resultType;
    private Map<HugeKeys, Order> orders;
    private long offset;
    private long limit;
    private long capacity;

    public Query(HugeType resultType) {
        this.resultType = resultType;
        this.orders = new LinkedHashMap<>();
        this.offset = 0L;
        this.limit = NO_LIMIT;
        this.capacity = DEFAULT_CAPACITY;
    }

    public HugeType resultType() {
        return this.resultType;
    }

    public void resultType(HugeType resultType) {
        this.resultType = resultType;
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
        E.checkArgument(offset <= this.limit,
                        "Invalid offset %s due to > limit %s",
                        offset, this.limit);
        this.offset = offset;
    }

    public long limit() {
        return this.limit;
    }

    public void limit(long limit) {
        E.checkArgument(limit >= this.offset,
                        "Invalid limit %s due to < offset %s",
                        limit, this.offset);
        this.limit = limit;
    }

    public long capacity() {
        return this.capacity;
    }

    public void capacity(long capacity) {
        this.capacity = capacity;
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

    @Override
    public Query clone() {
        try {
            return (Query) super.clone();
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
               this.ids().equals(other.ids()) &&
               this.conditions().equals(other.conditions());
    }

    @Override
    public int hashCode() {
        return this.resultType.hashCode() ^
               this.orders.hashCode() ^
               (int) this.offset ^
               (int) this.limit ^
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

    public static enum Order {
        ASC,
        DESC;
    }
}
