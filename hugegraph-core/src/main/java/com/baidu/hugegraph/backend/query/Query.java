package com.baidu.hugegraph.backend.query;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.type.HugeTypes;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class Query {

    private HugeTypes resultType;
    private Map<HugeKeys, Order> orders;
    private int offset;
    private int limit;

    public Query(HugeTypes resultType) {
        this.resultType = resultType;
    }

    public HugeTypes resultType() {
        return this.resultType;
    }

    public void resultType(HugeTypes resultType) {
        this.resultType = resultType;
    }

    public Map<HugeKeys, Order> orders() {
        return this.orders;
    }

    public void orders(Map<HugeKeys, Order> orders) {
        this.orders = orders;
    }

    public int offset() {
        return this.offset;
    }

    public void offset(int offset) {
        this.offset = offset;
    }

    public int limit() {
        return this.limit;
    }

    public void limit(int limit) {
        this.limit = limit;
    }

    public Set<Id> ids() {
        return ImmutableSet.of();
    }

    public List<Condition> conditions() {
        return ImmutableList.of();
    }

    public enum Order {
        ASC,
        DESC;
    }
}
