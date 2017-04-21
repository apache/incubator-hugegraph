package com.baidu.hugegraph.backend.query;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.type.HugeTypes;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class Query implements Cloneable {

    private HugeTypes resultType;
    private Map<HugeKeys, Order> orders;
    private int offset;
    private int limit;

    public Query(HugeTypes resultType) {
        this.resultType = resultType;
        this.orders = new ConcurrentHashMap<>();
        this.offset = 0;
        this.limit = 0;
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

    public void order(HugeKeys key, Order order) {
        this.orders.put(key, order);
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

    @Override
    public Query clone() {
        try {
            return (Query) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new BackendException(e);
        }
    }

    @Override
    public String toString() {
        return String.format("Query for %s offset=%d, limit=%d, order by %s",
                this.resultType,
                this.offset,
                this.limit,
                this.orders.toString());
    }

    public enum Order {
        ASC,
        DESC;
    }
}
