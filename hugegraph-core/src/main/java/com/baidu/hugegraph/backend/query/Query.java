package com.baidu.hugegraph.backend.query;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.google.common.collect.ImmutableSet;

public class Query implements Cloneable {

    public static final long NO_LIMIT = Long.MAX_VALUE;

    private HugeType resultType;
    private Map<HugeKeys, Order> orders;
    private long offset;
    private long limit;

    public Query(HugeType resultType) {
        this.resultType = resultType;
        this.orders = new ConcurrentHashMap<>();
        this.offset = 0;
        this.limit = NO_LIMIT;
    }

    public HugeType resultType() {
        return this.resultType;
    }

    public void resultType(HugeType resultType) {
        this.resultType = resultType;
    }

    public Map<HugeKeys, Order> orders() {
        return this.orders;
    }

    public void order(HugeKeys key, Order order) {
        this.orders.put(key, order);
    }

    public long offset() {
        return this.offset;
    }

    public void offset(long offset) {
        this.offset = offset;
    }

    public long limit() {
        return this.limit;
    }

    public void limit(long limit) {
        this.limit = limit;
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

    public enum Order {
        ASC,
        DESC;
    }
}
