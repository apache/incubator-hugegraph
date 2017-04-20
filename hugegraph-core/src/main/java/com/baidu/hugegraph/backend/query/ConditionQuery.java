package com.baidu.hugegraph.backend.query;

import java.util.LinkedList;
import java.util.List;

import com.baidu.hugegraph.type.HugeTypes;
import com.baidu.hugegraph.type.define.HugeKeys;

public class ConditionQuery extends IdQuery {

    // conditions will be concated with `and` by default
    private List<Condition> conditions;

    public ConditionQuery(HugeTypes resultType) {
        super(resultType);
        this.conditions = new LinkedList<>();
    }

    public ConditionQuery query(HugeKeys key) {
        // query value by key (such as: get property value bye key)
        this.conditions.add(new Condition.Relation(key));
        return this;
    }

    public ConditionQuery query(Condition condition) {
        this.conditions.add(condition);
        return this;
    }

    public ConditionQuery eq(HugeKeys key, Object value) {
        // filter value by key
        this.conditions.add(Condition.eq(key, value));
        return this;
    }

    public ConditionQuery gt(HugeKeys key, Object value) {
        this.conditions.add(Condition.gt(key, value));
        return this;
    }

    public ConditionQuery gte(HugeKeys key, Object value) {
        this.conditions.add(Condition.gte(key, value));
        return this;
    }

    public ConditionQuery lt(HugeKeys key, Object value) {
        this.conditions.add(Condition.lt(key, value));
        return this;
    }

    public ConditionQuery lte(HugeKeys key, Object value) {
        this.conditions.add(Condition.lte(key, value));
        return this;
    }

    public ConditionQuery neq(HugeKeys key, Object value) {
        this.conditions.add(Condition.neq(key, value));
        return this;
    }

    @Override
    public List<Condition> conditions() {
        return this.conditions;
    }
}