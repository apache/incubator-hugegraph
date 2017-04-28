package com.baidu.hugegraph.backend.query;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
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
        this.conditions.add(new Condition.SyspropRelation(key));
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

    public void resetConditions(List<Condition> conditions) {
        this.conditions = conditions;
    }

    @Override
    public String toString() {
        return String.format("%s and %s",
                super.toString(),
                this.conditions.toString());
    }

    public boolean allSysprop() {
        for (Condition c : this.conditions) {
            if (!c.isSysprop()) {
                return false;
            }
        }
        return true;
    }

    public Object condition(Object key) {
        for (Condition c : this.conditions) {
            if (c.type() == Condition.ConditionType.RELATION) {
                Condition.Relation r = (Condition.Relation) c;
                if (r.key().equals(key)) {
                    return r.value();
                }
            }
            // TODO: deal with other Condition
        }
        return null;
    }

    public List<Condition> userpropConditions() {
        List<Condition> conds = new LinkedList<>();
        for (Condition c : this.conditions) {
            if (c.type() == Condition.ConditionType.RELATION
                    && !((Condition.Relation) c).isSysprop()) {
                Condition.UserpropRelation r = (Condition.UserpropRelation) c;
                conds.add(r);
            }
            // TODO: deal with other Condition
        }
        return conds;
    }

    public void resetUserpropConditions() {
        Iterator<Condition> iterator = this.conditions.iterator();
        while (iterator.hasNext()) {
            Condition c = iterator.next();
            if (c.type() == Condition.ConditionType.RELATION
                    && !((Condition.Relation) c).isSysprop()) {
                iterator.remove();
            }
            // TODO: deal with other Condition
        }
    }

    public Set<String> userpropKeys() {
        Set<String> keys = new LinkedHashSet<>();
        for (Condition c : this.conditions) {
            if (c.type() == Condition.ConditionType.RELATION
                    && !((Condition.Relation) c).isSysprop()) {
                Condition.UserpropRelation r = (Condition.UserpropRelation) c;
                keys.add(r.key());
            }
            // TODO: deal with other Condition
        }
        return keys;
    }

    public Set<Object> userpropValues() {
        Set<Object> keys = new LinkedHashSet<>();
        for (Condition c : this.conditions) {
            if (c.type() == Condition.ConditionType.RELATION
                    && !((Condition.Relation) c).isSysprop()) {
                Condition.Relation r = (Condition.Relation) c;
                keys.add(r.value());
            }
            // TODO: deal with other Condition
        }
        return keys;
    }

    public String userpropValuesString() {
        return StringUtils.join(
                this.userpropValues(),
                SplicingIdGenerator.NAME_SPLITOR);
    }

    public boolean hasSearchCondition() {
        for (Condition c : this.conditions) {
            if (c.type() == Condition.ConditionType.RELATION) {
                Condition.Relation r = (Condition.Relation) c;
                if (r.relation() != Condition.RelationType.EQ) {
                    return true;
                }
            }
            // TODO: deal with other Condition
        }
        return false;
    }

    public boolean matchUserpropKeys(Set<String> keys) {
        Set<String> conditionKeys = userpropKeys();
        if (keys.size() == conditionKeys.size()
                && keys.containsAll(conditionKeys)) {
            return true;
        }

        return false;
    }
}