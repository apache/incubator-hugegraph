package com.baidu.hugegraph.backend.query;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;

public class ConditionQuery extends IdQuery {

    // conditions will be concated with `and` by default
    private Set<Condition> conditions;

    public ConditionQuery(HugeType resultType) {
        super(resultType);
        this.conditions = new LinkedHashSet<>();
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

    public ConditionQuery key(HugeKeys key, String value) {
        this.conditions.add(Condition.hasKey(key, value));
        return this;
    }

    public ConditionQuery scan(String start, String end) {
        this.conditions.add(Condition.scan(start, end));
        return this;
    }

    @Override
    public Set<Condition> conditions() {
        return Collections.unmodifiableSet(this.conditions);
    }

    public void resetConditions(Set<Condition> conditions) {
        this.conditions = conditions;
    }

    public void resetConditions() {
        this.conditions = new LinkedHashSet<>();
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

    public List<Condition.Relation> relations() {
        List<Condition.Relation> relations = new LinkedList<>();
        for (Condition c : this.conditions) {
            relations.addAll(c.relations());
        }
        return relations;
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

    public void unsetCondition(Object key) {
        Iterator<Condition> iterator = this.conditions.iterator();
        while (iterator.hasNext()) {
            Condition c = iterator.next();
            if (c.type() == Condition.ConditionType.RELATION
                    && ((Condition.Relation) c).key().equals(key)) {
                iterator.remove();
            }
            // TODO: deal with other Condition
        }
    }

    public boolean containsCondition(HugeKeys key) {
        return this.condition(key) != null;
    }

    public boolean containsCondition(Condition.RelationType type) {
        for (Condition c : this.conditions) {
            if (c.type() == Condition.ConditionType.RELATION
                    && ((Condition.Relation) c).relation().equals(type)) {
                return true;
            }
            // TODO: deal with other Condition
        }
        return false;
    }

    public boolean containsScanCondition() {
        return this.containsCondition(Condition.RelationType.SCAN);
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
        return SplicingIdGenerator.concatValues(this.userpropValues());
    }

    public boolean hasSearchCondition() {
        // NOTE: we need to judge all the conditions, including the nested
        for (Condition.Relation r : this.relations()) {
            if (r.relation().isSearchType()) {
                return true;
            }
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