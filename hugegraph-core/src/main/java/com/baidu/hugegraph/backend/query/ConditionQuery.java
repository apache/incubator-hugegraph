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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.query.Condition.Relation;
import com.baidu.hugegraph.backend.query.Condition.RelationType;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableSet;

public class ConditionQuery extends IdQuery {

    // Conditions will be concated with `and` by default
    private Set<Condition> conditions = null;

    public ConditionQuery(HugeType resultType) {
        super(resultType);
    }

    public ConditionQuery(HugeType resultType, Query originQuery) {
        super(resultType, originQuery);
    }

    public ConditionQuery query(Condition condition) {
        // Query by id (HugeGraph-259)
        if (condition instanceof Relation) {
            Relation relation = (Relation) condition;
            if (relation.key().equals(HugeKeys.ID) &&
                relation.relation() == RelationType.EQ) {

                super.query(HugeElement.getIdValue(relation.value()));
                return this;
            }
        }

        if (this.conditions == null) {
            this.conditions = new LinkedHashSet<>();
        }
        this.conditions.add(condition);
        return this;
    }

    public ConditionQuery eq(HugeKeys key, Object value) {
        // Filter value by key
        return this.query(Condition.eq(key, value));
    }

    public ConditionQuery gt(HugeKeys key, Object value) {
        return this.query(Condition.gt(key, value));
    }

    public ConditionQuery gte(HugeKeys key, Object value) {
        return this.query(Condition.gte(key, value));
    }

    public ConditionQuery lt(HugeKeys key, Object value) {
        return this.query(Condition.lt(key, value));
    }

    public ConditionQuery lte(HugeKeys key, Object value) {
        return this.query(Condition.lte(key, value));
    }

    public ConditionQuery neq(HugeKeys key, Object value) {
        return this.query(Condition.neq(key, value));
    }

    public ConditionQuery key(HugeKeys key, Object value) {
        return this.query(Condition.containsKey(key, value));
    }

    public ConditionQuery scan(String start, String end) {
        return this.query(Condition.scan(start, end));
    }

    @Override
    public Set<Condition> conditions() {
        if (this.conditions == null) {
            return ImmutableSet.of();
        }
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

    public List<Condition.Relation> relations() {
        List<Condition.Relation> relations = new ArrayList<>();
        for (Condition c : this.conditions) {
            relations.addAll(c.relations());
        }
        return relations;
    }

    public Object condition(Object key) {
        for (Condition c : this.conditions) {
            if (c.isRelation()) {
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
        for (Iterator<Condition> iter = this.conditions.iterator();
             iter.hasNext();) {
            Condition c = iter.next();
            if (c.isRelation() && ((Condition.Relation) c).key().equals(key)) {
                iter.remove();
            }
            // TODO: deal with other Condition
        }
    }

    public boolean containsCondition(HugeKeys key) {
        return this.condition(key) != null;
    }

    public boolean containsCondition(HugeKeys key,
                                     Condition.RelationType type) {
        for (Condition c : this.conditions) {
            if (c.isRelation()) {
                Condition.Relation r = (Condition.Relation) c;
                if (r.key().equals(key) && r.relation().equals(type)) {
                    return true;
                }
            }
            // TODO: deal with other Condition
        }
        return false;
    }

    public boolean containsCondition(Condition.RelationType type) {
        for (Condition c : this.conditions) {
            if (c.isRelation() &&
                ((Condition.Relation) c).relation().equals(type)) {
                return true;
            }
            // TODO: deal with other Condition
        }
        return false;
    }

    public boolean containsScanCondition() {
        return this.containsCondition(Condition.RelationType.SCAN);
    }

    public boolean allSysprop() {
        for (Condition c : this.conditions) {
            if (!c.isSysprop()) {
                return false;
            }
        }
        return true;
    }

    public List<Condition> syspropConditions() {
        List<Condition> conds = new ArrayList<>();
        for (Condition c : this.conditions) {
            if (c.isSysprop()) {
                conds.add(c);
            }
        }
        return conds;
    }

    public List<Condition> userpropConditions() {
        List<Condition> conds = new ArrayList<>();
        for (Condition c : this.conditions) {
            if (!c.isSysprop()) {
                conds.add(c);
            }
        }
        return conds;
    }

    public void resetUserpropConditions() {
        this.conditions.removeIf(condition -> !condition.isSysprop());
    }

    public Set<Id> userpropKeys() {
        Set<Id> keys = new LinkedHashSet<>();
        for (Relation r : this.relations()) {
            if (!r.isSysprop()) {
                Condition.UserpropRelation ur = (Condition.UserpropRelation) r;
                keys.add(ur.key());
            }
        }
        return keys;
    }

    public List<Object> userpropValues(List<Id> fields) {
        List<Object> values = new ArrayList<>(fields.size());
        for (Id field : fields) {
            boolean got = false;
            for (Condition c : this.conditions) {
                if (!c.isRelation()) {
                    // TODO: deal with other Condition like AND/OR
                    throw new NotSupportException(
                              "obtaining userprop from non relation");
                }
                Relation r = ((Relation) c);
                if (r.key().equals(field) && !r.isSysprop()) {
                    /*
                     * This method only used for secondary index scenario,
                     * relation must be IN or EQ
                     */
                    values.add(singleValueOfRelationInEq(r));
                }
                got = true;
            }
            if (!got) {
                throw new BackendException(
                          "No such userprop named '%s' in the query '%s'",
                          field, this);
            }
        }
        return values;
    }

    public Object userpropValue(Id field) {
        for (Condition c : this.conditions) {
            if (!c.isRelation()) {
                // And/Or
                continue;
            }
            Relation r = ((Relation) c);
            /*
             * Only relation type IN (only one element) or EQ has single value.
             * Relation type GT, LT, GTE, LTE etc. doesn't have.
             */
            if (r.key().equals(field) && !r.isSysprop() &&
                (r.relation == Condition.RelationType.IN ||
                 r.relation == Condition.RelationType.EQ)) {
                return singleValueOfRelationInEq(r);
            }
        }
        return null;
    }

    private static Object singleValueOfRelationInEq(Relation r) {
        if (r.relation() == Condition.RelationType.IN) {
            List<?> fieldValues = (List<?>) r.value();
            E.checkArgument(fieldValues.size() == 1,
                            "Only support one element IN index query");
            return fieldValues.get(0);
        } else {
            E.checkArgument(r.relation() == Condition.RelationType.EQ,
                            "Must be IN or EQ index query, but got %s",
                            r.relation());
            return r.value();
        }
    }

    public String userpropValuesString(List<Id> fields) {
        return SplicingIdGenerator.concatValues(this.userpropValues(fields));
    }

    public boolean hasRangeCondition() {
        // NOTE: we need to judge all the conditions, including the nested
        for (Condition.Relation r : this.relations()) {
            if (r.relation().isRangeType()) {
                return true;
            }
        }
        return false;
    }

    public boolean matchUserpropKeys(List<Id> keys) {
        Set<Id> conditionKeys = userpropKeys();
        if (keys.size() == conditionKeys.size() &&
            conditionKeys.containsAll(keys)) {
            return true;
        }
        return false;
    }

    public List<ConditionQuery> flatten() {
        return ConditionQueryFlatten.flatten(this);
    }

    @Override
    public ConditionQuery copy() {
        ConditionQuery query = (ConditionQuery) super.copy();
        query.conditions = new LinkedHashSet<>(this.conditions);
        return query;
    }

    @Override
    public boolean test(HugeElement element) {
        if (!this.ids().isEmpty() && !super.test(element)) {
            return false;
        }
        for (Condition cond : this.conditions()) {
            if (!cond.test(element)) {
                return false;
            }
        }
        return true;
    }
}
