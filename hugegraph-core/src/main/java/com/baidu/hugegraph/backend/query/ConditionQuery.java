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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.query.Condition.Relation;
import com.baidu.hugegraph.backend.query.Condition.RelationType;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.LongEncoding;
import com.baidu.hugegraph.util.NumericUtil;
import com.google.common.base.Function;

public final class ConditionQuery extends IdQuery {

    // Conditions will be concated with `and` by default
    private Set<Condition> conditions = new LinkedHashSet<>();

    private OptimizedType optimizedType = OptimizedType.NONE;
    private Function<HugeElement, Boolean> resultsFilter = null;

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
                E.checkArgument(relation.value() instanceof Id,
                                "Invalid id value '%s'", relation.value());
                super.query((Id) relation.value());
                return this;
            }
        }

        this.conditions.add(condition);
        return this;
    }

    public ConditionQuery query(List<Condition> conditions) {
        for (Condition condition : conditions) {
            this.query(condition);
        }
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

    public ConditionQuery prefix(HugeKeys key, Id value) {
        return this.query(Condition.prefix(key, value));
    }

    public ConditionQuery key(HugeKeys key, Object value) {
        return this.query(Condition.containsKey(key, value));
    }

    public ConditionQuery scan(String start, String end) {
        return this.query(Condition.scan(start, end));
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

    public List<Condition.Relation> relations() {
        List<Condition.Relation> relations = new ArrayList<>();
        for (Condition c : this.conditions) {
            relations.addAll(c.relations());
        }
        return relations;
    }

    public <T> T condition(Object key) {
        List<Object> values = new ArrayList<>();
        for (Condition c : this.conditions) {
            if (c.isRelation()) {
                Condition.Relation r = (Condition.Relation) c;
                if (r.key().equals(key) && r.relation() == RelationType.EQ) {
                    values.add(r.value());
                }
            }
        }
        if (values.isEmpty()) {
            return null;
        }
        E.checkState(values.size() == 1,
                     "Illegal key '%s' with more than one value", key);
        @SuppressWarnings("unchecked")
        T value = (T) values.get(0);
        return value;
    }

    public void unsetCondition(Object key) {
        for (Iterator<Condition> iter = this.conditions.iterator();
             iter.hasNext();) {
            Condition c = iter.next();
            E.checkState(c.isRelation(), "Can't unset condition '%s'", c);
            if (((Condition.Relation) c).key().equals(key)) {
                iter.remove();
            }
        }
    }

    public boolean containsCondition(HugeKeys key) {
        for (Condition c : this.conditions) {
            if (c.isRelation()) {
                Condition.Relation r = (Condition.Relation) c;
                if (r.key().equals(key)) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean containsCondition(HugeKeys key,
                                     Condition.RelationType type) {
        for (Relation r : this.relations()) {
            if (r.key().equals(key) && r.relation().equals(type)) {
                return true;
            }
        }
        return false;
    }

    public boolean containsCondition(Condition.RelationType type) {
        for (Relation r : this.relations()) {
            if (r.relation().equals(type)) {
                return true;
            }
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

    public boolean allRelation() {
        for (Condition c : this.conditions) {
            if (!c.isRelation()) {
                return false;
            }
        }
        return true;
    }

    public List<Condition> syspropConditions() {
        this.checkFlattened();
        List<Condition> conds = new ArrayList<>();
        for (Condition c : this.conditions) {
            if (c.isSysprop()) {
                conds.add(c);
            }
        }
        return conds;
    }

    public List<Condition> syspropConditions(HugeKeys key) {
        this.checkFlattened();
        List<Condition> conditions = new ArrayList<>();
        for (Condition condition : this.conditions) {
            Relation relation = (Relation) condition;
            if (relation.key().equals(key)) {
                conditions.add(relation);
            }
        }
        return conditions;
    }

    public List<Condition> userpropConditions() {
        this.checkFlattened();
        List<Condition> conds = new ArrayList<>();
        for (Condition c : this.conditions) {
            if (!c.isSysprop()) {
                conds.add(c);
            }
        }
        return conds;
    }

    public List<Condition> userpropConditions(Id key) {
        this.checkFlattened();
        List<Condition> conditions = new ArrayList<>();
        for (Condition condition : this.conditions) {
            Relation relation = (Relation) condition;
            if (relation.key().equals(key)) {
                conditions.add(relation);
            }
        }
        return conditions;
    }

    public List<Relation> userpropRelations() {
        List<Relation> relations = new ArrayList<>();
        for (Relation r : this.relations()) {
            if (!r.isSysprop()) {
                relations.add(r);
            }
        }
        return relations;
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

    /**
     * This method is only used for secondary index scenario,
     * its relation must be EQ
     * @param fields the user property fields
     * @return the corresponding user property serial values of fields
     */
    public String userpropValuesString(List<Id> fields) {
        List<Object> values = new ArrayList<>(fields.size());
        for (Id field : fields) {
            boolean got = false;
            for (Relation r : this.userpropRelations()) {
                if (r.key().equals(field) && !r.isSysprop()) {
                    E.checkState(r.relation == RelationType.EQ,
                                 "Method userpropValues(List<String>) only " +
                                 "used for secondary index, " +
                                 "relation must be EQ, but got %s",
                                 r.relation());
                    values.add(r.serialValue());
                    got = true;
                }
            }
            if (!got) {
                throw new BackendException(
                          "No such userprop named '%s' in the query '%s'",
                          field, this);
            }
        }
        return concatValues(values);
    }

    public Set<Object> userpropValues(Id field) {
        Set<Object> values = new HashSet<>();
        for (Relation r : this.userpropRelations()) {
            if (r.key().equals(field)) {
                values.add(r.serialValue());
            }
        }
        return values;
    }

    public Object userpropValue(Id field) {
        Set<Object> values = this.userpropValues(field);
        if (values.isEmpty()) {
            return null;
        }
        E.checkState(values.size() == 1,
                     "Expect one user-property value of field '%s', " +
                     "but got '%s'", field, values.size());
        return values.iterator().next();
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

    public boolean hasSearchCondition() {
        // NOTE: we need to judge all the conditions, including the nested
        for (Condition.Relation r : this.relations()) {
            if (r.relation().isSearchType()) {
                return true;
            }
        }
        return false;
    }

    public boolean hasSecondaryCondition() {
        // NOTE: we need to judge all the conditions, including the nested
        for (Condition.Relation r : this.relations()) {
            if (r.relation().isSecondaryType()) {
                return true;
            }
        }
        return false;
    }

    public boolean hasNeqCondition() {
        // NOTE: we need to judge all the conditions, including the nested
        for (Condition.Relation r : this.relations()) {
            if (r.relation() == RelationType.NEQ) {
                return true;
            }
        }
        return false;
    }


    public boolean matchUserpropKeys(List<Id> keys) {
        Set<Id> conditionKeys = this.userpropKeys();
        return keys.size() > 0 && conditionKeys.containsAll(keys);
    }

    @Override
    public ConditionQuery copy() {
        ConditionQuery query = (ConditionQuery) super.copy();
        query.originQuery(this);
        query.conditions = new LinkedHashSet<>(this.conditions);

        query.optimizedType = OptimizedType.NONE;
        query.resultsFilter = null;

        return query;
    }

    public ConditionQuery copyAndResetUnshared() {
        ConditionQuery query = this.copy();
        // These fields should not be shared by multiple sub-query
        query.optimizedType = OptimizedType.NONE;
        query.resultsFilter = null;
        return query;
    }

    @Override
    public boolean test(HugeElement element) {
        if (!this.ids().isEmpty() && !super.test(element)) {
            return false;
        }

        if (this.resultsFilter != null) {
            return this.resultsFilter.apply(element);
        }

        for (Condition cond : this.conditions()) {
            if (!cond.test(element)) {
                return false;
            }
        }
        return true;
    }

    public void checkFlattened() {
        E.checkState(this.isFlattened(),
                     "Query has none-flatten condition: %s", this);
    }

    public boolean isFlattened() {
        for (Condition condition : this.conditions) {
            if (!condition.isFlattened()) {
                return false;
            }
        }
        return true;
    }

    public boolean mayHasDupKeys(Set<HugeKeys> keys) {
        Map<HugeKeys, Integer> keyCounts = new HashMap<>();
        for (Condition condition : this.conditions()) {
            if (!condition.isRelation()) {
                // Assume may exist duplicate keys when has nested conditions
                return true;
            }
            Relation relation = (Relation) condition;
            if (keys.contains(relation.key())) {
                int keyCount = keyCounts.getOrDefault(relation.key(), 0);
                if (++keyCount > 1) {
                    return true;
                }
                keyCounts.put((HugeKeys) relation.key(), keyCount);
            }
        }
        return false;
    }

    public void optimized(OptimizedType optimizedType) {
        assert this.optimizedType.ordinal() <= optimizedType.ordinal() :
               this.optimizedType + " !<= " + optimizedType;
        this.optimizedType = optimizedType;

        Query originQuery = this.originQuery();
        if (originQuery instanceof ConditionQuery) {
            ConditionQuery cq = ((ConditionQuery) originQuery);
            /*
             * Two sub-query(flatten) will both set optimized of originQuery,
             * here we just keep the higher one, this may not be a perfect way
             */
            if (optimizedType.ordinal() > cq.optimized().ordinal()) {
                cq.optimized(optimizedType);
            }
        }
    }

    public OptimizedType optimized() {
        return this.optimizedType;
    }

    public void registerResultsFilter(Function<HugeElement, Boolean> filter) {
        assert this.resultsFilter == null;
        this.resultsFilter = filter;

        Query originQuery = this.originQuery();
        if (originQuery instanceof ConditionQuery) {
            ConditionQuery cq = ((ConditionQuery) originQuery);
            cq.registerResultsFilter(filter);
        }
    }

    public static String concatValues(List<Object> values) {
        List<Object> newValues = new ArrayList<>(values.size());
        for (Object v : values) {
            newValues.add(convertNumberIfNeeded(v));
        }
        return SplicingIdGenerator.concatValues(newValues);
    }

    private static Object convertNumberIfNeeded(Object value) {
        if (NumericUtil.isNumber(value) || value instanceof Date) {
            // Numeric or date values should be converted to string
            return LongEncoding.encodeNumber(value);
        }
        return value;
    }

    public enum OptimizedType {
        NONE,
        PRIMARY_KEY,
        SORT_KEYS,
        INDEX,
        INDEX_FILTER
    }
}
