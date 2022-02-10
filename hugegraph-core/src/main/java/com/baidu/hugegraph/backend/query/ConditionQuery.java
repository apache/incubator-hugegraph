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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
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
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeProperty;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.baidu.hugegraph.util.LongEncoding;
import com.baidu.hugegraph.util.NumericUtil;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class ConditionQuery extends IdQuery {

    public static final char INDEX_SYM_MIN = '\u0000';
    public static final String INDEX_SYM_ENDING = "\u0000";
    public static final String INDEX_SYM_NULL = "\u0001";
    public static final String INDEX_SYM_EMPTY = "\u0002";
    public static final char INDEX_SYM_MAX = '\u0003';

    public static final String INDEX_VALUE_NULL = "<null>";
    public static final String INDEX_VALUE_EMPTY = "<empty>";

    public static final Set<String> IGNORE_SYM_SET;
    static {
        List<String> list = new ArrayList<>(INDEX_SYM_MAX - INDEX_SYM_MIN);
        for (char ch = INDEX_SYM_MIN; ch <= INDEX_SYM_MAX; ch++) {
            list.add(String.valueOf(ch));
        }
        IGNORE_SYM_SET = ImmutableSet.copyOf(list);
    }

    private static final List<Condition> EMPTY_CONDITIONS = ImmutableList.of();

    // Conditions will be concated with `and` by default
    private List<Condition> conditions = EMPTY_CONDITIONS;

    private OptimizedType optimizedType = OptimizedType.NONE;
    private Function<HugeElement, Boolean> resultsFilter = null;
    private Element2IndexValueMap element2IndexValueMap = null;

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

        if (this.conditions == EMPTY_CONDITIONS) {
            this.conditions = InsertionOrderUtil.newList();
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
    public int conditionsSize() {
        return this.conditions.size();
    }

    @Override
    public Collection<Condition> conditions() {
        return Collections.unmodifiableList(this.conditions);
    }

    public void resetConditions(List<Condition> conditions) {
        this.conditions = conditions;
    }

    public void resetConditions() {
        this.conditions = EMPTY_CONDITIONS;
    }

    public void recordIndexValue(Id propertyId, Id id, Object indexValue) {
        this.ensureElement2IndexValueMap();
        this.element2IndexValueMap.addIndexValue(propertyId, id, indexValue);
    }

    public void selectedIndexField(Id indexField) {
        this.ensureElement2IndexValueMap();
        this.element2IndexValueMap.selectedIndexField(indexField);
    }

    public Set<LeftIndex> getElementLeftIndex(Id elementId) {
        if (this.element2IndexValueMap == null) {
            return null;
        }
        return this.element2IndexValueMap.getLeftIndex(elementId);
    }

    public void removeElementLeftIndex(Id elementId) {
        if (this.element2IndexValueMap == null) {
            return;
        }
        this.element2IndexValueMap.removeElementLeftIndex(elementId);
    }

    public boolean existLeftIndex(Id elementId) {
        return this.getElementLeftIndex(elementId) != null;
    }

    public List<Condition.Relation> relations() {
        List<Condition.Relation> relations = new ArrayList<>();
        for (Condition c : this.conditions) {
            relations.addAll(c.relations());
        }
        return relations;
    }

    public Relation relation(Id key){
        for (Relation r : this.relations()) {
            if (r.key().equals(key)) {
                return r;
            }
        }
        return null;
    }

    @Watched
    public <T> T condition(Object key) {
        List<Object> valuesEQ = InsertionOrderUtil.newList();
        List<Object> valuesIN = InsertionOrderUtil.newList();
        for (Condition c : this.conditions) {
            if (c.isRelation()) {
                Condition.Relation r = (Condition.Relation) c;
                if (r.key().equals(key)) {
                    if (r.relation() == RelationType.EQ) {
                        valuesEQ.add(r.value());
                    } else if (r.relation() == RelationType.IN) {
                        Object value = r.value();
                        assert value instanceof List;
                        valuesIN.add(value);
                    }
                }
            }
        }
        if (valuesEQ.isEmpty() && valuesIN.isEmpty()) {
            return null;
        }
        if (valuesEQ.size() == 1 && valuesIN.size() == 0) {
            @SuppressWarnings("unchecked")
            T value = (T) valuesEQ.get(0);
            return value;
        }
        if (valuesEQ.size() == 0 && valuesIN.size() == 1) {
            @SuppressWarnings("unchecked")
            T value = (T) valuesIN.get(0);
            return value;
        }

        Set<Object> intersectValues = InsertionOrderUtil.newSet();
        for (Object value : valuesEQ) {
            List<Object> valueAsList = ImmutableList.of(value);
            if (intersectValues.isEmpty()) {
                intersectValues.addAll(valueAsList);
            } else {
                CollectionUtil.intersectWithModify(intersectValues,
                                                   valueAsList);
            }
        }
        for (Object value : valuesIN) {
            @SuppressWarnings("unchecked")
            List<Object> valueAsList = (List<Object>) value;
            if (intersectValues.isEmpty()) {
                intersectValues.addAll(valueAsList);
            } else {
                CollectionUtil.intersectWithModify(intersectValues,
                                                   valueAsList);
            }
        }

        if (intersectValues.size() == 0) {
            return null;
        }
        E.checkState(intersectValues.size() == 1,
                     "Illegal key '%s' with more than one value: %s",
                     key, intersectValues);
        @SuppressWarnings("unchecked")
        T value = (T) intersectValues.iterator().next();
        return value;
    }

    public void unsetCondition(Object key) {
        for (Iterator<Condition> iter = this.conditions.iterator();
             iter.hasNext();) {
            Condition c = iter.next();
            if (c.isRelation() && ((Condition.Relation) c).key().equals(key)) {
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

    public boolean containsRelation(HugeKeys key, Condition.RelationType type) {
        for (Relation r : this.relations()) {
            if (r.key().equals(key) && r.relation().equals(type)) {
                return true;
            }
        }
        return false;
    }

    public boolean containsRelation(Condition.RelationType type) {
        for (Relation r : this.relations()) {
            if (r.relation().equals(type)) {
                return true;
            }
        }
        return false;
    }

    public boolean containsScanRelation() {
        return this.containsRelation(Condition.RelationType.SCAN);
    }

    public boolean containsContainsCondition(Id key) {
        for (Relation r : this.relations()) {
            if (r.key().equals(key)) {
                return r.relation().equals(RelationType.CONTAINS) ||
                       r.relation().equals(RelationType.TEXT_CONTAINS);
            }
        }
        return false;
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
                    E.checkState(r.relation == RelationType.EQ ||
                                 r.relation == RelationType.CONTAINS,
                                 "Method userpropValues(List<String>) only " +
                                 "used for secondary index, " +
                                 "relation must be EQ or CONTAINS, but got %s",
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
        if (query.conditions != EMPTY_CONDITIONS) {
            query.conditions = InsertionOrderUtil.newList(this.conditions);
        }
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
        boolean valid = true;
        for (Condition cond : this.conditions) {
            valid &= cond.test(element);
            valid &= (this.element2IndexValueMap == null ||
                      this.element2IndexValueMap.validRangeIndex(element,  cond));
        }
        return valid;
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
        for (Condition condition : this.conditions) {
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

    public ConditionQuery originConditionQuery() {
        Query originQuery = this.originQuery();
        if (!(originQuery instanceof ConditionQuery)) {
            return null;
        }

        while (originQuery.originQuery() instanceof ConditionQuery) {
            originQuery = originQuery.originQuery();
        }
        return (ConditionQuery) originQuery;
    }

    private void ensureElement2IndexValueMap() {
        if (this.element2IndexValueMap == null) {
            this.element2IndexValueMap = new Element2IndexValueMap();
        }
    }

    public static String concatValues(List<?> values) {
        assert !values.isEmpty();
        List<Object> newValues = new ArrayList<>(values.size());
        for (Object v : values) {
            newValues.add(concatValues(v));
        }
        return SplicingIdGenerator.concatValues(newValues);
    }

    public static String concatValues(Object value) {
        if (value instanceof String) {
            return escapeSpecialValueIfNeeded((String) value);
        } if (value instanceof List) {
            return concatValues((List<?>) value);
        } else if (needConvertNumber(value)) {
            return LongEncoding.encodeNumber(value);
        } else if (value.equals(INDEX_VALUE_NULL)) {
            return INDEX_SYM_NULL;
        } else {
            return escapeSpecialValueIfNeeded(value.toString());
        }
    }

    private static boolean needConvertNumber(Object value) {
        // Numeric or date values should be converted to number from string
        return NumericUtil.isNumber(value) || value instanceof Date;
    }

    private static String escapeSpecialValueIfNeeded(String value) {
        if (value.isEmpty()) {
            // Escape empty String to INDEX_SYM_EMPTY (char `\u0002`)
            value = INDEX_SYM_EMPTY;
        } else if (value.equals(INDEX_VALUE_EMPTY)) {
            value = "";
        } else {
            char ch = value.charAt(0);
            if (ch <= INDEX_SYM_MAX) {
                /*
                 * Special symbols can't be used due to impossible to parse,
                 * and treat it as illegal value for the origin text property.
                 * TODO: escape special symbols
                 */
                E.checkArgument(false,
                                "Illegal leading char '\\u%s' " +
                                "in index property: '%s'",
                                (int) ch, value);
            }
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

    public static final class Element2IndexValueMap {

        private final Map<Id, Set<LeftIndex>> leftIndexMap;
        private final Map<Id, Map<Id, Set<Object>>> filed2IndexValues;
        private Id selectedIndexField;

        public Element2IndexValueMap() {
            this.filed2IndexValues = new HashMap<>();
            this.leftIndexMap = new HashMap<>();
        }

        public void addIndexValue(Id indexField, Id elementId,
                                  Object indexValue) {
            if (!this.filed2IndexValues.containsKey(indexField)) {
                this.filed2IndexValues.put(indexField, new HashMap<>());
            }
            Map<Id, Set<Object>> element2IndexValueMap =
                                 this.filed2IndexValues.get(indexField);
            if (element2IndexValueMap.containsKey(elementId)) {
                element2IndexValueMap.get(elementId).add(indexValue);
            } else {
                element2IndexValueMap.put(elementId,
                                          Sets.newHashSet(indexValue));
            }
        }

        public void selectedIndexField(Id indexField) {
            this.selectedIndexField = indexField;
        }

        public Set<Object> removeIndexValues(Id indexField, Id elementId) {
            if (!this.filed2IndexValues.containsKey(indexField)) {
                return null;
            }
            return this.filed2IndexValues.get(indexField).get(elementId);
        }

        public void addLeftIndex(Id indexField, Set<Object> indexValues,
                                 Id elementId) {
            LeftIndex leftIndex = new LeftIndex(indexValues, indexField);
            if (this.leftIndexMap.containsKey(elementId)) {
                this.leftIndexMap.get(elementId).add(leftIndex);
            } else {
                this.leftIndexMap.put(elementId, Sets.newHashSet(leftIndex));
            }
        }

        public Set<LeftIndex> getLeftIndex(Id elementId) {
            return this.leftIndexMap.get(elementId);
        }

        public void removeElementLeftIndex(Id elementId) {
            this.leftIndexMap.remove(elementId);
        }

        public boolean validRangeIndex(HugeElement element, Condition cond) {
            // Not UserpropRelation
            if (!(cond instanceof Condition.UserpropRelation)) {
                return true;
            }

            Condition.UserpropRelation propRelation =
                                       (Condition.UserpropRelation) cond;
            Id propId = propRelation.key();
            Set<Object> fieldValues = this.removeIndexValues(propId,
                                                             element.id());
            if (fieldValues == null) {
                // Not range index
                return true;
            }

            HugeProperty<Object> hugeProperty = element.getProperty(propId);
            if (hugeProperty == null) {
                // Property value has been deleted
                this.addLeftIndex(propId, fieldValues, element.id());
                return false;
            }

            /*
             * NOTE: If success remove means has correct index,
             * we should add left index values to left index map
             * waiting to be removed
             */
            boolean hasRightValue = removeValue(fieldValues, hugeProperty.value());
            if (fieldValues.size() > 0) {
                this.addLeftIndex(propId, fieldValues, element.id());
            }

            /*
             * NOTE: When query by more than one range index field,
             * if current field is not the selected one, it can only be used to
             * determine whether the index values matched, can't determine
             * the element is valid or not
             */
            if (this.selectedIndexField != null) {
                return !propId.equals(this.selectedIndexField) || hasRightValue;
            }

            return hasRightValue;
        }

        private static boolean removeValue(Set<Object> values, Object value){
            for (Object compareValue : values) {
                if (numberEquals(compareValue, value)) {
                    values.remove(compareValue);
                    return true;
                }
            }
            return false;
        }

        private static boolean numberEquals(Object number1, Object number2) {
            // Same class compare directly
            if (number1.getClass().equals(number2.getClass())) {
                return number1.equals(number2);
            }

            // Otherwise convert to BigDecimal to make two numbers comparable
            Number n1 = NumericUtil.convertToNumber(number1);
            Number n2 = NumericUtil.convertToNumber(number2);
            BigDecimal b1 = BigDecimal.valueOf(n1.doubleValue());
            BigDecimal b2 = BigDecimal.valueOf(n2.doubleValue());
            return b1.compareTo(b2) == 0;
        }
    }

    public static final class LeftIndex {

        private final Set<Object> indexFieldValues;
        private final Id indexField;

        public LeftIndex(Set<Object> indexFieldValues, Id indexField) {
            this.indexFieldValues = indexFieldValues;
            this.indexField = indexField;
        }

        public Set<Object> indexFieldValues() {
            return this.indexFieldValues;
        }

        public Id indexField() {
            return this.indexField;
        }
    }
}
