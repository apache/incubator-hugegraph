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
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Condition.Relation;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.NumericUtil;
import com.google.common.collect.ImmutableList;

public final class ConditionQueryFlatten {

    public static List<ConditionQuery> flatten(ConditionQuery query) {
        if (query.isFlattened()) {
            return Arrays.asList(query);
        }

        List<ConditionQuery> queries = new ArrayList<>();

        // Flatten IN/NOT_IN if needed
        Set<Condition> conditions = new HashSet<>();
        for (Condition condition : query.conditions()) {
            Condition cond = flattenIn(condition);
            if (cond == null) {
                // Process 'XX in []'
                return ImmutableList.of();
            }
            conditions.add(cond);
        }
        query = query.copy();
        query.resetConditions(conditions);

        // Flatten OR if needed
        Set<Relations> results = null;
        for (Condition condition : query.conditions()) {
            if (results == null) {
                results = flattenAndOr(condition);
            } else {
                results = and(results, flattenAndOr(condition));
            }
        }

        // Optimize useless condition
        assert results != null;
        for (Relations relations : results) {
            relations = optimizeRelations(relations);
            /*
             * Relations may be null after optimize, for example:
             * age > 10 and age == 9
             */
            if (relations == null) {
                continue;
            }
            ConditionQuery cq = newQueryFromRelations(query, relations);
            if (cq != null) {
                queries.add(cq);
            }
        }

        return queries;
    }

    private static Condition flattenIn(Condition condition) {
        switch (condition.type()) {
            case RELATION:
                Relation relation = (Relation) condition;
                switch (relation.relation()) {
                    case IN:
                        // Flatten IN if needed
                        return convIn2Or(relation);
                    case NOT_IN:
                        // Flatten NOT_IN if needed
                        return convNotin2And(relation);
                    case TEXT_CONTAINS_ANY:
                        // Flatten TEXT_CONTAINS_ANY if needed
                        return convTextContainsAny2Or(relation);
                    default:
                        return condition;
                }
            case AND:
                Condition.And and = (Condition.And) condition;
                return new Condition.And(flattenIn(and.left()),
                                         flattenIn(and.right()));
            case OR:
                Condition.Or or = (Condition.Or) condition;
                return new Condition.Or(flattenIn(or.left()),
                                        flattenIn(or.right()));
            default:
                throw new AssertionError(String.format(
                          "Wrong condition type: '%s'", condition.type()));
        }
    }

    private static Condition convIn2Or(Relation relation) {
        assert relation.relation() == Condition.RelationType.IN;
        Object key = relation.key();
        @SuppressWarnings("unchecked")
        List<Object>  values = (List<Object>) relation.value();
        Condition cond, conds = null;
        for (Object value : values) {
            if (key instanceof HugeKeys) {
                cond = Condition.eq((HugeKeys) key, value);
            } else {
                cond = Condition.eq((Id) key, value);
            }
            conds = conds == null ? cond : Condition.or(conds, cond);
        }
        return conds;
    }

    private static Condition convNotin2And(Relation relation) {
        assert relation.relation() == Condition.RelationType.NOT_IN;
        Object key = relation.key();
        @SuppressWarnings("unchecked")
        List<Object>  values = (List<Object>) relation.value();
        Condition cond;
        Condition conds = null;
        for (Object value : values) {
            if (key instanceof HugeKeys) {
                cond = Condition.neq((HugeKeys) key, value);
            } else {
                cond = Condition.neq((Id) key, value);
            }
            conds = conds == null ? cond : Condition.and(conds, cond);
        }
        return conds;
    }

    private static Condition convTextContainsAny2Or(Relation relation) {
        assert relation.relation() == Condition.RelationType.TEXT_CONTAINS_ANY;
        @SuppressWarnings("unchecked")
        Collection<String> words = (Collection<String>) relation.value();
        Condition cond, conds = null;
        for (String word : words) {
            assert relation.key() instanceof Id;
            cond = Condition.textContains((Id) relation.key(), word);
            conds = conds == null ? cond : Condition.or(conds, cond);
        }
        return conds;
    }

    private static Set<Relations> flattenAndOr(Condition condition) {
        Set<Relations> result = new HashSet<>();
        switch (condition.type()) {
            case RELATION:
                Relation relation = (Relation) condition;
                result.add(Relations.of(relation));
                break;
            case AND:
                Condition.And and = (Condition.And) condition;
                result = and(flattenAndOr(and.left()),
                             flattenAndOr(and.right()));
                break;
            case OR:
                Condition.Or or = (Condition.Or) condition;
                result = or(flattenAndOr(or.left()),
                            flattenAndOr(or.right()));
                break;
            default:
                throw new AssertionError(String.format(
                          "Wrong condition type: '%s'", condition.type()));
        }
        return result;
    }

    private static Set<Relations> and(Set<Relations> left,
                                      Set<Relations> right) {
        Set<Relations> result = new HashSet<>();
        for (Relations leftRelations : left) {
            for (Relations rightRelations : right) {
                Relations relations = new Relations();
                relations.addAll(leftRelations);
                relations.addAll(rightRelations);
                result.add(relations);
            }
        }
        return result;
    }

    private static Set<Relations> or(Set<Relations> left,
                                     Set<Relations> right) {
        Set<Relations> result = new HashSet<>(left);
        result.addAll(right);
        return result;
    }

    private static ConditionQuery newQueryFromRelations(ConditionQuery query,
                                                        Relations relations) {
        ConditionQuery cq = query.copy();
        cq.resetConditions();
        for (Relation relation : relations) {
            cq.query(relation);
        }
        return cq;
    }

    private static Relations optimizeRelations(Relations relations) {
        // Optimize and-relations in one query
        // e.g. (age>1 and age>2) -> (age>2)
        Set<Object> keys = relations.stream().map(Relation::key)
                                    .collect(Collectors.toSet());

        // No duplicated keys
        if (keys.size() == relations.size()) {
            return relations;
        }

        for (Object key : keys) {
            // Get same key relations
            Relations rs = new Relations();
            for (Relation relation : relations) {
                if (relation.key().equals(key)) {
                    rs.add(relation);
                }
            }
            // Same key relation number is 1, needn't merge
            if (rs.size() == 1) {
                continue;
            }
            // Relations in rs having same key might need merge
            relations.removeAll(rs);
            rs = mergeRelations(rs);
            // Conditions are mutually exclusive(e.g. age>10 and age==9)
            if (rs.isEmpty()) {
                return null;
            }
            relations.addAll(rs);
        }

        return relations;
    }

    /**
     * Reduce and merge relations linked with 'AND' for same key
     * @param relations linked with 'AND' having same key, may contains 'in',
     *                 'not in', '>', '<', '>=', '<=', '==', '!='
     * @return merged relations
     */
    private static Relations mergeRelations(Relations relations) {
        Relations result = new Relations();
        boolean isNum = false;

        Relation gt = null;
        Relation gte = null;
        Relation eq = null;
        Relation lt = null;
        Relation lte = null;

        for (Relation relation : relations) {
            switch (relation.relation()) {
                case GT:
                    isNum = true;
                    if (gt == null || compare(relation, gt) > 0) {
                        gt = relation;
                    }
                    break;
                case GTE:
                    isNum = true;
                    if (gte == null || compare(relation, gte) > 0) {
                        gte = relation;
                    }
                    break;
                case EQ:
                    if (eq == null) {
                        eq = relation;
                        if (eq.value() instanceof Number) {
                            isNum = true;
                        }
                        break;
                    } else if (!relation.value().equals(eq.value())) {
                        return Relations.of();
                    }
                    break;
                case LT:
                    isNum = true;
                    if (lt == null || compare(lt, relation) > 0) {
                        lt = relation;
                    }
                    break;
                case LTE:
                    isNum = true;
                    if (lte == null || compare(lte, relation) > 0) {
                        lte = relation;
                    }
                    break;
                default: // NEQ, IN, NOT_IN, CONTAINS, CONTAINS_KEY, SCAN
                    result.add(relation);
                    break;
            }
        }

        if (!isNum) {
            // Not number, only may have equal relation
            if (eq != null) {
                result.add(eq);
            }
        } else {
            // At most have 1 eq, 1 lt, 1 lte, 1 gt, 1 gte
            result.addAll(calcValidRange(gte, gt, eq, lte, lt));
        }
        return result;
    }

    private static Relations calcValidRange(Relation gte, Relation gt,
                                            Relation eq, Relation lte,
                                            Relation lt) {
        Relations result = new Relations();
        Relation lower = null;
        Relation upper = null;

        if (gte != null) {
            lower = gte;
        }
        if (gt != null) {
            // Select bigger one in (gt, gte) as lower limit
            lower = highRelation(gte, gt);
        }
        if (lt != null) {
            upper = lt;
        }
        if (lte != null) {
            // Select smaller one in (lt, lte) as upper limit
            upper = lowRelation(lte, lt);
        }
        // Ensure low < high
        if (!validRange(lower, upper)) {
            return Relations.of();
        }

        // Handle eq
        if (eq != null) {
            if (!validEq(eq, lower, upper)) {
                return Relations.of();
            }
            result.add(eq);
            return result;
        }

        // No eq if come here
        assert lower != null || upper != null;
        if (lower != null) {
            result.add(lower);
        }
        if (upper != null) {
            result.add(upper);
        }

        return result;
    }

    private static boolean validRange(Relation low, Relation high) {
        if (low == null || high == null) {
            return true;
        }
        return compare(low, high) < 0 || compare(low, high) == 0 &&
               low.relation() == Condition.RelationType.GTE &&
               high.relation() == Condition.RelationType.LTE;
    }

    private static boolean validEq(Relation eq, Relation low, Relation high) {
        // Ensure equalValue > lowValue
        if (low != null) {
            switch (low.relation()) {
                case GTE:
                    if (compare(eq, low) < 0) {
                        return false;
                    }
                    break;
                case GT:
                    if (compare(eq, low) <= 0) {
                        return false;
                    }
                    break;
                default:
                    throw new AssertionError("Must be GTE or GT");
            }
        }
        // Ensure equalValue < highValue
        if (high != null) {
            switch (high.relation()) {
                case LTE:
                    if (compare(eq, high) > 0) {
                        return false;
                    }
                    break;
                case LT:
                    if (compare(eq, high) >= 0) {
                        return false;
                    }
                    break;
                default:
                    throw new AssertionError("Must be LTE or LT");
            }
        }
        return true;
    }

    private static Relation highRelation(Relation first, Relation second) {
        return selectRelation(first, second, true);
    }

    private static Relation lowRelation(Relation first, Relation second) {
        return selectRelation(first, second, false);
    }

    private static Relation selectRelation(Relation first, Relation second,
                                           boolean high) {
        if (first == null) {
            return second;
        }
        if (second == null) {
            return first;
        }
        if (high) {
            if (compare(first, second) > 0) {
                return first;
            } else {
                return second;
            }
        } else {
            if (compare(first, second) < 0) {
                return first;
            } else {
                return second;
            }
        }
    }

    private static int compare(Relation first, Relation second) {
        Object firstValue = first.value();
        Object secondValue = second.value();
        if (firstValue instanceof Number && secondValue instanceof Number) {
            return NumericUtil.compareNumber(firstValue, (Number) secondValue);
        } else if (firstValue instanceof Date && secondValue instanceof Date) {
            return ((Date) firstValue).compareTo((Date) secondValue);
        } else {
            throw new IllegalArgumentException(String.format(
                      "Can't compare between %s and %s", first, second));
        }
    }

    /**
     * Rename Relation Set to Relations to make code more readable
     */
    private static class Relations extends HashSet<Relation> {

        private static final long serialVersionUID = -2110811280408887334L;

        public static Relations of(Relation... relations) {
            Relations rs = new Relations();
            rs.addAll(Arrays.asList(relations));
            return rs;
        }
    }
}
