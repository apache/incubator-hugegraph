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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.baidu.hugegraph.backend.query.Condition.Relation;
import com.google.common.collect.ImmutableSet;

public class ConditionQueryFlatten {

    public static List<ConditionQuery> flatten(ConditionQuery query) {
        List<ConditionQuery> queries = new ArrayList<>();
        Set<Set<Relation>> results = null;
        for (Condition condition : query.conditions()) {
            if (results == null) {
                results = flatten(condition);
            } else {
                results = and(results, flatten(condition));
            }
        }
        assert results != null;
        for (Set<Relation> relations : results) {
            queries.add(queryFromRelations(query, optimizeRelation(relations)));
        }
        return queries;
    }

    private static Set<Set<Relation>> flatten(Condition condition) {
        Set<Set<Relation>> result = new HashSet<>();
        switch (condition.type()) {
            case RELATION:
                Relation relation = condition.relations().get(0);
                result.add(ImmutableSet.of(relation));
                break;
            case AND:
                Condition.And and = (Condition.And) condition;
                result = and(flatten(and.left()), flatten(and.right()));
                break;
            case OR:
                Condition.Or or = (Condition.Or) condition;
                result = or(flatten(or.left()), flatten(or.right()));
                break;
            default:
                throw new AssertionError(String.format(
                          "Wrong condition type: '%s'", condition.type()));
        }
        return result;
    }

    private static Set<Set<Relation>> and(Set<Set<Relation>> left,
                                          Set<Set<Relation>> right) {
        Set<Set<Relation>> result = new HashSet<>();
        for (Set<Relation> leftRelations : left) {
            for (Set<Relation> rightRelations : right) {
                Set<Relation> relations = new HashSet<>();
                relations.addAll(leftRelations);
                relations.addAll(rightRelations);
                result.add(relations);
            }
        }
        return result;
    }

    private static Set<Set<Relation>> or(Set<Set<Relation>> left,
                                         Set<Set<Relation>> right) {
        Set<Set<Relation>> result = new HashSet<>(left);
        result.addAll(right);
        return result;
    }

    private static ConditionQuery queryFromRelations(ConditionQuery query,
                                                     Set<Relation> relations) {
        ConditionQuery q = query.copy();
        q.resetConditions();
        for (Relation relation : relations) {
            q.query(relation);
        }
        return q;
    }

    private static Set<Relation> optimizeRelation(Set<Relation> relations) {
        // TODO: optimize and-relations in one query
        // e.g. (age>1 and age>2) -> (age>2)
        return relations;
    }
}
