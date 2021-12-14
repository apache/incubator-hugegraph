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

package com.baidu.hugegraph.traversal.optimize;

import java.util.function.BiPredicate;
import java.util.regex.Pattern;

import org.apache.tinkerpop.gremlin.process.traversal.P;

import com.baidu.hugegraph.backend.query.Condition.RelationType;

public class ConditionP extends P<Object> {

    private static final long serialVersionUID = 9094970577400072902L;

    private ConditionP(final BiPredicate<Object, Object> predicate,
                       Object value) {
        super(predicate, value);
    }

    public static ConditionP textContains(Object value) {
        return new ConditionP(RelationType.TEXT_CONTAINS, value);
    }

    public static ConditionP textNotContains(Object value) {
        return new ConditionP(RelationType.TEXT_NOT_CONTAINS, value);
    }

    public static ConditionP textStartingWith(Object value) {
        return new ConditionP(RelationType.TEXT_STARTING_WITH, value);
    }

    public static ConditionP textNotStartingWith(Object value) {
        return new ConditionP(RelationType.TEXT_NOT_STARTING_WITH, value);
    }

    public static ConditionP textEndingWith(Object value) {
        return new ConditionP(RelationType.TEXT_ENDING_WITH, value);
    }

    public static ConditionP textNotEndingWith(Object value) {
        return new ConditionP(RelationType.TEXT_NOT_ENDING_WITH, value);
    }

    public static ConditionP textMatchRegex(Object value) {
        return new ConditionP(RelationType.TEXT_MATCH_REGEX, value);
    }

    public static ConditionP textMatchEditDistance(Object value) {
        return new ConditionP(RelationType.TEXT_MATCH_EDIT_DISTANCE, value);
    }

    public static ConditionP contains(Object value) {
        return new ConditionP(RelationType.CONTAINS, value);
    }

    public static ConditionP containsK(Object value) {
        return new ConditionP(RelationType.CONTAINS_KEY, value);
    }

    public static ConditionP containsV(Object value) {
        return new ConditionP(RelationType.CONTAINS_VALUE, value);
    }

    public static ConditionP eq(Object value) {
        // EQ that can compare two array
        return new ConditionP(RelationType.EQ, value);
    }
}
