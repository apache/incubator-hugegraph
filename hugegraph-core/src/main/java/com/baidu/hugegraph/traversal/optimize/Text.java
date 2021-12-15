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

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.util.E;

import java.util.regex.Pattern;

public class Text {

    public static ConditionP contains(String value) {
        return ConditionP.textContains(value);
    }

    public static ConditionP matchRegex(String value) {
        return ConditionP.textMatchRegex(value);
    }

    public static ConditionP matchEditDistance(String value, int distance) {
        E.checkArgument(distance >= 0, "The distance should be >= 0");
        String prefix = String.valueOf(distance) + "#";
        return ConditionP.textMatchEditDistance(prefix + value);
    }

    public static ConditionP notContains(String value) {
        return ConditionP.textNotContains(value);
    }

    public static ConditionP prefix(String value) {
        return ConditionP.prefix(value);
    }

    public static ConditionP notPrefix(String value) {
        return ConditionP.notPrefix(value);
    }

    public static ConditionP suffix(String value) {
        return ConditionP.suffix(value);
    }

    public static ConditionP notSuffix(String value) {
        return ConditionP.notSuffix(value);
    }

    public static ConditionP containsFuzzy(String value) {
        return ConditionP.containsFuzzy(value);
    }

    public static ConditionP containsRegex(String value) {
        return ConditionP.containsRegex(value);
    }

    public static ConditionP fuzzy(String value) {
        return ConditionP.fuzzy(value);
    }

    public static ConditionP regex(String value) {
        return ConditionP.regex(value);
    }

    public static Id uuid(String id) {
        return IdGenerator.of(id, true);
    }
}
